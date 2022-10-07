/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CDC_RSYNC_BASE_MESSAGE_PUMP_H_
#define CDC_RSYNC_BASE_MESSAGE_PUMP_H_

#include <queue>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "common/buffer.h"

namespace google {
namespace protobuf {
class MessageLite;
}
}  // namespace google

namespace cdc_ft {

class Socket;

// See messages.proto. When sending a kXXXRequest from client to server or a
// kXXXResponse from server to client, use packet type kXXX. See messages.proto.
enum class PacketType {
  // Not a proto, just raw bytes.
  kRawData = 0,

  // Used for testing.
  kTest,

  // Send options to server.
  kSetOptions,

  // Toggle compression on/off.
  kToggleCompression,

  //
  // Send all files from client to server.
  //

  // Send file paths including timestamps and sizes, and directories to server.
  // An empty request indicates that all data has been sent.
  kAddFiles,
  // Send stats about missing, excessive, changed and matching files to client.
  kSendFileStats,

  //
  // Send all missing files from server to client.
  //

  // Send indices of missing files to client.
  // An empty request indicates that all data has been sent.
  // Also used for sending indices of changed files.
  kAddFileIndices,

  // Start sending missing file data to the server. After each
  // SendMissingFileDataRequest, the client sends file data as raw packets and
  // an empty packet to indicate eof.
  kSendMissingFileData,

  //
  // Rsync data exchange.
  //

  // Send signatures to client.
  // An empty response indicates that all data has been sent.
  kAddSignatures,

  // Send patch commands to server.
  // An empty request indicates that all data has been sent.
  kAddPatchCommands,

  //
  // Deletion of extraneous files.
  //
  kAddDeletedFiles,

  //
  // Shutdown.
  //

  // Ask the server to shut down. Also used for shutdown ack.
  kShutdown,

  // Must be last.
  kCount
};

class MessagePump {
 public:
  using PacketReceivedDelegate = std::function<void(PacketType)>;

  // |socket| is the underlying socket that data is sent to and received from,
  // unless redirected with one of the Redirect* methods. |packet_received| is
  // a callback that is called from the receiver thread as soon as a packet is
  // received. RedirectInput() should be called from this delegate. Useful for
  // things like decompression.
  MessagePump(Socket* socket, PacketReceivedDelegate packet_received);
  virtual ~MessagePump();

  // Starts worker threads to send/receive messages. Should be called after the
  // socket is connected. Must not be already started.
  // NOT thread-safe. Should be called from the creation thread.
  void StartMessagePump();

  // Stops worker threads to send/receive messages. No-op if already stopped or
  // not started. Cannot be restarted.
  // NOT thread-safe. Should be called from the creation thread.
  void StopMessagePump() ABSL_LOCKS_EXCLUDED(outgoing_mutex_, incoming_mutex_);

  // Queues data for sending. May block if too much data is queued.
  // Thread-safe.
  absl::Status SendRawData(const void* data, size_t size);
  absl::Status SendMessage(PacketType type,
                           const google::protobuf::MessageLite& message);

  // Receives a packet. Blocks if currently no packets is available.
  // Thread-safe.
  absl::Status ReceiveRawData(Buffer* data);
  absl::Status ReceiveMessage(PacketType type,
                              google::protobuf::MessageLite* message);

  // Returns true if the Receive* functions have data available. Note that
  // receiving messages from multiple threads might be racy, i.e. if
  // CanReceive() returns true and Receive* is called afterwards, the method
  // might block if another thread has grabbed the packet in the meantime.
  bool CanReceive() const { return incoming_packets_byte_size_ > 0; }

  // Blocks until all outgoing messages were sent. Does not prevent that other
  // threads queue new packets while the method is blocking, so the caller
  // should make sure that that's not the case for consistent behavior.
  // Thread-safe.
  void FlushOutgoingQueue() ABSL_LOCKS_EXCLUDED(outgoing_mutex_);

  class InputReader {
   public:
    virtual ~InputReader() {}

    // Reads as much as data possible to |out_buffer|, but no more than
    // |out_size| bytes. Sets |bytes_read| to the number of bytes read.
    // |eof| is set to true if no more input data is available. The flag
    // indicates that the parent MessagePump should reset the input reader
    // and read data from the socket again.
    virtual absl::Status Read(void* out_buffer, size_t out_size,
                              size_t* bytes_read, bool* eof) = 0;
  };

  // Starts receiving input from |input_reader| instead of from the socket.
  // |input_reader| is called on a background thread. It must be a valid
  // pointer. The input reader stays in place until it returns |eof| == true.
  // After that, the input reader is reset and data is received from the socket
  // again.
  // This method must be called from the receiver thread, usually during the
  // execution of the PacketReceivedDelegate passed in the constructor.
  // Otherwise, the receiver thread might be blocked on a recv() call and the
  // first data received would still be read the socket.
  void RedirectInput(std::unique_ptr<InputReader> input_reader);

  // If set to a non-empty function, starts sending output to |output_handler|
  // instead of to the socket. If set to an empty function, starts sending to
  // the socket again. |output_handler| is called on a background thread.
  // The outgoing packet queue is flushed prior to changing the output handler.
  // The caller must make sure that no background threads are sending new
  // messages while this method is running.
  using OutputHandler =
      std::function<absl::Status(const void* data, size_t size)>;
  void RedirectOutput(OutputHandler output_handler);

  // Returns the number of packets queued for sending.
  size_t GetNumOutgoingPackagesForTesting()
      ABSL_LOCKS_EXCLUDED(outgoing_mutex_);

  // Returns the max total size of messages in the packet queues.
  size_t GetMaxInOutBufferSizeForTesting();

  // Returns hte max size of a single raw or proto message (including header).
  size_t GetMaxPacketSizeForTesting();

 protected:
  struct Packet {
    PacketType type = PacketType::kCount;
    Buffer data;

    // Instances should be moved, not copied.
    Packet() = default;
    Packet(Packet&& other) { *this = std::move(other); }
    Packet(const Packet&) = delete;
    Packet& operator=(const Packet&) = delete;

    Packet& operator=(Packet&& other) {
      type = other.type;
      data = std::move(other.data);
      return *this;
    }
  };

 private:
  // Outgoing packets are already serialized to save mem copies.
  absl::Status QueuePacket(Buffer&& serialized_packet)
      ABSL_LOCKS_EXCLUDED(outgoing_mutex_, status_mutex_);
  absl::Status DequeuePacket(Packet* packet)
      ABSL_LOCKS_EXCLUDED(incoming_mutex_, status_mutex_);

  // Underlying socket, not owned.
  Socket* socket_;

  // Delegate called if a packet was received.
  // Called immediately from the receiver thread.
  PacketReceivedDelegate packet_received_;

  // Message pump threads main method for sending and receiving data.
  void ThreadSenderMain() ABSL_LOCKS_EXCLUDED(outgoing_mutex_, status_mutex_);
  void ThreadReceiverMain() ABSL_LOCKS_EXCLUDED(incoming_mutex_, status_mutex_);

  // Actually send/receive packets.
  absl::Status ThreadDoSendPacket(Buffer&& serialized_packet);
  absl::Status ThreadDoReceivePacket(Packet* packet);
  absl::Status ThreadDoReceive(void* buffer, size_t size);

  std::thread message_sender_thread_;
  std::thread message_receiver_thread_;

  // If set, input is not received from the socket, but from |input_reader_|.
  std::unique_ptr<InputReader> input_reader_;
  // If set, output is not sent to the socket, but to |output_handler_|.
  OutputHandler output_handler_;

  //
  // Synchronization of message pump threads and main thread.
  //

  // Guards to protect access to queued packets.
  absl::Mutex outgoing_mutex_;
  absl::Mutex incoming_mutex_ ABSL_ACQUIRED_AFTER(outgoing_mutex_);

  // Queued packets.
  std::queue<Buffer> outgoing_packets_ ABSL_GUARDED_BY(outgoing_mutex_);
  std::queue<Packet> incoming_packets_ ABSL_GUARDED_BY(incoming_mutex_);

  // Total size of queued packets. Used to limit max queue size.
  std::atomic_uint64_t outgoing_packets_byte_size_{0};
  std::atomic_uint64_t incoming_packets_byte_size_{0};

  // If true, the respective thread saw an error and shut down.
  std::atomic_bool send_error_{false};
  std::atomic_bool receive_error_{false};

  // Shutdown signal to sender and receiver threads.
  std::atomic_bool shutdown_{false};

  absl::Mutex status_mutex_;
  absl::Status status_ ABSL_GUARDED_BY(status_mutex_);

  std::thread::id creation_thread_id_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_BASE_MESSAGE_PUMP_H_
