// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cdc_rsync/base/message_pump.h"

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "cdc_rsync/base/socket.h"
#include "common/buffer.h"
#include "common/log.h"
#include "common/status.h"
#include "google/protobuf/message_lite.h"

namespace cdc_ft {
namespace {

// Max total size of messages in the packet queues.
// If exdeeded, Send/Receive methods start blocking.
uint64_t kInOutBufferSize = 1024 * 1024 * 8;

// Header is 1 byte type, 3 bytes size.
constexpr size_t kHeaderSize = 4;

// Size is compressed to 3 bytes.
constexpr uint32_t kMaxPacketSize = 256 * 256 * 256 - 1;

// Creates a packet of size |kHeaderSize| + |size| and sets the header.
absl::Status CreateSerializedPacket(PacketType type, size_t size,
                                    Buffer* serialized_packet) {
  if (size > kMaxPacketSize) {
    return MakeStatus("Max packet size exceeded: %u", size);
  }

  serialized_packet->clear();
  serialized_packet->reserve(kHeaderSize + size);

  // Header is 1 byte type, 3 bytes size.
  static_assert(static_cast<size_t>(PacketType::kCount) <= 256, "");
  static_assert(kMaxPacketSize < 256 * 256 * 256, "");
  static_assert(kHeaderSize == 4, "");

  uint8_t header[] = {static_cast<uint8_t>(type),
                      static_cast<uint8_t>(size & 0xFF),
                      static_cast<uint8_t>((size >> 8) & 0xFF),
                      static_cast<uint8_t>((size >> 16) & 0xFF)};
  serialized_packet->append(header, sizeof(header));
  return absl::OkStatus();
}

#define HANDLE_PACKET_TYPE(type) \
  case PacketType::type:         \
    return #type;

const char* PacketTypeName(PacketType type) {
  if (type > PacketType::kCount) {
    return "<unknown>";
  }

  switch (type) {
    HANDLE_PACKET_TYPE(kRawData)
    HANDLE_PACKET_TYPE(kTest)
    HANDLE_PACKET_TYPE(kSetOptions)
    HANDLE_PACKET_TYPE(kToggleCompression)
    HANDLE_PACKET_TYPE(kAddFiles)
    HANDLE_PACKET_TYPE(kSendFileStats)
    HANDLE_PACKET_TYPE(kAddFileIndices)
    HANDLE_PACKET_TYPE(kSendMissingFileData)
    HANDLE_PACKET_TYPE(kAddSignatures)
    HANDLE_PACKET_TYPE(kAddPatchCommands)
    HANDLE_PACKET_TYPE(kAddDeletedFiles)
    HANDLE_PACKET_TYPE(kShutdown)
    HANDLE_PACKET_TYPE(kCount)
  }

  return "<unknown>";
}

#undef HANDLE_PACKET_TYPE

}  // namespace

MessagePump::MessagePump(Socket* socket, PacketReceivedDelegate packet_received)
    : socket_(socket),
      packet_received_(packet_received),
      creation_thread_id_(std::this_thread::get_id()) {
  assert(socket_ != nullptr);
}

MessagePump::~MessagePump() { StopMessagePump(); }

void MessagePump::StartMessagePump() {
  assert(creation_thread_id_ == std::this_thread::get_id());

  message_sender_thread_ = std::thread([this]() { ThreadSenderMain(); });
  message_receiver_thread_ = std::thread([this]() { ThreadReceiverMain(); });
}

void MessagePump::StopMessagePump() {
  assert(creation_thread_id_ == std::this_thread::get_id());

  if (shutdown_) {
    return;
  }

  FlushOutgoingQueue();

  {
    absl::MutexLock outgoing_lock(&outgoing_mutex_);
    absl::MutexLock incoming_lock(&incoming_mutex_);
    shutdown_ = true;
  }

  if (message_sender_thread_.joinable()) {
    message_sender_thread_.join();
  }

  if (message_receiver_thread_.joinable()) {
    message_receiver_thread_.join();
  }
}

absl::Status MessagePump::SendRawData(const void* data, size_t size) {
  Buffer serialized_packet;
  absl::Status status =
      CreateSerializedPacket(PacketType::kRawData, size, &serialized_packet);
  if (!status.ok()) {
    return status;
  }
  const uint8_t* u8_data = static_cast<const uint8_t*>(data);
  serialized_packet.append(u8_data, size);
  return QueuePacket(std::move(serialized_packet));
}

absl::Status MessagePump::SendMessage(
    PacketType type, const google::protobuf::MessageLite& message) {
  Buffer serialized_packet;
  size_t size = message.ByteSizeLong();
  absl::Status status = CreateSerializedPacket(type, size, &serialized_packet);
  if (!status.ok()) {
    return status;
  }

  // Serialize the message directly into the packet.
  serialized_packet.resize(kHeaderSize + size);
  if (size > 0 &&
      !message.SerializeToArray(serialized_packet.data() + kHeaderSize,
                                static_cast<int>(size))) {
    return MakeStatus("Failed to serialize message to array");
  }

  return QueuePacket(std::move(serialized_packet));
}

absl::Status MessagePump::QueuePacket(Buffer&& serialize_packet) {
  // Wait a little if the max queue size is exceeded.
  absl::MutexLock outgoing_lock(&outgoing_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(outgoing_mutex_) {
    return outgoing_packets_byte_size_ < kInOutBufferSize || send_error_ ||
           receive_error_;
  };
  outgoing_mutex_.Await(absl::Condition(&cond));

  // There could be a race where send_error_ is set to true after this, but
  // that's OK.
  if (send_error_ || receive_error_) {
    absl::MutexLock status_lock(&status_mutex_);
    return WrapStatus(status_,
                      "Failed to send packet. Message pump thread is down");
  }

  // Put packet into outgoing queue.
  outgoing_packets_byte_size_ += serialize_packet.size();
  outgoing_packets_.push(std::move(serialize_packet));

  return absl::OkStatus();
}

absl::Status MessagePump::ThreadDoSendPacket(Buffer&& serialized_packet) {
  if (receive_error_) {
    // Just eat the packet if there was a receive error as the other side is
    // probably down and won't read packets anymore.
    return absl::OkStatus();
  }

  if (output_handler_) {
    // Redirect output, don't send to socket.
    absl::Status status =
        output_handler_(serialized_packet.data(), serialized_packet.size());
    return WrapStatus(status, "Output handler failed");
  }

  absl::Status status =
      socket_->Send(serialized_packet.data(), serialized_packet.size());
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send packet of size %u",
                      serialized_packet.size());
  }

  LOG_VERBOSE("Sent packet of size %u (total buffer: %u)",
              serialized_packet.size(), outgoing_packets_byte_size_.load());

  return absl::OkStatus();
}

absl::Status MessagePump::ReceiveRawData(Buffer* data) {
  Packet packet;
  absl::Status status = DequeuePacket(&packet);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to dequeue packet");
  }

  if (packet.type != PacketType::kRawData) {
    return MakeStatus("Unexpected packet type %s. Expected kRawData.",
                      PacketTypeName(packet.type));
  }

  *data = std::move(packet.data);
  return absl::OkStatus();
}

absl::Status MessagePump::ReceiveMessage(
    PacketType type, google::protobuf::MessageLite* message) {
  Packet packet;
  absl::Status status = DequeuePacket(&packet);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to dequeue packet");
  }

  if (packet.type != type) {
    return MakeStatus("Unexpected packet type %s. Expected %s.",
                      PacketTypeName(packet.type), PacketTypeName(type));
  }

  if (!message->ParseFromArray(packet.data.data(),
                               static_cast<int>(packet.data.size()))) {
    return MakeStatus("Failed to parse packet of type %s and size %u",
                      PacketTypeName(packet.type), packet.data.size());
  }
  return absl::OkStatus();
}

absl::Status MessagePump::DequeuePacket(Packet* packet) {
  // Wait for a packet to be available.
  absl::MutexLock incoming_lock(&incoming_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(incoming_mutex_) {
    return !incoming_packets_.empty() || send_error_ || receive_error_;
  };
  incoming_mutex_.Await(absl::Condition(&cond));

  // If receive_error_ is true, do not return an error until |incoming_packets_|
  // is empty and all valid packets have been returned. This way, the error
  // shows up for the packet that failed to be received.
  if (send_error_ || (receive_error_ && incoming_packets_.empty())) {
    absl::MutexLock status_lock(&status_mutex_);
    return WrapStatus(status_, "Message pump thread is down");
  }

  // Grab packet from incoming queue.
  *packet = std::move(incoming_packets_.front());
  incoming_packets_.pop();

  // Update byte size.
  incoming_packets_byte_size_ -= kHeaderSize + packet->data.size();

  return absl::OkStatus();
}

absl::Status MessagePump::ThreadDoReceivePacket(Packet* packet) {
  // Read type and size in one go for performance reasons.
  uint8_t header[kHeaderSize];
  absl::Status status = ThreadDoReceive(&header, kHeaderSize);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive packet of size %u",
                      kHeaderSize);
  }

  static_assert(kHeaderSize == 4, "");

  uint8_t packet_type = header[0];
  uint32_t packet_size = static_cast<uint32_t>(header[1]) |
                         (static_cast<uint32_t>(header[2]) << 8) |
                         (static_cast<uint32_t>(header[3]) << 16);

  if (packet_type >= static_cast<uint8_t>(PacketType::kCount)) {
    return MakeStatus("Invalid packet type: %u", packet_type);
  }
  packet->type = static_cast<PacketType>(packet_type);

  if (packet_size > kMaxPacketSize) {
    return MakeStatus("Max packet size exceeded: %u", packet_size);
  }

  packet->data.resize(packet_size);
  status = ThreadDoReceive(packet->data.data(), packet_size);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to read packet data of size %u",
                      packet_size);
  }

  LOG_VERBOSE("Received packet of size %u (total buffer: %u)", packet_size,
              incoming_packets_byte_size_.load());

  return absl::OkStatus();
}

absl::Status MessagePump::ThreadDoReceive(void* buffer, size_t size) {
  if (size == 0) {
    return absl::OkStatus();
  }

  if (input_reader_) {
    size_t bytes_read = 0;
    bool eof = false;
    absl::Status status = input_reader_->Read(buffer, size, &bytes_read, &eof);
    if (eof) {
      input_reader_.reset();
    }
    if (!status.ok()) {
      return status;
    }

    // |input_reader_| should read |size| bytes unless |eof| is hit.
    assert(bytes_read == size || eof);

    // Since this method never reads across packet boundaries and since packets
    // should not be partially received through |input_reader_|, it is an error
    // if there's a partial read on EOF.
    if (eof && (bytes_read > 0 && bytes_read < size)) {
      return MakeStatus("EOF after partial read of %u / %u bytes", bytes_read,
                        size);
    }

    // Special case, might happen if |input_reader_| was an unzip stream and the
    // last read stopped right before zlib's EOF marker. Fall through to reading
    // uncompressed data in that case.
    if (bytes_read == size) {
      return absl::OkStatus();
    }

    assert(eof && bytes_read == 0);
  }

  size_t unused;
  return socket_->Receive(buffer, size, /*allow_partial_read=*/false, &unused);
}

void MessagePump::FlushOutgoingQueue() {
  absl::MutexLock outgoing_lock(&outgoing_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(outgoing_mutex_) {
    return outgoing_packets_byte_size_ == 0 || send_error_ || receive_error_;
  };
  outgoing_mutex_.Await(absl::Condition(&cond));
}

void MessagePump::RedirectInput(std::unique_ptr<InputReader> input_reader) {
  assert(std::this_thread::get_id() == message_receiver_thread_.get_id());
  assert(input_reader);

  if (input_reader_) {
    LOG_WARNING("Input reader already set");
    return;
  }

  input_reader_ = std::move(input_reader);
}

void MessagePump::RedirectOutput(OutputHandler output_handler) {
  FlushOutgoingQueue();
  output_handler_ = std::move(output_handler);
}

size_t MessagePump::GetNumOutgoingPackagesForTesting() {
  absl::MutexLock outgoing_lock(&outgoing_mutex_);
  return outgoing_packets_.size();
}

size_t MessagePump::GetMaxInOutBufferSizeForTesting() {
  return kInOutBufferSize;
}

size_t MessagePump::GetMaxPacketSizeForTesting() { return kMaxPacketSize; }

void MessagePump::ThreadSenderMain() {
  while (!send_error_) {
    Buffer serialized_packet;
    size_t size;
    {
      // Wait for a packet to be available.
      absl::MutexLock outgoing_lock(&outgoing_mutex_);
      auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(outgoing_mutex_) {
        return outgoing_packets_.size() > 0 || shutdown_;
      };
      outgoing_mutex_.Await(absl::Condition(&cond));
      if (shutdown_) {
        break;
      }

      // Grab packet from outgoing queue.
      serialized_packet = std::move(outgoing_packets_.front());
      size = serialized_packet.size();
      outgoing_packets_.pop();
    }

    // Send data. This blocks until all data is submitted.
    absl::Status status = ThreadDoSendPacket(std::move(serialized_packet));
    if (!status.ok()) {
      {
        absl::MutexLock status_lock(&status_mutex_);
        status_ = WrapStatus(status, "Failed to send packet");
      }
      absl::MutexLock outgoing_lock(&outgoing_mutex_);
      absl::MutexLock incoming_lock(&incoming_mutex_);
      send_error_ = true;
      break;
    }

    // Decrease AFTER sending, this is important for FlushOutgoingQueue().
    absl::MutexLock outgoing_lock(&outgoing_mutex_);
    outgoing_packets_byte_size_ -= size;
  }
}

void MessagePump::ThreadReceiverMain() {
  while (!receive_error_) {
    // Wait for a packet to be available.
    {
      absl::MutexLock incoming_lock(&incoming_mutex_);
      auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(incoming_mutex_) {
        return incoming_packets_byte_size_ < kInOutBufferSize || shutdown_;
      };
      incoming_mutex_.Await(absl::Condition(&cond));
      if (shutdown_) {
        break;
      }
    }

    // Receive packet. This blocks until data is available.
    Packet packet;
    absl::Status status = ThreadDoReceivePacket(&packet);
    if (!status.ok()) {
      {
        absl::MutexLock status_lock(&status_mutex_);
        status_ = WrapStatus(status, "Failed to receive packet");
      }
      absl::MutexLock outgoing_lock(&outgoing_mutex_);
      absl::MutexLock incoming_lock(&incoming_mutex_);
      receive_error_ = true;
      break;
    }

    if (packet_received_) {
      packet_received_(packet.type);
    }

    // Queue the packet for receiving.
    absl::MutexLock incoming_lock(&incoming_mutex_);
    incoming_packets_byte_size_ += kHeaderSize + packet.data.size();
    incoming_packets_.push(std::move(packet));
  }
}

}  // namespace cdc_ft
