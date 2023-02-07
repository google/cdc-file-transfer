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

#ifndef COMMON_SERVER_SOCKET_H_
#define COMMON_SERVER_SOCKET_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/socket.h"

struct addrinfo;

namespace cdc_ft {

class ServerSocket : public Socket {
 public:
  ServerSocket();
  ~ServerSocket();

  // Returns an available ephemeral port that can be used as a listening port.
  // Note that calling this function, followed by StartListening() or similar,
  // is slightly racy as another process might use the port in the meantime.
  // However, the OS usually returns ephemeral ports in a round-robin manner,
  // and ports remain in TIME_WAIT state for a while, which may block other apps
  // from reusing the port. Hence, the chances of races are small. Nevertheless,
  // consider calling StartListening() with zero |port| if possible.
  static absl::StatusOr<int> FindAvailablePort();

  // Starts listening for connections on |port|.
  // Passing 0 as port will bind to any available port.
  // Returns the port that was bound to.
  absl::StatusOr<int> StartListening(int port);

  // Stops listening for connections. No-op if already stopped/never started.
  void StopListening();

  // Waits for a client to connect. Only supports one connection. Repeating
  // the call with an existing connection results in an error.
  absl::Status WaitForConnection();

  // Disconnects the client. No-op if not connected.
  void Disconnect();

  // Shuts down the sending end of the socket. This will interrupt any receive
  // calls on the client and shut it down.
  absl::Status ShutdownSendingEnd();

  // Socket:
  absl::Status Send(const void* buffer, size_t size) override;
  absl::Status Receive(void* buffer, size_t size, bool allow_partial_read,
                       size_t* bytes_received) override;

 private:
  // Called by StartListening() for a specific IPV4 or IPV6 |addr_info|.
  // Passing 0 as port will bind to any available port.
  // Returns the port that was bound to.
  absl::StatusOr<int> StartListeningInternal(int port, addrinfo* addr);

  std::unique_ptr<struct ServerSocketInfo> socket_info_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_SERVER_SOCKET_H_
