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

#ifndef CDC_RSYNC_CLIENT_SOCKET_H_
#define CDC_RSYNC_CLIENT_SOCKET_H_

#include <memory>

#include "absl/status/status.h"
#include "cdc_rsync/base/socket.h"

namespace cdc_ft {

class ClientSocket : public Socket {
 public:
  ClientSocket();
  ~ClientSocket();

  // Connects to localhost on |port|.
  absl::Status Connect(int port);

  // Disconnects again. No-op if not connected.
  void Disconnect();

  // Shuts down the sending end of the socket. This will interrupt any receive
  // calls on the server and shut it down.
  absl::Status ShutdownSendingEnd();

  // Socket:
  absl::Status Send(const void* buffer, size_t size) override;
  absl::Status Receive(void* buffer, size_t size, bool allow_partial_read,
                       size_t* bytes_received) override;

 private:
  std::unique_ptr<struct ClientSocketInfo> socket_info_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_CLIENT_SOCKET_H_
