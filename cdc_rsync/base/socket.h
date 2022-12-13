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

#ifndef CDC_RSYNC_BASE_SOCKET_H_
#define CDC_RSYNC_BASE_SOCKET_H_

#include "absl/status/status.h"

namespace cdc_ft {

class Socket {
 public:
  Socket() = default;
  virtual ~Socket() = default;

  // Calls WSAStartup() on Windows, no-op on Linux.
  // Must be called before using sockets.
  static absl::Status Initialize();

  // Calls WSACleanup() on Windows, no-op on Linux.
  // Must be called after using sockets.
  static absl::Status Shutdown();

  // Send data to the socket.
  virtual absl::Status Send(const void* buffer, size_t size) = 0;

  // Receives data from the socket. Blocks until data is available or the
  // sending end of the socket gets shut down by the sender.
  // If |allow_partial_read| is false, blocks until |size| bytes are available.
  // If |allow_partial_read| is true, may return with success if less than
  // |size| (but more than 0) bytes were received.
  // The number of bytes written to |buffer| is returned in |bytes_received|.
  virtual absl::Status Receive(void* buffer, size_t size,
                               bool allow_partial_read,
                               size_t* bytes_received) = 0;
};

// Convenience class that calls Shutdown() on destruction. Logs on errors.
class SocketFinalizer {
 public:
  ~SocketFinalizer();
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_BASE_SOCKET_H_
