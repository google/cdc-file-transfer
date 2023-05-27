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

#include "common/client_socket.h"

#include <cassert>

#include "common/log.h"
#include "common/socket_internal.h"
#include "common/status.h"
#include "common/stopwatch.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

// Creates a status with the given |message| and the last WSA error.
// Assigns Tag::kSocketEof for WSAECONNRESET errors.
absl::Status MakeSocketStatus(const char* message) {
  const int err = GetLastError();
  absl::Status status = MakeStatus("%s: %s", message, GetErrorStr(err));
  if (err == kErrConnReset) {
    status = SetTag(status, Tag::kSocketEof);
  }
  return status;
}

}  // namespace

struct ClientSocketInfo {
  SocketType socket;

  ClientSocketInfo() : socket(kInvalidSocket) {}
};

ClientSocket::ClientSocket() = default;

ClientSocket::~ClientSocket() { Disconnect(); }

// static
absl::Status ClientSocket::WaitForConnection(int port, absl::Duration timeout) {
  assert(port != 0);
  Stopwatch timeout_timer;
  ClientSocket socket;
  for (;;) {
    absl::Status status = socket.Connect(port);
    if (status.ok()) {
      return absl::OkStatus();
    }

    if (timeout_timer.Elapsed() > timeout) {
      return SetTag(
          absl::DeadlineExceededError("Timeout while connecting to server"),
          Tag::kConnectionTimeout);
    }

    Util::Sleep(50);
  }
}

absl::Status ClientSocket::Connect(int port) {
  addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  // Resolve the server address and port.
  addrinfo* addr_infos = nullptr;
  int result = getaddrinfo("localhost", std::to_string(port).c_str(), &hints,
                           &addr_infos);
  if (result != 0) {
    return MakeStatus("getaddrinfo() failed: %i", result);
  }
  AddrInfoReleaser releaser(addr_infos);

  socket_info_ = std::make_unique<ClientSocketInfo>();
  int count = 0;
  for (addrinfo* curr = addr_infos; curr; curr = curr->ai_next, count++) {
    socket_info_->socket =
        socket(curr->ai_family, curr->ai_socktype, curr->ai_protocol);
    if (socket_info_->socket == kInvalidSocket) {
      LOG_DEBUG("socket() failed for addr_info %i: %s", count,
                GetLastErrorStr());
      continue;
    }

    // Connect to server.
    result = connect(socket_info_->socket, curr->ai_addr,
                     static_cast<int>(curr->ai_addrlen));
    if (result == kSocketError) {
      LOG_DEBUG("connect() failed for addr_info %i: %s", count,
                GetLastErrorStr());
      Close(&socket_info_->socket);
      continue;
    }

    // Success!
    break;
  }

  if (socket_info_->socket == kInvalidSocket) {
    socket_info_.reset();
    return MakeStatus("Unable to connect to port %i", port);
  }

  LOG_INFO("Client socket connected to port %i", port);
  return absl::OkStatus();
}

void ClientSocket::Disconnect() {
  if (!socket_info_) {
    return;
  }

  Close(&socket_info_->socket);
  socket_info_.reset();
}

absl::Status ClientSocket::Send(const void* buffer, size_t size) {
  int result =
      HANDLE_EINTR(send(socket_info_->socket, static_cast<const char*>(buffer),
                        static_cast<int>(size), /*flags */ 0));
  if (result == kSocketError) {
    return MakeSocketStatus("send() failed");
  }

  return absl::OkStatus();
}

absl::Status ClientSocket::Receive(void* buffer, size_t size,
                                   bool allow_partial_read,
                                   size_t* bytes_received) {
  *bytes_received = 0;
  if (size == 0) {
    return absl::OkStatus();
  }

  int flags = allow_partial_read ? 0 : MSG_WAITALL;
  int bytes_read =
      HANDLE_EINTR(recv(socket_info_->socket, static_cast<char*>(buffer),
                        static_cast<int>(size), flags));
  if (bytes_read == kSocketError) {
    return MakeSocketStatus("recv() failed");
  }

  if (bytes_read == 0) {
    // EOF
    return SetTag(MakeStatus("EOF detected"), Tag::kSocketEof);
  }

  if (bytes_read != size && !allow_partial_read) {
    // Can this happen?
    return MakeStatus("Partial read");
  }

  *bytes_received = bytes_read;
  return absl::OkStatus();
}

absl::Status ClientSocket::ShutdownSendingEnd() {
  int result = shutdown(socket_info_->socket, kSendingEnd);
  if (result == kSocketError) {
    return MakeStatus("Socket shutdown failed: %s", GetLastErrorStr());
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
