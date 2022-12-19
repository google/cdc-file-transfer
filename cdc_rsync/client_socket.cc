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

#include "cdc_rsync/client_socket.h"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <cassert>

#include "common/log.h"
#include "common/status.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

// Creates a status with the given |message| and the last WSA error.
// Assigns Tag::kSocketEof for WSAECONNRESET errors.
absl::Status MakeSocketStatus(const char* message) {
  const int err = WSAGetLastError();
  absl::Status status = MakeStatus("%s: %s", message, Util::GetWin32Error(err));
  if (err == WSAECONNRESET) {
    status = SetTag(status, Tag::kSocketEof);
  }
  return status;
}

}  // namespace

struct ClientSocketInfo {
  SOCKET socket;

  ClientSocketInfo() : socket(INVALID_SOCKET) {}
};

ClientSocket::ClientSocket() = default;

ClientSocket::~ClientSocket() { Disconnect(); }

absl::Status ClientSocket::Connect(int port) {
  addrinfo hints;
  ZeroMemory(&hints, sizeof(hints));
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

  socket_info_ = std::make_unique<ClientSocketInfo>();
  int count = 0;
  for (addrinfo* curr = addr_infos; curr; curr = curr->ai_next, count++) {
    socket_info_->socket =
        socket(addr_infos->ai_family, addr_infos->ai_socktype,
               addr_infos->ai_protocol);
    if (socket_info_->socket == INVALID_SOCKET) {
      LOG_DEBUG("socket() failed for addr_info %i: %s", count,
                Util::GetWin32Error(WSAGetLastError()).c_str());
      continue;
    }

    // Connect to server.
    result = connect(socket_info_->socket, curr->ai_addr,
                     static_cast<int>(curr->ai_addrlen));
    if (result == SOCKET_ERROR) {
      LOG_DEBUG("connect() failed for addr_info %i: %i", count, result);
      closesocket(socket_info_->socket);
      socket_info_->socket = INVALID_SOCKET;
      continue;
    }

    // Success!
    break;
  }

  freeaddrinfo(addr_infos);

  if (socket_info_->socket == INVALID_SOCKET) {
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

  if (socket_info_->socket != INVALID_SOCKET) {
    closesocket(socket_info_->socket);
    socket_info_->socket = INVALID_SOCKET;
  }

  socket_info_.reset();
}

absl::Status ClientSocket::Send(const void* buffer, size_t size) {
  int result = send(socket_info_->socket, static_cast<const char*>(buffer),
                    static_cast<int>(size), /*flags */ 0);
  if (result == SOCKET_ERROR) {
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
  int bytes_read = recv(socket_info_->socket, static_cast<char*>(buffer),
                        static_cast<int>(size), flags);
  if (bytes_read == SOCKET_ERROR) {
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
  int result = shutdown(socket_info_->socket, SD_SEND);
  if (result == SOCKET_ERROR) {
    return MakeSocketStatus("shutdown() failed");
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
