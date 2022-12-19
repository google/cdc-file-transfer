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

#include "cdc_rsync_server/server_socket.h"

#include "common/log.h"
#include "common/platform.h"
#include "common/status.h"
#include "common/util.h"

#if PLATFORM_WINDOWS

#include <winsock2.h>

#elif PLATFORM_LINUX

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#endif

namespace cdc_ft {
namespace {

#if PLATFORM_WINDOWS

using SocketType = SOCKET;
using SockAddrType = SOCKADDR;
constexpr SocketType kInvalidSocket = INVALID_SOCKET;
constexpr int kSocketError = SOCKET_ERROR;
constexpr int kSendingEnd = SD_SEND;

constexpr int kErrAgain = WSAEWOULDBLOCK;  // There's no EAGAIN on Windows.
constexpr int kErrWouldBlock = WSAEWOULDBLOCK;
constexpr int kErrAddrInUse = WSAEADDRINUSE;

int GetLastError() { return WSAGetLastError(); }
std::string GetErrorStr(int err) { return Util::GetWin32Error(err); }
void Close(SocketType* socket) {
  if (*socket != kInvalidSocket) {
    closesocket(*socket);
    *socket = kInvalidSocket;
  }
}

// Not necessary on Windows.
#define HANDLE_EINTR(x) (x)

#elif PLATFORM_LINUX

using SocketType = int;
using SockAddrType = sockaddr;
constexpr SocketType kInvalidSocket = -1;
constexpr int kSocketError = -1;
constexpr int kSendingEnd = SHUT_WR;

constexpr int kErrAgain = EAGAIN;
constexpr int kErrWouldBlock = EWOULDBLOCK;
constexpr int kErrAddrInUse = EADDRINUSE;

int GetLastError() { return errno; }
std::string GetErrorStr(int err) { return strerror(err); }
void Close(SocketType* socket) {
  if (*socket != kInvalidSocket) {
    close(*socket);
    *socket = kInvalidSocket;
  }
}

// Keep re-evaluating the expression |x| while it returns EINTR.
#define HANDLE_EINTR(x)                                     \
  ({                                                        \
    decltype(x) eintr_wrapper_result;                       \
    do {                                                    \
      eintr_wrapper_result = (x);                           \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result;                                   \
  })

#endif

std::string GetLastErrorStr() { return GetErrorStr(GetLastError()); }

}  // namespace

struct ServerSocketInfo {
  // Listening socket file descriptor (where new connections are accepted).
  SocketType listen_sock = kInvalidSocket;

  // Connection socket file descriptor (where data is sent to/received from).
  SocketType conn_sock = kInvalidSocket;
};

ServerSocket::ServerSocket()
    : Socket(), socket_info_(std::make_unique<ServerSocketInfo>()) {}

ServerSocket::~ServerSocket() {
  Disconnect();
  StopListening();
}

absl::Status ServerSocket::StartListening(int port) {
  if (socket_info_->listen_sock != kInvalidSocket) {
    return MakeStatus("Already listening");
  }

  LOG_DEBUG("Open socket");
  socket_info_->listen_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_info_->listen_sock == kInvalidSocket) {
    return MakeStatus("Creating listen socket failed: %s", GetLastErrorStr());
  }

  // If the program terminates abnormally, the socket might remain in a
  // TIME_WAIT state and report "address already in use" on bind(). Setting
  // SO_REUSEADDR works around that. See
  // https://hea-www.harvard.edu/~fine/Tech/addrinuse.html
  const int enable = 1;
  int result =
      setsockopt(socket_info_->listen_sock, SOL_SOCKET, SO_REUSEADDR,
                 reinterpret_cast<const char*>(&enable), sizeof(enable));
  if (result == kSocketError) {
    LOG_DEBUG("Enabling address reusal failed");
  }

  LOG_DEBUG("Bind socket");
  sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(port);

  result = bind(socket_info_->listen_sock,
                reinterpret_cast<const SockAddrType*>(&serv_addr),
                sizeof(serv_addr));
  if (result == kSocketError) {
    int err = GetLastError();
    absl::Status status =
        MakeStatus("Binding to port %i failed: %s", port, GetErrorStr(err));
    if (err == kErrAddrInUse) {
      // Happens when two instances are run at the same time. Help callers to
      // print reasonable errors.
      status = SetTag(status, Tag::kAddressInUse);
    }
    Close(&socket_info_->listen_sock);
    return status;
  }

  LOG_DEBUG("Listen");
  result = listen(socket_info_->listen_sock, 1);
  if (result == kSocketError) {
    int err = GetLastError();
    Close(&socket_info_->listen_sock);
    return MakeStatus("Listening to socket failed: %s", GetErrorStr(err));
  }

  return absl::OkStatus();
}

void ServerSocket::StopListening() {
  Close(&socket_info_->listen_sock);
  LOG_INFO("Stopped listening.");
}

absl::Status ServerSocket::WaitForConnection() {
  if (socket_info_->conn_sock != kInvalidSocket) {
    return MakeStatus("Already connected");
  }

  socket_info_->conn_sock = accept(socket_info_->listen_sock, nullptr, nullptr);
  if (socket_info_->conn_sock == kInvalidSocket) {
    return MakeStatus("Accepting connection failed: %s", GetLastErrorStr());
  }

  LOG_DEBUG("Client connected");
  return absl::OkStatus();
}

void ServerSocket::Disconnect() {
  Close(&socket_info_->conn_sock);
  LOG_INFO("Disconnected");
}

absl::Status ServerSocket::ShutdownSendingEnd() {
  int result = shutdown(socket_info_->conn_sock, kSendingEnd);
  if (result == kSocketError) {
    return MakeStatus("Socket shutdown failed: %s", GetLastErrorStr());
  }

  return absl::OkStatus();
}

absl::Status ServerSocket::Send(const void* buffer, size_t size) {
  const char* curr_ptr = reinterpret_cast<const char*>(buffer);
  assert(size <= INT_MAX);
  int bytes_left = static_cast<int>(size);
  while (bytes_left > 0) {
    int bytes_written = HANDLE_EINTR(
        send(socket_info_->conn_sock, curr_ptr, bytes_left, /*flags*/ 0));

    if (bytes_written < 0) {
      const int err = GetLastError();
      if (err == kErrAgain || err == kErrWouldBlock) {
        // Shouldn't happen as the socket should be blocking.
        LOG_DEBUG("Socket would block");
        continue;
      }

      return MakeStatus("Sending to socket failed: %s", GetErrorStr(err));
    }

    bytes_left -= bytes_written;
    curr_ptr += bytes_written;
  }
  return absl::OkStatus();
}

absl::Status ServerSocket::Receive(void* buffer, size_t size,
                                   bool allow_partial_read,
                                   size_t* bytes_received) {
  *bytes_received = 0;
  if (size == 0) {
    return absl::OkStatus();
  }

  char* curr_ptr = static_cast<char*>(buffer);
  assert(size <= INT_MAX);
  int bytes_left = size;
  while (bytes_left > 0) {
    int bytes_read = HANDLE_EINTR(
        recv(socket_info_->conn_sock, curr_ptr, bytes_left, /*flags*/ 0));

    if (bytes_read < 0) {
      const int err = GetLastError();
      if (err == kErrAgain || err == kErrWouldBlock) {
        // Shouldn't happen as the socket should be blocking.
        LOG_DEBUG("Socket would block");
        continue;
      }

      return MakeStatus("Receiving from socket failed: %s", GetErrorStr(err));
    }

    bytes_left -= bytes_read;
    *bytes_received += bytes_read;
    curr_ptr += bytes_read;

    if (bytes_read == 0) {
      // EOF. Make sure we're not in the middle of a message.
      if (bytes_left < static_cast<int>(size)) {
        return MakeStatus("EOF after partial read");
      }

      LOG_DEBUG("EOF() detected");
      return SetTag(MakeStatus("EOF detected"), Tag::kSocketEof);
    }

    if (allow_partial_read) {
      break;
    }
  }
  return absl::OkStatus();
}

}  // namespace cdc_ft
