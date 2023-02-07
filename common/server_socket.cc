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

#include "common/server_socket.h"

#include "common/log.h"
#include "common/platform.h"
#include "common/status.h"
#include "common/util.h"

#if PLATFORM_WINDOWS

#include <winsock2.h>
#include <ws2tcpip.h>

#elif PLATFORM_LINUX

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
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

class AddrInfoReleaser {
 public:
  AddrInfoReleaser(addrinfo* addr_infos) : addr_infos_(addr_infos) {}
  ~AddrInfoReleaser() { freeaddrinfo(addr_infos_); }

 private:
  addrinfo* addr_infos_;
};

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

// static
absl::StatusOr<int> ServerSocket::FindAvailablePort() {
  ServerSocket socket;
  return socket.StartListening(0);
}

absl::StatusOr<int> ServerSocket::StartListening(int port) {
  if (socket_info_->listen_sock != kInvalidSocket) {
    return MakeStatus("Already listening");
  }

  // Find addrinfos suitable for listening via IPV4 and IPV6.
  addrinfo hints;
  addrinfo* addr_infos = nullptr;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = PF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  // AI_PASSIVE indicates that the addresses are used with bind(). The returned
  // addresses will be the unspecified addresses for each family.
  hints.ai_flags = AI_NUMERICHOST | AI_PASSIVE;
  int result = getaddrinfo(/*address=*/nullptr, std::to_string(port).c_str(),
                           &hints, &addr_infos);
  if (result != 0) {
    return MakeStatus("Getting address infos failed: %s", GetLastErrorStr());
  }
  AddrInfoReleaser releaser(addr_infos);

  // Prefer IPV6 sockets. They can also accept IPV4 connections.
  for (addrinfo* curr = addr_infos; curr; curr = curr->ai_next) {
    if (curr->ai_family == PF_INET6) {
      return StartListeningInternal(port, curr);
    }
  }

  // Fall back to IPV4 sockets.
  for (addrinfo* curr = addr_infos; curr; curr = curr->ai_next) {
    if (curr->ai_family == PF_INET) {
      return StartListeningInternal(port, curr);
    }
  }

  return MakeStatus("No IPV4 and IPV6 network addresses available");
}

absl::StatusOr<int> ServerSocket::StartListeningInternal(int port,
                                                         addrinfo* addr) {
  assert(addr->ai_family == PF_INET || addr->ai_family == PF_INET6);
  const char* family = addr->ai_family == PF_INET ? "IPV4" : "IPV6";

  // Open a socket with the correct address family for this address.
  LOG_DEBUG("Open %s listen socket", family);
  socket_info_->listen_sock =
      socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (socket_info_->listen_sock == kInvalidSocket) {
    return MakeStatus("Creating %s listen socket failed: %s", family,
                      GetLastErrorStr());
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

  // Allow ipv4 connections on the ipv6 socket. By default, ipv6 sockets only
  // allow ipv4 connections on Windows.
  if (addr->ai_family == PF_INET6) {
    const int disable = 0;
    result =
        setsockopt(socket_info_->listen_sock, IPPROTO_IPV6, IPV6_V6ONLY,
                   reinterpret_cast<const char*>(&disable), sizeof(disable));
    if (result == kSocketError) {
      LOG_DEBUG("Disabling IPV6-only failed");
    }
  }

  LOG_DEBUG("Bind socket");
  result = bind(socket_info_->listen_sock, addr->ai_addr,
                static_cast<int>(addr->ai_addrlen));
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

  if (port == 0) {
    // Find out which port was auto-selected.
    socklen_t len = addr->ai_addrlen;
    result = getsockname(socket_info_->listen_sock, addr->ai_addr, &len);
    if (result == kSocketError) {
      Close(&socket_info_->listen_sock);
      return MakeStatus("Getting port failed: %s", GetLastErrorStr());
    }
    if (addr->ai_family == PF_INET) {
      port = ntohs(reinterpret_cast<sockaddr_in*>(addr->ai_addr)->sin_port);
    } else if (addr->ai_family == PF_INET6) {
      port = ntohs(reinterpret_cast<sockaddr_in6*>(addr->ai_addr)->sin6_port);
    }
  }

  LOG_DEBUG("Listen");
  result = listen(socket_info_->listen_sock, 1);
  if (result == kSocketError) {
    int err = GetLastError();
    Close(&socket_info_->listen_sock);
    return MakeStatus("Listening to socket failed: %s", GetErrorStr(err));
  }

  return port;
}

void ServerSocket::StopListening() {
  Close(&socket_info_->listen_sock);
  LOG_DEBUG("Stopped listening.");
}

absl::Status ServerSocket::WaitForConnection() {
  if (socket_info_->conn_sock != kInvalidSocket) {
    return MakeStatus("Already connected");
  }
  if (socket_info_->listen_sock == kInvalidSocket) {
    return MakeStatus("Not listening");
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
  LOG_DEBUG("Disconnected");
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
