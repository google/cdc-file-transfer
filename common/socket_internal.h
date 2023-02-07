/*
 * Copyright 2023 Google LLC
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

#ifndef COMMON_SOCKET_INTERNAL_H_

#include "common/platform.h"
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

// Platform-specific abstractions for socket classes.

#if PLATFORM_WINDOWS

using SocketType = SOCKET;
using SockAddrType = SOCKADDR;
constexpr SocketType kInvalidSocket = INVALID_SOCKET;
constexpr int kSocketError = SOCKET_ERROR;
constexpr int kSendingEnd = SD_SEND;

constexpr int kErrAgain = WSAEWOULDBLOCK;  // There's no EAGAIN on Windows.
constexpr int kErrWouldBlock = WSAEWOULDBLOCK;
constexpr int kErrAddrInUse = WSAEADDRINUSE;
constexpr int kErrConnReset = WSAECONNRESET;

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
constexpr int kErrConnReset = ECONNRESET;

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
}  // namespace cdc_ft

#endif  // COMMON_SOCKET_INTERNAL_H_
