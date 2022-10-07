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

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "common/log.h"
#include "common/status.h"

namespace cdc_ft {

namespace {

int kInvalidFd = -1;

// Keep re-evaluating the expression |x| while it returns EINTR.
#define HANDLE_EINTR(x)                                     \
  ({                                                        \
    decltype(x) eintr_wrapper_result;                       \
    do {                                                    \
      eintr_wrapper_result = (x);                           \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result;                                   \
  })

}  // namespace

ServerSocket::ServerSocket()
    : Socket(), listen_sockfd_(kInvalidFd), conn_sockfd_(kInvalidFd) {}

ServerSocket::~ServerSocket() {
  Disconnect();
  StopListening();
}

absl::Status ServerSocket::StartListening(int port) {
  if (listen_sockfd_ != kInvalidFd) {
    return MakeStatus("Already listening");
  }

  LOG_DEBUG("Open socket");
  listen_sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_sockfd_ < 0) {
    listen_sockfd_ = kInvalidFd;
    return MakeStatus("socket() failed: %s", strerror(errno));
  }

  // If the program terminates abnormally, the socket might remain in a
  // TIME_WAIT state and report "address already in use" on bind(). Setting
  // SO_REUSEADDR works around that. See
  // https://hea-www.harvard.edu/~fine/Tech/addrinuse.html
  int enable = 1;
  if (setsockopt(listen_sockfd_, SOL_SOCKET, SO_REUSEADDR, &enable,
                 sizeof(enable)) < 0) {
    LOG_DEBUG("setsockopt() failed");
  }

  LOG_DEBUG("Bind socket");
  sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(port);
  if (bind(listen_sockfd_, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) <
      0) {
    absl::Status status =
        MakeStatus("bind() to port %i failed: %s", port, strerror(errno));
    if (errno == EADDRINUSE) {
      // Happens when two instances are run at the same time. Help callers to
      // print reasonable errors.
      status = SetTag(status, Tag::kAddressInUse);
    }
    close(listen_sockfd_);
    listen_sockfd_ = kInvalidFd;

    return status;
  }

  LOG_DEBUG("Listen");
  listen(listen_sockfd_, 1);
  return absl::OkStatus();
}

void ServerSocket::StopListening() {
  if (listen_sockfd_ != kInvalidFd) {
    close(listen_sockfd_);
    listen_sockfd_ = kInvalidFd;
  }

  LOG_INFO("Stopped listening.");
}

absl::Status ServerSocket::WaitForConnection() {
  if (conn_sockfd_ != kInvalidFd) {
    return MakeStatus("Already connected");
  }

  sockaddr_in cli_addr;
  socklen_t cli_len = sizeof(cli_addr);
  conn_sockfd_ = accept(listen_sockfd_, (struct sockaddr*)&cli_addr, &cli_len);
  if (conn_sockfd_ < 0) {
    conn_sockfd_ = kInvalidFd;
    return MakeStatus("accept() failed: %s", strerror(errno));
  }

  LOG_DEBUG("Client connected");
  return absl::OkStatus();
}

void ServerSocket::Disconnect() {
  if (conn_sockfd_ != kInvalidFd) {
    close(conn_sockfd_);
    conn_sockfd_ = kInvalidFd;
  }

  LOG_INFO("Disconnected");
}

absl::Status ServerSocket::ShutdownSendingEnd() {
  int result = shutdown(conn_sockfd_, SHUT_WR);
  if (result != 0) {
    return MakeStatus("shutdown() failed: %s", strerror(errno));
  }

  return absl::OkStatus();
}

absl::Status ServerSocket::Send(const void* buffer, size_t size) {
  const uint8_t* curr_ptr = reinterpret_cast<const uint8_t*>(buffer);
  ssize_t bytes_left = size;
  while (bytes_left > 0) {
    ssize_t bytes_written =
        HANDLE_EINTR(send(conn_sockfd_, curr_ptr, bytes_left, /*flags*/ 0));

    if (bytes_written < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Shouldn't happen as the socket should be blocking.
        LOG_DEBUG("Socket would block");
        continue;
      }

      return MakeStatus("write() to fd %i failed: %s", conn_sockfd_,
                        strerror(errno));
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

  uint8_t* curr_ptr = reinterpret_cast<uint8_t*>(buffer);
  ssize_t bytes_left = size;
  while (bytes_left > 0) {
    ssize_t bytes_read =
        HANDLE_EINTR(recv(conn_sockfd_, curr_ptr, bytes_left, /*flags*/ 0));

    if (bytes_read < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Shouldn't happen as the socket should be blocking.
        LOG_DEBUG("Socket would block");
        continue;
      }

      return MakeStatus("recv() from fd %i failed: %s", conn_sockfd_,
                        strerror(errno));
    }

    bytes_left -= bytes_read;
    *bytes_received += bytes_read;
    curr_ptr += bytes_read;

    if (bytes_read == 0) {
      // EOF. Make sure we're not in the middle of a message.
      if (bytes_left < static_cast<ssize_t>(size)) {
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
