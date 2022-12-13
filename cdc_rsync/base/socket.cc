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

#include "cdc_rsync/base/socket.h"

#include "common/log.h"
#include "common/platform.h"
#include "common/status.h"
#include "common/util.h"

#if PLATFORM_WINDOWS
#include <winsock2.h>
#endif

namespace cdc_ft {

// static
absl::Status Socket::Initialize() {
#if PLATFORM_WINDOWS
  WSADATA wsaData;
  const int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
  if (result != 0) {
    return MakeStatus("WSAStartup() failed: %s", Util::GetWin32Error(result));
  }
  return absl::OkStatus();
#elif PLATFORM_LINUX
  return absl::OkStatus();
#endif
}

// static
absl::Status Socket::Shutdown() {
#if PLATFORM_WINDOWS
  const int result = WSACleanup();
  if (result == SOCKET_ERROR) {
    return MakeStatus("WSACleanup() failed: %s",
                      Util::GetWin32Error(WSAGetLastError()));
  }
  return absl::OkStatus();
#elif PLATFORM_LINUX
  return absl::OkStatus();
#endif
}

SocketFinalizer::~SocketFinalizer() {
  absl::Status status = Socket::Shutdown();
  if (!status.ok()) {
    LOG_ERROR("Socket shutdown failed: %s", status.message())
  }
};

}  // namespace cdc_ft
