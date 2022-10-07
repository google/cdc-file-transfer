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

#ifndef CDC_RSYNC_BASE_FAKE_SOCKET_H_
#define CDC_RSYNC_BASE_FAKE_SOCKET_H_

#include <condition_variable>
#include <mutex>

#include "absl/status/status.h"
#include "cdc_rsync/base/socket.h"

namespace cdc_ft {

// Fake socket that receives the same data it sends.
class FakeSocket : public Socket {
 public:
  FakeSocket();
  ~FakeSocket();

  // Socket:
  absl::Status Send(const void* buffer, size_t size) override;  // thread-safe
  absl::Status Receive(void* buffer, size_t size, bool allow_partial_read,
                       size_t* bytes_received) override;  // thread-safe

  void ShutdownSendingEnd();

  // If set to true, blocks on Send() until it is set to false again.
  void SuspendSending(bool suspended);

 private:
  std::mutex data_mutex_;
  std::condition_variable data_cv_;
  std::string data_;
  bool shutdown_ = false;

  bool sending_suspended_ = false;
  std::mutex suspend_mutex_;
  std::condition_variable suspend_cv_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_BASE_FAKE_SOCKET_H_
