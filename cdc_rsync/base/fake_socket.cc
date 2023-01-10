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

#include "cdc_rsync/base/fake_socket.h"

namespace cdc_ft {

FakeSocket::FakeSocket() = default;

FakeSocket::~FakeSocket() = default;

absl::Status FakeSocket::Send(const void* buffer, size_t size) {
  // Wait until we can send again.
  std::unique_lock<std::mutex> suspend_lock(suspend_mutex_);
  suspend_cv_.wait(suspend_lock, [this]() { return !sending_suspended_; });
  suspend_lock.unlock();

  std::unique_lock<std::mutex> lock(data_mutex_);
  data_.append(static_cast<const char*>(buffer), size);
  lock.unlock();
  data_cv_.notify_all();
  return absl::OkStatus();
}

absl::Status FakeSocket::Receive(void* buffer, size_t size,
                                 bool allow_partial_read,
                                 size_t* bytes_received) {
  *bytes_received = 0;
  std::unique_lock<std::mutex> lock(data_mutex_);
  data_cv_.wait(lock, [this, size, allow_partial_read]() {
    size_t min_size = allow_partial_read ? 1 : size;
    return data_.size() >= min_size || shutdown_;
  });
  if (shutdown_) {
    return absl::UnavailableError("Pipe is shut down");
  }
  size_t to_copy = std::min(size, data_.size());
  memcpy(buffer, data_.data(), to_copy);
  *bytes_received = to_copy;

  // This is horribly inefficent, but should be OK in a fake.
  data_.erase(0, to_copy);
  return absl::OkStatus();
}

void FakeSocket::ShutdownSendingEnd() {
  std::unique_lock<std::mutex> lock(data_mutex_);
  shutdown_ = true;
  lock.unlock();
  data_cv_.notify_all();
}

void FakeSocket::SuspendSending(bool suspended) {
  std::unique_lock<std::mutex> lock(suspend_mutex_);
  sending_suspended_ = suspended;
  lock.unlock();
  suspend_cv_.notify_all();
}

}  // namespace cdc_ft
