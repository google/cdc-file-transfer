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

#include "common/semaphore.h"

namespace cdc_ft {

Semaphore::Semaphore(int initial_count) : count_(initial_count) {}

Semaphore::~Semaphore() = default;

void Semaphore::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  condition_.wait(lock, [&]() { return count_ > 0; });
  count_--;
}

void Semaphore::Signal() {
  std::lock_guard<std::mutex> lock(mutex_);
  count_++;
  condition_.notify_one();
}

}  // namespace cdc_ft
