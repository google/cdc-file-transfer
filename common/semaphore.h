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

#ifndef COMMON_SEMAPHORE_H_
#define COMMON_SEMAPHORE_H_

#include <condition_variable>
#include <mutex>

namespace cdc_ft {

// Standard semaphore. Can be replaced by std::counting_semaphore in C++20.
// Not fair! Wait() does not unlock in FIFO order.
class Semaphore {
 public:
  // Initialize the semaphore with |initial_count|. The count indicates how
  // often Wait() can be called without blocking. There is no max count.
  explicit Semaphore(int initial_count);
  ~Semaphore();

  // If the count is larger than zero, decreases the count and returns
  // immediately. If the count is zero, blocks until some other thread calls
  // Signal() and decreases the count.
  // This method is thread-safe.
  void Wait();

  // Increases the count.
  // This method is thread-safe.
  void Signal();

 private:
  uint32_t count_;
  std::mutex mutex_;
  std::condition_variable condition_;
};

}  // namespace cdc_ft

#endif  // COMMON_SEMAPHORE_H_
