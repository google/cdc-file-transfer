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

#ifndef COMMON_THREAD_SAFE_MAP_H_
#define COMMON_THREAD_SAFE_MAP_H_

#include <unordered_map>

#include "absl/synchronization/mutex.h"

namespace cdc_ft {

// A map with keys of type |key_type| and values of type |mapped_type|.
template <class key_type, class mapped_type>
class ThreadSafeMap {
 public:
  // Returns a value by the |key|. If the |key| is not present,
  // returns a default value of the `mapped_type`.
  mapped_type Get(const key_type& key) const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock lock(&mutex_);
    auto it = kv_.find(key);
    return it == kv_.end() ? mapped_type() : it->second;
  }

  // Sets |value| to |key|.
  void Set(const key_type& key, const mapped_type& value)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::WriterMutexLock lock(&mutex_);
    kv_[key] = value;
  }

 private:
  mutable absl::Mutex mutex_;
  std::unordered_map<key_type, mapped_type> kv_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace cdc_ft

#endif  // COMMON_THREAD_SAFE_MAP_H_
