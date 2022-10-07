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

#ifndef COMMON_STOPWATCH_H_
#define COMMON_STOPWATCH_H_

#include <chrono>

#include "absl/time/time.h"
#include "common/clock.h"

namespace cdc_ft {

class Stopwatch {
 public:
  using Timestamp = SteadyClock::Timestamp;

  explicit Stopwatch(SteadyClock* clock = DefaultSteadyClock::GetInstance());
  ~Stopwatch();

  // Returns the elapsed time (in seconds) relative to construction or to the
  // last time when Reset() was called.
  double ElapsedSeconds() const;

  // Resets the elapsed time to zero.
  void Reset();

  // Returns the duration relative to construction or to the last time when
  // Reset() was called.
  absl::Duration Elapsed() const;

 private:
  SteadyClock* clock_;
  Timestamp start_;
};

}  // namespace cdc_ft

#endif  // COMMON_STOPWATCH_H_
