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

#include "common/testing_clock.h"

#include "common/stopwatch.h"

namespace cdc_ft {

TestingSteadyClock::Timestamp TestingSteadyClock::Now() const {
  const Timestamp prev_now = now_;
  now_ += std::chrono::milliseconds(auto_advance_ms_);
  return prev_now;
}

void TestingSteadyClock::Advance(int milliseconds) {
  now_ += std::chrono::milliseconds(milliseconds);
}

void TestingSteadyClock::AutoAdvance(int milliseconds) {
  auto_advance_ms_ = milliseconds;
}

TestingSystemClock::TestingSystemClock()
    : now_(std::chrono::system_clock::now()) {}

TestingSystemClock::Timestamp TestingSystemClock::Now() const { return now_; }

void TestingSystemClock::Advance(int milliseconds) {
  now_ += std::chrono::milliseconds(milliseconds);
}

}  // namespace cdc_ft
