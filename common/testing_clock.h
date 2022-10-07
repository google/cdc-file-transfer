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

#ifndef COMMON_TESTING_CLOCK_H_
#define COMMON_TESTING_CLOCK_H_

#include "common/clock.h"

namespace cdc_ft {

class TestingSteadyClock : public SteadyClock {
 public:
  // SteadyClock:
  Timestamp Now() const override;

  // Explicitly advances the timestamp by the given number of |milliseconds|.
  void Advance(int milliseconds);

  // Sets a time that is added to the current time after every Now() call.
  void AutoAdvance(int milliseconds);

 private:
  mutable Timestamp now_;
  int auto_advance_ms_ = 0;
};

class TestingSystemClock : public SystemClock {
 public:
  TestingSystemClock();

  // SystemClock:
  Timestamp Now() const override;

  // Explicitly advances the timestamp by the given number of |milliseconds|.
  void Advance(int milliseconds);

 private:
  Timestamp now_;
};

}  // namespace cdc_ft

#endif  // COMMON_TESTING_CLOCK_H_
