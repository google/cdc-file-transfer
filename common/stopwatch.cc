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

#include "common/stopwatch.h"

namespace cdc_ft {

Stopwatch::Stopwatch(SteadyClock* clock) : clock_(clock) { Reset(); }

Stopwatch::~Stopwatch() = default;

double Stopwatch::ElapsedSeconds() const {
  // Assigning to a duration<double> automagically converts to seconds.
  const std::chrono::duration<double> elapsed = clock_->Now() - start_;
  return elapsed.count();
}

void Stopwatch::Reset() { start_ = clock_->Now(); }

absl::Duration Stopwatch::Elapsed() const {
  return absl::FromChrono(clock_->Now() - start_);
}

}  // namespace cdc_ft
