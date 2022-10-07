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

#include "common/clock.h"

#include "absl/strings/str_format.h"
#include "common/platform.h"

namespace cdc_ft {

// static
DefaultSteadyClock* DefaultSteadyClock::GetInstance() {
  static DefaultSteadyClock instance;
  return &instance;
}

SteadyClock::Timestamp DefaultSteadyClock::Now() const {
  return std::chrono::steady_clock::now();
}

std::string SystemClock::FormatNow(const char* format,
                                   bool append_millis) const {
  const Timestamp now = Now();
  const std::time_t now_t = std::chrono::system_clock::to_time_t(now);
  tm now_tm;
#if PLATFORM_WINDOWS
  localtime_s(&now_tm, &now_t);
#elif PLATFORM_LINUX
  localtime_r(&now_t, &now_tm);
#endif
  std::stringstream ss;
  ss << std::put_time(&now_tm, format);

  if (append_millis) {
    const int millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - std::chrono::system_clock::from_time_t(now_t))
                           .count();
    ss << std::setfill('0') << std::setw(3) << millis;
  }
  return ss.str();
}

// static
DefaultSystemClock* DefaultSystemClock::GetInstance() {
  static DefaultSystemClock instance;
  return &instance;
}

SystemClock::Timestamp DefaultSystemClock::Now() const {
  return std::chrono::system_clock::now();
}

}  // namespace cdc_ft
