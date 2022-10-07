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

#ifndef COMMON_CLOCK_H_
#define COMMON_CLOCK_H_

#include <chrono>
#include <string>

namespace cdc_ft {

// Clock that never runs backwards. Useful for stopwatch etc.
class SteadyClock {
 public:
  using Timestamp = std::chrono::time_point<std::chrono::steady_clock>;
  virtual ~SteadyClock() = default;
  virtual Timestamp Now() const = 0;
};

class DefaultSteadyClock : public SteadyClock {
 public:
  static DefaultSteadyClock* GetInstance();
  Timestamp Now() const override;
};

// Clock that matches the system's clock.
class SystemClock {
 public:
  using Timestamp = std::chrono::time_point<std::chrono::system_clock>;
  virtual ~SystemClock() = default;
  virtual Timestamp Now() const = 0;

  // Formats the current timestamp. |format| is the format according to the
  // std::put_time specification, see
  //   https://en.cppreference.com/w/cpp/io/manip/put_time.
  // If |append_millis| is true, appends the milliseconds formatted as %03i.
  std::string FormatNow(const char* format, bool append_millis) const;
};

class DefaultSystemClock : public SystemClock {
 public:
  static DefaultSystemClock* GetInstance();
  Timestamp Now() const override;
};

}  // namespace cdc_ft

#endif  // COMMON_CLOCK_H_
