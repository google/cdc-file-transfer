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

#ifndef COMMON_STATS_COLLECTOR_H_
#define COMMON_STATS_COLLECTOR_H_

#include <memory>

namespace cdc_ft {

class StatsCollector {
 public:
  StatsCollector();
  virtual ~StatsCollector();

  // Initializes the stats collector. If not called, the instance returned from
  // Instance() is a no-op stats collector where all calls have no effect.
  static void Initialize();

  // Shuts down the stats collector. Should be called during shutdown if
  // Initialize() was called during startup.
  static void Shutdown();

  // Returns a no-op stats collector if Initialize() has not been called.
  static StatsCollector* Instance();

  class DurationScope {
   public:
    virtual ~DurationScope();
  };

  virtual void IncCounter(const char* name, size_t value = 1) = 0;
  virtual std::unique_ptr<DurationScope> RecordDuration(const char* name) = 0;

 private:
  static StatsCollector* instance_;
};

}  // namespace cdc_ft

#endif  // COMMON_STATS_COLLECTOR_H_
