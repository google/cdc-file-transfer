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

#include "common/stats_collector.h"

#include <map>
#include <thread>

#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "common/log.h"
#include "common/stopwatch.h"

namespace cdc_ft {
namespace {
constexpr absl::Duration kStatsPrintPeriod = absl::Seconds(1);
}

class StatsCollectorImpl : public StatsCollector {
 public:
  class DurationScopeImpl : public DurationScope {
   public:
    DurationScopeImpl(StatsCollectorImpl* stats_collector, const char* name)
        : stats_collector_(stats_collector), name_(name) {
      assert(stats_collector_);
    }

    ~DurationScopeImpl() override {
      stats_collector_->DoRecordDuration(name_, stopwatch_.Elapsed());
    }

   private:
    StatsCollectorImpl* stats_collector_;
    const char* name_;
    Stopwatch stopwatch_;
  };

  StatsCollectorImpl() {
    print_thread_ = std::thread([this]() { ThreadPrintMain(); });
  }

  ~StatsCollectorImpl() override {
    {
      absl::MutexLock lock(&mutex_);
      shutdown_ = true;
    }
    if (print_thread_.joinable()) {
      print_thread_.join();
    }
  }

  void IncCounter(const char* name, size_t value) override
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    counters_[name] += value;
  }

  std::unique_ptr<DurationScope> RecordDuration(const char* name) override {
    return std::make_unique<DurationScopeImpl>(this, name);
  }

 private:
  void DoRecordDuration(const char* name, absl::Duration duration)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    durations_[std::move(name)] += duration;
  }

  void ThreadPrintMain() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    while (!shutdown_) {
      Stopwatch sw;
      auto cond = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
        return shutdown_ || sw.Elapsed() >= kStatsPrintPeriod;
      };
      mutex_.Await(absl::Condition(&cond));
      if (shutdown_) {
        break;
      }

      if (counters_.empty() && durations_.empty()) continue;

      std::string line;
      for (const auto& [name, value] : counters_) {
        line += absl::StrFormat("%s:%u ", name, value);
      }
      for (const auto& [name, value] : durations_) {
        line +=
            absl::StrFormat("%s:%0.3fs ", name, absl::ToDoubleSeconds(value));
      }
      counters_.clear();
      durations_.clear();

      LOG_WARNING("\n%s\n", line);
    }
  }

  absl::Mutex mutex_;
  std::map<std::string, size_t> counters_ ABSL_GUARDED_BY(mutex_);
  std::map<std::string, absl::Duration> durations_ ABSL_GUARDED_BY(mutex_);

  std::thread print_thread_;
  bool shutdown_ ABSL_GUARDED_BY(mutex_) = false;
};

class NullStatsCollector : public StatsCollector {
 public:
  class NullDurationScope : public DurationScope {};

  NullStatsCollector() = default;
  ~NullStatsCollector() override = default;

  void IncCounter(const char*, size_t) override {}

  std::unique_ptr<DurationScope> RecordDuration(const char*) override {
    return std::make_unique<NullDurationScope>();
  }
};

StatsCollector::StatsCollector() = default;

StatsCollector::~StatsCollector() = default;

StatsCollector* StatsCollector::instance_ = nullptr;

StatsCollector::DurationScope::~DurationScope() = default;

// static
void StatsCollector::Initialize() {
  assert(!instance_);
  instance_ = new StatsCollectorImpl;
}

// static
void StatsCollector::Shutdown() {
  assert(instance_);
  delete instance_;
  instance_ = nullptr;
}

// static
StatsCollector* StatsCollector::Instance() {
  static NullStatsCollector null_instance;
  return instance_ ? instance_ : &null_instance;
}

}  // namespace cdc_ft
