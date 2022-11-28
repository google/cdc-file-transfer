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

#ifndef COMMON_LOG_H_
#define COMMON_LOG_H_

#include <memory>

#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "common/clock.h"

namespace cdc_ft {

enum class LogLevel { kVerbose, kDebug, kInfo, kWarning, kError };

// Note: Bazel always uses forward slashes for paths.
#define __FILENAME__ \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define LOG_VERBOSE(...)                                                \
  ::cdc_ft::Log::SafePrintf(::cdc_ft::LogLevel::kVerbose, __FILENAME__, \
                            __LINE__, __func__, __VA_ARGS__);
#define LOG_DEBUG(...)                                                \
  ::cdc_ft::Log::SafePrintf(::cdc_ft::LogLevel::kDebug, __FILENAME__, \
                            __LINE__, __func__, __VA_ARGS__);
#define LOG_INFO(...)                                                          \
  ::cdc_ft::Log::SafePrintf(::cdc_ft::LogLevel::kInfo, __FILENAME__, __LINE__, \
                            __func__, __VA_ARGS__);
#define LOG_WARNING(...)                                                \
  ::cdc_ft::Log::SafePrintf(::cdc_ft::LogLevel::kWarning, __FILENAME__, \
                            __LINE__, __func__, __VA_ARGS__);
#define LOG_ERROR(...)                                                \
  ::cdc_ft::Log::SafePrintf(::cdc_ft::LogLevel::kError, __FILENAME__, \
                            __LINE__, __func__, __VA_ARGS__);
#define LOG_LEVEL(level, ...)                                        \
  ::cdc_ft::Log::SafePrintf(level, __FILENAME__, __LINE__, __func__, \
                            __VA_ARGS__);

class Log {
 public:
  // Initializes the Log singleton. Should be called in the beginning.
  static void Initialize(std::unique_ptr<Log> log);

  // Deletes the Log singleton. Must be called in the end.
  static void Shutdown();

  // Returns the global log instance.
  static Log* Instance();

  template <typename... Args>
  static void SafePrintf(LogLevel level, const char* file, int line,
                         const char* func,
                         const absl::FormatSpec<Args...>& format,
                         Args... args) {
    Log* inst = MaybeNullInstance();
    if (inst) {
      inst->Printf(level, file, line, func, format, args...);
    } else {
      DefaultWriteLogMessage(level, file, line,
                             absl::StrFormat(format, args...).c_str());
    }
  }

  template <typename... Args>
  void Printf(LogLevel level, const char* file, int line, const char* func,
              const absl::FormatSpec<Args...>& format, Args... args) {
    if (log_level_ <= level) {
      WriteLogMessage(level, file, line, func,
                      absl::StrFormat(format, args...).c_str());
    }
  }

  static LogLevel VerbosityToLogLevel(int verbosity);

  void SetLogLevel(LogLevel log_level) { log_level_ = log_level; }
  LogLevel GetLogLevel() const { return log_level_; }

  virtual ~Log();

 protected:
  explicit Log(LogLevel log_level) : log_level_(log_level) {}

  virtual void WriteLogMessage(LogLevel level, const char* file, int line,
                               const char* func, const char* message) = 0;

 private:
  // A default log writer implementation in case the log was not initialized.
  static void DefaultWriteLogMessage(LogLevel level, const char* file, int line,
                                     const char* message);

  // Returns the global log instance.
  static Log* MaybeNullInstance();

  static Log* instance_;

  LogLevel log_level_;
};

class ConsoleLog : public Log {
 public:
  explicit ConsoleLog(LogLevel log_level) : Log(log_level) {}

 protected:
  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override
      ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  absl::Mutex mutex_;
};

// Initializes the log in the constructor and shuts it down on destruction.
class ScopedLog {
 public:
  ScopedLog(std::unique_ptr<Log> log) { Log::Initialize(std::move(log)); }
  ~ScopedLog() { Log::Shutdown(); }
};

class FileLog : public Log {
 public:
  FileLog(LogLevel log_level, const char* path);
  ~FileLog();

 protected:
  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override
      ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  FILE* file_;
  DefaultSystemClock clock_;
  absl::Mutex mutex_;
};

}  // namespace cdc_ft

#endif  // COMMON_LOG_H_
