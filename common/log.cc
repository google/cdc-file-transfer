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

#include "log.h"

#include <cassert>

#include "common/platform.h"

#if PLATFORM_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

namespace cdc_ft {
namespace {

const char* GetLogLevelString(LogLevel level) {
  switch (level) {
    case LogLevel::kVerbose:
      return "VERBOSE";
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarning:
      return "WARNING";
    case LogLevel::kError:
      return "ERROR";
  }
  return "UnknownLogLevel";
}

}  // namespace

// static
void Log::Initialize(std::unique_ptr<Log> log) {
  assert(!instance_);
  instance_ = log.release();
}

// static
void Log::Shutdown() {
  assert(instance_);
  delete instance_;
  instance_ = nullptr;
}

// static
Log* Log::Instance() {
  assert(instance_);
  return instance_;
}

// static
Log* Log::MaybeNullInstance() { return instance_; }

Log* Log::instance_ = nullptr;

Log::~Log() = default;

// static
LogLevel Log::VerbosityToLogLevel(int verbosity) {
  if (verbosity >= 4) {
    return LogLevel::kVerbose;
  }
  if (verbosity >= 3) {
    return LogLevel::kDebug;
  }
  if (verbosity >= 2) {
    return LogLevel::kInfo;
  }
  return LogLevel::kWarning;
}

// static
void Log::DefaultWriteLogMessage(LogLevel level, const char* file, int line,
                                 const char* message) {
  // Only print warnings and above.
  if (level < LogLevel::kWarning) return;
  fprintf(stderr, "%-7s %s(%i): %s\n", GetLogLevelString(level), file, line,
          message);
}

#if PLATFORM_WINDOWS
enum Colors {
  kLightGray = 7,
  kGray = 8,
  kBlue = 9,
  kCyan = 11,
  kRed = 12,
  kYellow = 14,
  kWhite = 15
};

WORD GetConsoleColor(LogLevel level) {
  switch (level) {
    case LogLevel::kVerbose:
      return kGray;
    case LogLevel::kDebug:
      return kCyan;
    case LogLevel::kInfo:
      return kWhite;
    case LogLevel::kWarning:
      return kYellow;
    case LogLevel::kError:
      return kRed;
  }
  return 15;
}
#endif

void ConsoleLog::WriteLogMessage(LogLevel level, const char* file, int line,
                                 const char* func, const char* message) {
  absl::MutexLock lock(&mutex_);

  // Show leaner log messages in non-verbose mode.
  bool show_time_file_func = GetLogLevel() <= LogLevel::kDebug;
  FILE* stdfile = level >= LogLevel::kError ? stderr : stdout;
#if PLATFORM_WINDOWS
  HANDLE hConsole = GetStdHandle(STD_OUTPUT_HANDLE);
  SetConsoleTextAttribute(hConsole, GetConsoleColor(level));
  if (show_time_file_func) {
    fprintf(stdfile, "%0.3f %s(%i): %s(): %s\n", stopwatch_.ElapsedSeconds(),
            file, line, func, message);
  } else {
    fprintf(stdfile, "%s\n", message);
  }
  SetConsoleTextAttribute(hConsole, kLightGray);
#else
  if (show_time_file_func) {
    fprintf(stdfile, "%-7s %0.3f %s(%i): %s(): %s\n", GetLogLevelString(level),
            stopwatch_.ElapsedSeconds(), file, line, func, message);
  } else {
    fprintf(stdfile, "%-7s %s\n", GetLogLevelString(level), message);
  }
#endif
}

FileLog::FileLog(LogLevel log_level, const char* path) : Log(log_level) {
  file_ = fopen(path, "wt");
  if (!file_) fprintf(stderr, "Failed to open log file '%s'", path);
}

FileLog::~FileLog() {
  if (file_) fclose(file_);
}

void FileLog::WriteLogMessage(LogLevel level, const char* file, int line,
                              const char* func, const char* message) {
  if (!file_) return;
  std::string timestamp = clock_.FormatNow("%Y-%m-%d %H:%M:%S.", true);

  absl::MutexLock lock(&mutex_);
  fprintf(file_, "%s %-7s %s(%i): %s(): %s\n", timestamp.c_str(),
          GetLogLevelString(level), file, line, func, message);
  fflush(file_);
}

}  // namespace cdc_ft
