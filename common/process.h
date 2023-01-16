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

#ifndef COMMON_PROCESS_H_
#define COMMON_PROCESS_H_

#include <functional>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "common/log.h"

namespace cdc_ft {

// Helper function to forward process output to the logs.
// |name| is prefixed to the logs.
// |log_level| specifies the log level. If not given, the level is guessed from
// the log lines.
absl::Status LogOutput(const char* name, const char* data, size_t data_size,
                       absl::optional<LogLevel> log_level = {});

enum class ProcessFlags {
  kNone = 0,
  kDetached = 1 << 0,
  kNoWindow = 1 << 1,
};

struct ProcessStartInfo {
  // Handler for stdout/stderr. |data| is guaranteed to be NULL terminated, so
  // it may be used like a C-string if it's known to be text, e.g. for printf().
  // The NULL terminator does not count towards |data_size|.
  using OutputHandler =
      std::function<absl::Status(const char* data, size_t data_size)>;

  // Human readable name for debugging purposes, e.g. the message pump thread
  // name. Falls back to command.
  std::string name;

  // Command line, UTF-8 encoded.
  std::string command;

  // Full path to the process startup working directory.
  // If empty, uses parent process working dir.
  std::string startup_dir;

  // If set, the process stdin is redirected to a pipe.
  // It not set, the input is connected to the stdin of the calling process.
  bool redirect_stdin = false;

  // If set, forwards stderr and stdout to logging unless a log handler is set.
  bool forward_output_to_log = false;

  // If set, these handlers get called automatically whenever stdout/stderr data
  // is available. The calls happen on a WORKER THREAD, so the methods have to
  // make sure the code is thread-safe. The |data| sent to the output handler
  // is NULL terminated.
  // If not set, the output is forwarded to the stdout/stderr of the calling
  // process.
  OutputHandler stdout_handler;
  OutputHandler stderr_handler;

  // Flags that define additional properties of the process.
  ProcessFlags flags = ProcessFlags::kNone;

  // Returns |name| if set, otherwise |command|.
  const std::string& Name() const;

  // Tests ALL flags (flags & flag) == flag.
  bool HasFlag(ProcessFlags flag) const;
};

// Runs a background process and pipes stdin/stdout/stderr.
class Process {
 public:
  static constexpr uint32_t kExitCodeNotStarted = 4000000000;
  static constexpr uint32_t kExitCodeStillRunning = 4000000001;
  static constexpr uint32_t kExitCodeFailedToGetExitCode = 4000000002;

  explicit Process(const ProcessStartInfo& start_info);

  // Terminates the process unless it's running with ProcessFlags::kDetached.
  virtual ~Process();

  // Start the background process.
  virtual absl::Status Start() = 0;

  // Runs the process until it exits, an error occurs (see GetStatus()) or
  // |exit_condition| returns true.
  virtual absl::Status RunUntil(std::function<bool()> exit_condition) = 0;

  // Runs the process until it exits or an error occurs (see GetStatus()).
  absl::Status RunUntilExit();

  // Ends the process.
  virtual absl::Status Terminate() = 0;

  // Writes |data| of size |size| to the child process stdin. Only applicable
  // if |redirect_stdin| was set to true in the start info, no-op if not.
  virtual absl::Status WriteToStdIn(const void* data, size_t size) = 0;

  // Closes the stdin pipe if |redirect_stdin| was set to true.
  virtual void CloseStdIn() = 0;

  // Returns true if the process has exited.
  virtual bool HasExited() const = 0;

  // Returns the process exit code if the process exited and the code could be
  // retrieved successfully. Returns |kExitCodeNotStarted| if the process has
  // not been started. Returns |kExitCodeStillRunning| if the process has not
  // exited yet. Returns |kExitCodeFailedToGetExitCode| if the process exit code
  // could not be retrieved.
  virtual uint32_t ExitCode() const = 0;

  // Returns the internal status of the process. This is an internal status. If
  // the process fails with an error, this is not reported in this status, but
  // instead in ExitCode().
  virtual absl::Status GetStatus() const = 0;

 protected:
  ProcessStartInfo start_info_;
};

// Abstract process factory.
class ProcessFactory {
 public:
  virtual ~ProcessFactory();

  // Creates a new process with given |start_info|.
  virtual std::unique_ptr<Process> Create(
      const ProcessStartInfo& start_info) = 0;

  // Convenience method that starts a process with the given |start_info| and
  // runs it until exit. Returns an error if starting or running the process
  // fails, or if the exit code is not 0.
  absl::Status Run(const ProcessStartInfo& start_info);
};

// Creates Windows processes.
class WinProcessFactory : public ProcessFactory {
 public:
  ~WinProcessFactory() override;

  // ProcessFactory:
  std::unique_ptr<Process> Create(const ProcessStartInfo& start_info) override;
};

inline ProcessFlags operator|(ProcessFlags a, ProcessFlags b) {
  using T = std::underlying_type_t<ProcessFlags>;
  return static_cast<ProcessFlags>(static_cast<T>(a) | static_cast<T>(b));
}

inline ProcessFlags operator&(ProcessFlags a, ProcessFlags b) {
  using T = std::underlying_type_t<ProcessFlags>;
  return static_cast<ProcessFlags>(static_cast<T>(a) & static_cast<T>(b));
}

}  // namespace cdc_ft

#endif  // COMMON_PROCESS_H_
