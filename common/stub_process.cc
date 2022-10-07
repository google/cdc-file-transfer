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

#include "common/stub_process.h"

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "common/status_macros.h"
#include "common/util.h"

namespace cdc_ft {

class StubProcess : public Process {
 public:
  StubProcess(const ProcessStartInfo& start_info, std::string std_out,
              std::string std_err, uint32_t exit_code, bool has_exited,
              bool never_exits, StubProcessFactory::ProcessHandlerFn handler)
      : Process(start_info),
        std_out_(std::move(std_out)),
        std_err_(std::move(std_err)),
        exit_code_(exit_code),
        has_exited_(has_exited),
        never_exits_(never_exits),
        handler_(handler) {}
  ~StubProcess() = default;

  // Process:
  absl::Status Start() override { return absl::OkStatus(); }

  absl::Status RunUntil(std::function<bool()> exit_condition) override {
    if (handler_) {
      handler_(&std_out_, &std_err_, &exit_code_);
    }

    // Write stdout/stderr including the null terminator.
    if (start_info_.stdout_handler) {
      RETURN_IF_ERROR(
          start_info_.stdout_handler(std_out_.c_str(), std_out_.size() + 1));
    }
    if (start_info_.stderr_handler) {
      RETURN_IF_ERROR(
          start_info_.stderr_handler(std_out_.c_str(), std_err_.size() + 1));
    }
    if (never_exits_) {
      // Poll until exit condition (e.g. timeout) is satisfied.
      while (!exit_condition()) Util::Sleep(1);
    } else {
      // Exit immediately.
      has_exited_ = true;
    }
    return absl::OkStatus();
  }

  absl::Status Terminate() override { return absl::OkStatus(); }

  absl::Status WriteToStdIn(const void*, size_t) override {
    return absl::OkStatus();
  }

  void CloseStdIn() override {}

  bool HasExited() const override { return has_exited_; }

  uint32_t ExitCode() const override { return exit_code_; }

  absl::Status GetStatus() const override { return absl::OkStatus(); }

 private:
  std::string std_out_;
  std::string std_err_;
  uint32_t exit_code_;
  const bool never_exits_;
  bool has_exited_ = false;
  StubProcessFactory::ProcessHandlerFn handler_;
};

// An ErrorProcess is returned if the corresponding command isn't handled in
// StubProcessFactory::Create(). It will fail to start.
class ErrorProcess : public Process {
 public:
  explicit ErrorProcess(const ProcessStartInfo& start_info)
      : Process(start_info),
        status_(absl::InternalError(
            absl::StrFormat("Unhandled process '%s'", start_info_.command))) {}
  ~ErrorProcess() = default;

  // Process:
  absl::Status Start() override { return status_; }
  absl::Status RunUntil(std::function<bool()>) override { return status_; }
  absl::Status Terminate() override { return status_; }
  absl::Status WriteToStdIn(const void*, size_t) override { return status_; }
  void CloseStdIn() override {}
  bool HasExited() const override { return false; }
  uint32_t ExitCode() const override { return kExitCodeStillRunning; }
  absl::Status GetStatus() const override { return status_; }

 private:
  absl::Status status_;
};

StubProcessFactory::~StubProcessFactory() = default;

std::unique_ptr<Process> StubProcessFactory::Create(
    const ProcessStartInfo& start_info) {
  for (const ProcessInfo& pi : process_info_) {
    if (absl::StrContains(start_info.command, pi.command_part)) {
      return std::make_unique<StubProcess>(start_info, pi.std_out, pi.std_err,
                                           pi.exit_code, pi.has_exited,
                                           pi.never_exits, pi.handler);
    }
  }

  return std::make_unique<ErrorProcess>(start_info);
}

void StubProcessFactory::SetProcessOutput(std::string command_part,
                                          std::string std_out,
                                          std::string std_err,
                                          uint32_t exit_code) {
  process_info_.push_back({std::move(command_part), std::move(std_out),
                           std::move(std_err), exit_code,
                           /*has_exited=*/false, /*never_exits=*/false,
                           ProcessHandlerFn()});
}

void StubProcessFactory::SetProcessNeverExits(std::string command_part) {
  process_info_.push_back({std::move(command_part), "", "", 1,
                           /*has_exited=*/false,
                           /*never_exits=*/true, ProcessHandlerFn()});
}

void StubProcessFactory::SetProcessExitsImmediately(std::string command_part,
                                                    uint32_t exit_code) {
  process_info_.push_back({std::move(command_part), /*std_out=*/"",
                           /*std_err=*/"", exit_code, /*has_exited=*/true,
                           /*never_exits=*/false, ProcessHandlerFn()});
}

void StubProcessFactory::SetProcessHandler(std::string command_part,
                                           ProcessHandlerFn handler) {
  process_info_.push_back({std::move(command_part), /*std_out=*/"",
                           /*std_err=*/"",
                           /*exit_code_=*/0, /*has_exited=*/false,
                           /*never_exits=*/false, handler});
}

}  // namespace cdc_ft
