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

#ifndef COMMON_STUB_PROCESS_H_
#define COMMON_STUB_PROCESS_H_

#include <vector>

#include "absl/status/status.h"
#include "common/process.h"

namespace cdc_ft {

// A stub process returns prefabricated stdout, stderr and exit codes to
// predetermined commands.
class StubProcessFactory : public ProcessFactory {
 public:
  using ProcessHandlerFn = std::function<void(
      std::string* std_out, std::string* std_err, uint32_t* exit_code)>;

  virtual StubProcessFactory::~StubProcessFactory();

  // ProcessFactory:
  std::unique_ptr<Process> Create(const ProcessStartInfo& start_info) override;

  // Sets up a stub process that is returned by a call to Create() if the
  // command contains |command_part|. The stub process exits on RunUntil(),
  // writes the given |std_out| and |std_err| to the output stream and exits
  // with |exit_code|.
  void SetProcessOutput(std::string command_part, std::string std_out,
                        std::string std_err, uint32_t exit_code);

  // Sets up a stub process that is returned by a call to Create() if the
  // command contains |command_part|. The process never exits. Useful for
  // testing timeout conditions.
  void SetProcessNeverExits(std::string command_part);

  // Sets up a stub process that is returned by a call to Create() if the
  // command contains |command_part|. The stub process is immediately in an
  // exited state with code |exit_code| and does not write stdout or stderr.
  void SetProcessExitsImmediately(std::string command_part, uint32_t exit_code);

  // Sets up a stub process that is returned by a call to Create() if the
  // command contains |command_part|. The stub process calls |handler| on
  // RunUntil(), writes returned |std_out| and |std_err| to the output stream
  // and exits with |exit_code|.
  void SetProcessHandler(std::string command_part, ProcessHandlerFn handler);

 private:
  struct ProcessInfo {
    std::string command_part;
    std::string std_out;
    std::string std_err;
    uint32_t exit_code;
    bool has_exited;
    bool never_exits;
    ProcessHandlerFn handler;
  };

  std::vector<ProcessInfo> process_info_;
};

}  // namespace cdc_ft

#endif  // COMMON_STUB_PROCESS_H_
