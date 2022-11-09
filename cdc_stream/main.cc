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

#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/status/status.h"
#include "common/log.h"

ABSL_FLAG(std::string, src_dir, "",
          "Start a streaming session immediately from the given Windows path. "
          "Must also specify --user_host and --port.");
ABSL_FLAG(std::string, user_host, "",
          "SSH username and hostname of the remote instance to stream to."
          "This flag is ignored unless --src_dir is set as well.");
ABSL_FLAG(uint16_t, port, 22,
          "SSH port to use while connecting to the remote instance. "
          "This flag is ignored unless --src_dir is set as well.");
ABSL_FLAG(int, verbosity, 2, "Verbosity of the log output");

int main(int argc, char* argv[]) {
  absl::SetProgramUsageMessage(
      R"!(Stream files from a Windows to a Linux device

Usage: cdc_stream [flags] start windows_dir [user@]host:linux_dir
       Streams the Windows directory windows_dir to directory linux_dir on the
       Linux host using SSH username user.
 
       cdc_stream [flags] stop host
       Stops the streaming session to the given Linux host.
)!");
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);

  cdc_ft::LogLevel level =
      cdc_ft::Log::VerbosityToLogLevel(absl::GetFlag(FLAGS_verbosity));
  cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(level));

  absl::Status status;

  cdc_ft::Log::Shutdown();
  return static_cast<int>(status.code());
}
