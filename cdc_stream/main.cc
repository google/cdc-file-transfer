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

#include "cdc_stream/start_command.h"
#include "cdc_stream/start_service_command.h"
#include "cdc_stream/stop_command.h"
#include "lyra/lyra.hpp"

int main(int argc, char* argv[]) {
  // Set up commands.
  auto cli = lyra::cli();
  bool show_help = false;
  int exit_code = -1;
  cli.add_argument(lyra::help(show_help));

  cdc_ft::StartCommand start_cmd(&exit_code);
  start_cmd.Register(cli);

  cdc_ft::StopCommand stop_cmd(&exit_code);
  stop_cmd.Register(cli);

  cdc_ft::StartServiceCommand start_service_cmd(&exit_code);
  start_service_cmd.Register(cli);

  // Parse args and run. Note that parse actually runs the commands.
  // exit_code is -1 if no command was run.
  auto result = cli.parse({argc, argv});
  if (show_help || exit_code == -1) {
    std::cout << cli;
    return 0;
  }
  if (!result) {
    // Parse error.
    std::cerr << "Error: " << result.message() << std::endl;
    return 1;
  }
  // If cli.parse() succeeds, it also runs the commands and writes |exit_code|.

  return exit_code;
}
