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

#include "asset_stream_manager/base_command.h"

#include "lyra/lyra.hpp"

namespace cdc_ft {

BaseCommand::BaseCommand(std::string name, std::string help, int* exit_code)
    : name_(name), help_(help), exit_code_(exit_code) {
  assert(exit_code_);
}

BaseCommand::~BaseCommand() = default;

void BaseCommand::Register(lyra::cli& cli) {
  lyra::command cmd(name_,
                    [this](const lyra::group& g) { this->CommandHandler(g); });
  cmd.help(help_);
  cmd.add_argument(lyra::help(show_help_));

  RegisterCommandLineFlags(cmd);

  // Workaround for Lyra treating --unknown_flags as positional argument.
  // If this argument is not empty, it's an unknown arg.
  cmd.add_argument(lyra::arg(invalid_arg_, ""));

  // Register command with CLI.
  cli.add_argument(std::move(cmd));
}

void BaseCommand::CommandHandler(const lyra::group& g) {
  // Handle -h, --help.
  if (show_help_) {
    std::cout << g;
    *exit_code_ = 0;
    return;
  }

  // Handle invalid arguments.
  if (!invalid_arg_.empty()) {
    std::cerr << "Error: Unknown parameter '" << invalid_arg_
              << "'. Try -h for help." << std::endl;
    *exit_code_ = 1;
    return;
  }

  // Run and print error.
  absl::Status status = Run();
  if (!status.ok()) {
    std::cerr << "Error: " << status.message() << std::endl;
  }

  // Write status code to |exit_code_|.
  static_assert(static_cast<int>(absl::StatusCode::kOk) == 0, "kOk not 0");
  *exit_code_ = static_cast<int>(status.code());
}

}  // namespace cdc_ft
