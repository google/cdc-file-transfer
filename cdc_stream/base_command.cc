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

#include "cdc_stream/base_command.h"

#include "absl/strings/str_format.h"
#include "absl_helper/jedec_size_flag.h"
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

  // Detect extra positional args.
  cmd.add_argument(lyra::arg(PosArgValidator(&extra_positional_arg_), ""));

  // Register command with CLI.
  cli.add_argument(std::move(cmd));
}

std::function<void(const std::string&)> BaseCommand::JedecParser(
    const char* flag_name, uint64_t* bytes) {
  return [flag_name, bytes,
          error = &jedec_parse_error_](const std::string& value) {
    JedecSize size;
    if (AbslParseFlag(value, &size, error)) {
      *bytes = size.Size();
    } else {
      *error = absl::StrFormat("Failed to parse %s=%s: %s", flag_name, value,
                               *error);
    }
  };
}

std::function<void(const std::string&)> BaseCommand::PosArgValidator(
    std::string* str) {
  return [str, invalid_arg = &invalid_arg_](const std::string& value) {
    if (!value.empty() && value[0] == '-') {
      *invalid_arg = value;
    } else {
      *str = value;
    }
  };
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

  if (!jedec_parse_error_.empty()) {
    std::cerr << "Error: " << jedec_parse_error_ << std::endl;
    *exit_code_ = 1;
    return;
  }

  if (!extra_positional_arg_.empty()) {
    std::cerr << "Error: Extraneous positional argument '"
              << extra_positional_arg_ << "'. Try -h for help." << std::endl;
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
