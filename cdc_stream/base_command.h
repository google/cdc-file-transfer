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

#ifndef CDC_STREAM_BASE_COMMAND_H_
#define CDC_STREAM_BASE_COMMAND_H_

#include <string>

#include "absl/status/status.h"

namespace lyra {
class cli;
class command;
class group;
}  // namespace lyra

namespace cdc_ft {

// Base class for commands that wraps Lyra commands to reduce common
// boilerplate like help text display, invalid args and return values from
// command execution.
class BaseCommand {
 public:
  // Creates a new command with given |name| and |help| text. After the command
  // ran, the status code as returned by Run() is written to |exit_code|.
  BaseCommand(std::string name, std::string help, int* exit_code);
  ~BaseCommand();

  // Registers the command with Lyra. Must be called before parsing args.
  void Register(lyra::cli& cli);

  // Jedec parser for Lyra options. Usage:
  //   lyra::opt(JedecParser("size-flag", &size_bytes), "bytes"))
  // Automatically reports a parse failure on error.
  std::function<void(const std::string&)> JedecParser(const char* flag_name,
                                                      uint64_t* bytes);

  // Parser for single ports "123" or port ranges "123-234". Usage:
  //   lyra::opt(PortRangeParser("port-flag", &first, &last), "port"))
  // Automatically reports a parse failure on error.
  std::function<void(const std::string&)> PortRangeParser(const char* flag_name,
                                                          uint16_t* first,
                                                          uint16_t* last);

  // Validator that should be used for all positional arguments. Lyra interprets
  // -u, --unknown_flag as positional argument. This validator makes sure that
  // a positional argument starting with - is reported as an error. Otherwise,
  // writes the value to |str|.
  std::function<void(const std::string&)> PosArgValidator(std::string* str);

 protected:
  // Adds all optional and required arguments used by the command.
  // Called by Register().
  virtual void RegisterCommandLineFlags(lyra::command& cmd) = 0;

  // Runs the command. Called by lyra::cli::parse() when this command is
  // actually triggered and all flags have been parsed successfully.
  virtual absl::Status Run() = 0;

 private:
  // Called by lyra::cli::parse() after successfully parsing arguments. Catches
  // unknown arguments (Lyra interprets those as positional args, not as an
  // error!), and displays the help text if appropriate, otherwise calls Run().
  void CommandHandler(const lyra::group& g);

  std::string name_;
  std::string help_;
  int* exit_code_ = nullptr;

  bool show_help_ = false;

  // Workaround for invalid args. Lyra doesn't interpret --invalid as invalid
  // argument, but as positional argument "--invalid".
  std::string invalid_arg_;

  // Extraneous positional args. Gets reported as error if present.
  std::string extra_positional_arg_;

  // Errors from custom flag parsers, e.g. JEDEC sizes or port ranges.
  // Works around Lyra not accepting errors from parsers.
  std::string parse_error_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_BASE_COMMAND_H_
