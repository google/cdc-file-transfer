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

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <string>
#include <vector>

#include "cdc_rsync/cdc_rsync.h"
#include "cdc_rsync/params.h"
#include "common/util.h"

int wmain(int argc, wchar_t* argv[]) {
  cdc_ft::params::Parameters parameters;

  // Convert args from wide to UTF8 strings.
  std::vector<std::string> utf8_str_args;
  utf8_str_args.reserve(argc);
  for (int i = 0; i < argc; i++) {
    utf8_str_args.push_back(cdc_ft::Util::WideToUtf8Str(argv[i]));
  }

  // Convert args from UTF8 strings to UTF8 c-strings.
  std::vector<const char*> utf8_args;
  utf8_args.reserve(argc);
  for (const auto& utf8_str_arg : utf8_str_args) {
    utf8_args.push_back(utf8_str_arg.c_str());
  }

  if (!cdc_ft::params::Parse(argc, utf8_args.data(), &parameters)) {
    return 1;
  }

  std::string error_message;
  cdc_ft::ReturnCode code =
      cdc_ft::Sync(parameters.options, parameters.sources, parameters.user_host,
                   parameters.destination, &error_message);

  if (!error_message.empty()) {
    fprintf(stderr, "Error: %s\n", error_message.c_str());
  }
  return static_cast<int>(code);
}
