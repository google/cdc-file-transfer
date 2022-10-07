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
#include "cdc_rsync_cli/params.h"
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

  // Convert sources from string-vec to c-str-vec.
  std::vector<const char*> sources_ptr;
  sources_ptr.reserve(parameters.sources.size());
  for (const std::string& source : parameters.sources) {
    sources_ptr.push_back(source.c_str());
  }

  // Convert filter rules from string-structs to c-str-structs.
  std::vector<cdc_ft::FilterRule> filter_rules;
  filter_rules.reserve(parameters.filter_rules.size());
  for (const cdc_ft::params::Parameters::FilterRule& rule :
       parameters.filter_rules) {
    filter_rules.emplace_back(rule.type, rule.pattern.c_str());
  }

  const char* error_message = nullptr;
  cdc_ft::ReturnCode code = cdc_ft::Sync(
      &parameters.options, filter_rules.data(), parameters.filter_rules.size(),
      parameters.sources_dir.c_str(), sources_ptr.data(),
      parameters.sources.size(), parameters.destination.c_str(),
      &error_message);

  if (error_message) {
    fprintf(stderr, "Error: %s\n", error_message);
  }
  return static_cast<int>(code);
}
