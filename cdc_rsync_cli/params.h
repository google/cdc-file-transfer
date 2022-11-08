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

#ifndef CDC_RSYNC_CLI_PARAMS_H_
#define CDC_RSYNC_CLI_PARAMS_H_

#include <string>
#include <vector>

#include "cdc_rsync/cdc_rsync.h"

namespace cdc_ft {
namespace params {

// All cdc_rsync command line parameters.
struct Parameters {
  Options options;
  std::vector<std::string> sources;
  std::string user_host;
  std::string destination;
  std::string files_from;
};

// Parses sources, destination and options from the command line args.
// Prints a help text if not enough arguments were given or -h/--help was given.
bool Parse(int argc, const char* const* argv, Parameters* parameters);

}  // namespace params
}  // namespace cdc_ft

#endif  // CDC_RSYNC_CLI_PARAMS_H_
