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

#ifndef COMMON_GAMELET_COMPONENT_H_
#define COMMON_GAMELET_COMPONENT_H_

#include <string>
#include <vector>

#include "absl/status/status.h"

namespace cdc_ft {

// Provides functionality to check the freshness of gamelet components.
// The components are considered fresh if both the timestamp and the file size
// match.
struct GameletComponent {
  std::string build_version;
  std::string filename;
  uint64_t size;
  int64_t modified_time;

  GameletComponent(std::string build_version, std::string filename,
                   uint64_t size, time_t modified_time);
  ~GameletComponent();

  bool operator==(const GameletComponent& other) const;
  bool operator!=(const GameletComponent& other) const;

  // Gets the list of gamelet |components| from the given |component_paths|.
  // Returns false if any of the components does not exist or cannot be stat'ed.
  static absl::Status Get(const std::vector<std::string> component_paths,
                          std::vector<GameletComponent>* components);

  // Serializes the list of gamelet |components| as command line options.
  // This should be called on the workstation. The resulting command line
  // should be passed to the gamelet process.
  static std::string ToCommandLineArgs(
      const std::vector<GameletComponent>& components);

  // Deserializes the list of gamelet components from command line options.
  // This should be called in the gamelet process to retrieve the workstation
  // components, which can then be compared to the gamelet components to check
  // freshness.
  static std::vector<GameletComponent> FromCommandLineArgs(int argc,
                                                           const char** argv);

  // Deserializes the list of gamelet components from a |components_arg| string
  // as it is returned by ToCommandLineArgs().
  // This should be called in the gamelet process to retrieve the workstation
  // components, which can then be compared to the gamelet components to check
  // freshness.
  static std::vector<GameletComponent> FromCommandLineArgs(
      const std::string& components_arg);
};

}  // namespace cdc_ft

#endif  // COMMON_GAMELET_COMPONENT_H_
