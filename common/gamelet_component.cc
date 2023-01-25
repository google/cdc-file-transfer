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

#include "common/gamelet_component.h"

#include <cinttypes>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "common/build_version.h"
#include "common/path.h"
#include "common/status.h"

namespace cdc_ft {

GameletComponent::GameletComponent(std::string build_version,
                                   std::string filename, uint64_t size,
                                   time_t modified_time)
    : build_version(build_version),
      filename(filename),
      size(size),
      modified_time(modified_time) {}

GameletComponent::~GameletComponent() = default;

bool GameletComponent::operator==(const GameletComponent& other) const {
  if (filename != other.filename) {
    return false;
  }

  // If either build version is the dev version, it means that the component was
  // built locally, so that we can't compare build versions. Fall back to
  // comparing file_size and  modified_time.
  if (build_version != DEV_BUILD_VERSION &&
      build_version != DEV_BUILD_VERSION) {
    return build_version == other.build_version;
  }

  return size == other.size && modified_time == other.modified_time;
}

bool GameletComponent::operator!=(const GameletComponent& other) const {
  return !(*this == other);
}

// static
absl::Status GameletComponent::Get(
    const std::vector<std::string> component_paths,
    std::vector<GameletComponent>* components) {
  components->clear();

  for (const std::string& path : component_paths) {
    path::Stats stats;
    absl::Status status = path::GetStats(path, &stats);
    if (!status.ok())
      return WrapStatus(status, "GetStats() failed for '%s'", path);
    components->emplace_back(BUILD_VERSION, path::BaseName(path), stats.size,
                             stats.modified_time);
  }

  return absl::OkStatus();
}

// static
std::string GameletComponent::ToCommandLineArgs(
    const std::vector<GameletComponent>& components) {
  std::string args;
  for (const GameletComponent& comp : components) {
    args += absl::StrFormat("%s%s %s %u %d", args.empty() ? "" : " ",
                            comp.build_version.c_str(), comp.filename.c_str(),
                            comp.size, comp.modified_time);
  }
  return args;
}

// static
std::vector<GameletComponent> GameletComponent::FromCommandLineArgs(
    int argc, const char** argv) {
  std::vector<GameletComponent> components;
  for (int n = 0; n + 3 < argc; n += 4) {
    components.emplace_back(argv[n], argv[n + 1], std::stol(argv[n + 2]),
                            std::stol(argv[n + 3]));
  }
  return components;
}

// static
std::vector<GameletComponent> GameletComponent::FromCommandLineArgs(
    const std::string& components_arg) {
  std::vector<std::string> args_vec = absl::StrSplit(components_arg, ' ');
  int argc = static_cast<int>(args_vec.size());
  std::vector<const char*> argv;
  for (const std::string& arg : args_vec) argv.push_back(arg.c_str());
  return FromCommandLineArgs(argc, argv.data());
}

}  // namespace cdc_ft
