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

#ifndef ASSET_STREAM_MANAGER_ASSET_STREAM_CONFIG_H_
#define ASSET_STREAM_MANAGER_ASSET_STREAM_CONFIG_H_

#include <map>
#include <set>
#include <string>

#include "absl/status/status.h"
#include "asset_stream_manager/session_config.h"

namespace cdc_ft {

// Class containing all configuration settings for asset streaming.
// Reads flags from the command line and optionally applies overrides from
// a json file.
class AssetStreamConfig {
 public:
  // Constructs the configuration by applying command line flags.
  AssetStreamConfig();
  ~AssetStreamConfig();

  // Loads a configuration from the JSON file at |path| and overrides any config
  // values that are set in this file. Sample json file:
  // {
  //   "src_dir":"C:\\path\\to\\assets",
  //   "verbosity":3,
  //   "debug":0,
  //   "singlethreaded":0,
  //   "stats":0,
  //   "quiet":0,
  //   "check":0,
  //   "log_to_stdout":0,
  //   "cache_capacity":"150G",
  //   "cleanup_timeout":300,
  //   "access_idle_timeout":5,
  //   "manifest_updater_threads":4,
  //   "file_change_wait_duration_ms":500
  // }
  // Returns NotFoundError if the file does not exist.
  // Returns InvalidArgumentError if the file is not valid JSON.
  absl::Status LoadFromFile(const std::string& path);

  // Returns a string with all config values, suitable for logging.
  std::string ToString();

  // Gets a comma-separated list of flags that were read from the JSON file.
  // These flags override command line flags.
  std::string GetFlagsReadFromFile();

  // Gets a newline-separated list of errors for each flag that could not be
  // read from the JSON file.
  std::string GetFlagReadErrors();

  // Workstation directory to stream. Should usually be empty since mounts are
  // triggered by the CLI or the partner portal via a gRPC call, but useful
  // during development.
  const std::string& src_dir() const { return src_dir_; }

  // IP address of the instance to stream to. Should usually be empty since
  // mounts are triggered by the CLI or the partner portal via a gRPC call, but
  // useful during development.
  const std::string& instance_ip() const { return instance_ip_; }

  // IP address of the instance to stream to. Should usually be unset (0) since
  // mounts are triggered by the CLI or the partner portal via a gRPC call, but
  // useful during development.
  const uint16_t instance_port() const { return instance_port_; }

  // Session configuration.
  const SessionConfig session_cfg() const { return session_cfg_; }

  // Whether to log to a file or to stdout.
  bool log_to_stdout() const { return log_to_stdout_; }

 private:
  std::string src_dir_;
  std::string instance_ip_;
  uint16_t instance_port_ = 0;
  SessionConfig session_cfg_;
  bool log_to_stdout_ = false;

  // Use a set, so the flags are sorted alphabetically.
  std::set<std::string> flags_read_from_file_;

  // Maps flags to errors occurred while reading this flag.
  std::map<std::string, std::string> flag_read_errors_;
};

};  // namespace cdc_ft

#endif  // ASSET_STREAM_MANAGER_ASSET_STREAM_CONFIG_H_
