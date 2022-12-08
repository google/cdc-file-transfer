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

#ifndef CDC_STREAM_ASSET_STREAM_CONFIG_H_
#define CDC_STREAM_SET_STREAM_CONFIG_H_

#include <map>
#include <set>
#include <string>

#include "absl/status/status.h"
#include "cdc_stream/session_config.h"
#include "session.h"

namespace lyra {
class command;
}

namespace cdc_ft {

class BaseCommand;

// Class containing all configuration settings for asset streaming.
// Reads flags from the command line and optionally applies overrides from
// a json file.
class AssetStreamConfig {
 public:
  // Constructs the configuration by applying command line flags.
  AssetStreamConfig();
  ~AssetStreamConfig();

  // Registers arguments with Lyra.
  void RegisterCommandLineFlags(lyra::command& cmd, BaseCommand& base_command);

  // Loads a configuration from the JSON file at |path| and overrides any config
  // values that are set in this file. Sample json file:
  // {
  //   "service-port":44432
  //   "forward-port-first":"44433"
  //   "forward-port-last":"44442"
  //   "verbosity":3,
  //   "debug":0,
  //   "singlethreaded":0,
  //   "stats":0,
  //   "quiet":0,
  //   "check":0,
  //   "log-to-stdout":0,
  //   "cache-capacity":"150G",
  //   "cleanup-timeout":300,
  //   "access-idle-timeout":5,
  //   "manifest-updater-threads":4,
  //   "file-change-wait-duration-ms":500
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

  // Gets the port to use for the asset streaming service.
  uint16_t service_port() const { return service_port_; }

  // Session configuration.
  const SessionConfig& session_cfg() const { return session_cfg_; }

  // Workstation directory to be streamed. Used for development purposes only
  // to start a session right away when the service starts up. See dev CLI args.
  const std::string& dev_src_dir() const { return dev_src_dir_; }

  // Session target. Used for development purposes only to start a session right
  // away when the service starts up. See dev CLI args.
  const SessionTarget& dev_target() const { return dev_target_; }

  // Whether to log to a file or to stdout.
  bool log_to_stdout() const { return log_to_stdout_; }

 private:
  // Jedec parser for Lyra options. Usage:
  //   lyra::opt(JedecParser("size-flag", &size_bytes), "bytes"))
  // Sets jedec_parse_error_ on error, Lyra doesn't support errors from lambdas.
  std::function<void(const std::string&)> JedecParser(const char* flag_name,
                                                      uint64_t* bytes);

  uint16_t service_port_ = 0;
  SessionConfig session_cfg_;
  bool log_to_stdout_ = false;

  // Configuration used for development. Allows users to specify a session
  // via the service's command line.
  std::string dev_src_dir_;
  SessionTarget dev_target_;

  // Use a set, so the flags are sorted alphabetically.
  std::set<std::string> flags_read_from_file_;

  // Maps flags to errors occurred while reading this flag.
  std::map<std::string, std::string> flag_read_errors_;
};

};  // namespace cdc_ft

#endif  // CDC_STREAM_SET_STREAM_CONFIG_H_
