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

#ifndef ASSET_STREAM_MANAGER_START_SERVICE_COMMAND_H_
#define ASSET_STREAM_MANAGER_START_SERVICE_COMMAND_H_

#include "asset_stream_manager/asset_stream_config.h"
#include "asset_stream_manager/base_command.h"

namespace cdc_ft {

// Handler for the start-service command. Starts the asset streaming service
// and returns when the service is shut down.
class StartServiceCommand : public BaseCommand {
 public:
  explicit StartServiceCommand(int* exit_code);
  ~StartServiceCommand();

  // BaseCommand:
  void RegisterCommandLineFlags(lyra::command& cmd) override;
  absl::Status Run() override;

 private:
  // Initializes LOG* logging.
  // Depending on the flags, might log to console or to a file.
  absl::Status InitLogging();

  // Runs the asset streaming service.
  absl::Status RunService();

  AssetStreamConfig cfg_;
  std::string config_file_;
  std::string log_dir_;
};

}  // namespace cdc_ft

#endif  // ASSET_STREAM_MANAGER_START_SERVICE_COMMAND_H_
