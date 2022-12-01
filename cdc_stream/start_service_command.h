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

#ifndef CDC_STREAM_START_SERVICE_COMMAND_H_
#define CDC_STREAM_START_SERVICE_COMMAND_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cdc_stream/asset_stream_config.h"
#include "cdc_stream/base_command.h"

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
  // Depending on the flags, returns a console or file logger.
  absl::StatusOr<std::unique_ptr<Log>> GetLogger();

  // Runs the asset streaming service.
  absl::Status RunService();

  AssetStreamConfig cfg_;
  std::string config_file_;
  std::string log_dir_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_START_SERVICE_COMMAND_H_
