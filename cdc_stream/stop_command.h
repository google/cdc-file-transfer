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

#ifndef CDC_STREAM_STOP_COMMAND_H_
#define CDC_STREAM_STOP_COMMAND_H_

#include "absl/status/status.h"
#include "cdc_stream/base_command.h"

namespace cdc_ft {

// Handler for the stop command. Sends an RPC call to the service to stop an
// asset streaming session.
class StopCommand : public BaseCommand {
 public:
  explicit StopCommand(int* exit_code);
  ~StopCommand();

  // BaseCommand:
  void RegisterCommandLineFlags(lyra::command& cmd) override;
  absl::Status Run() override;

 private:
  int verbosity_ = 0;
  uint16_t service_port_ = 0;
  std::string user_host_dir_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_STOP_COMMAND_H_
