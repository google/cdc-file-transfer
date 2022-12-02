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

#include "cdc_stream/stop_service_command.h"

#include "absl/strings/str_format.h"
#include "cdc_stream/background_service_client.h"
#include "cdc_stream/session_management_server.h"
#include "common/log.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/support/channel_arguments.h"
#include "lyra/lyra.hpp"

namespace cdc_ft {

StopServiceCommand::StopServiceCommand(int* exit_code)
    : BaseCommand("stop-service", "Stops the streaming service", exit_code) {}
StopServiceCommand::~StopServiceCommand() = default;

void StopServiceCommand::RegisterCommandLineFlags(lyra::command& cmd) {
  verbosity_ = 2;
  cmd.add_argument(lyra::opt(verbosity_, "num")
                       .name("--verbosity")
                       .help("Verbosity of the log output, default: " +
                             std::to_string(verbosity_) +
                             ".Increase to make logs more verbose."));

  service_port_ = SessionManagementServer::kDefaultServicePort;
  cmd.add_argument(lyra::opt(service_port_, "port")
                       .name("--service-port")
                       .help("Local port to use while connecting to the local "
                             "asset stream service, default: " +
                             std::to_string(service_port_)));
}

absl::Status StopServiceCommand::Run() {
  LogLevel level = Log::VerbosityToLogLevel(verbosity_);
  ScopedLog scoped_log(std::make_unique<ConsoleLog>(level));

  std::string client_address = absl::StrFormat("localhost:%u", service_port_);
  std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
      client_address, grpc::InsecureChannelCredentials(),
      grpc::ChannelArguments());

  BackgroundServiceClient bg_client(channel);
  absl::Status status = bg_client.Exit();
  if (status.ok()) {
    LOG_INFO("Stopped streaming service");
  } else if (absl::IsUnavailable(status)) {
    // Server wasn't running. This doesn't count as an error.
    LOG_INFO("Streaming service already stopped");
    return absl::OkStatus();
  }

  return status;
}

}  // namespace cdc_ft
