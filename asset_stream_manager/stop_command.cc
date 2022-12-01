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

#include "asset_stream_manager/stop_command.h"

#include <memory>

#include "asset_stream_manager/local_assets_stream_manager_client.h"
#include "asset_stream_manager/session_management_server.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_macros.h"
#include "lyra/lyra.hpp"

namespace cdc_ft {
namespace {
constexpr int kDefaultVerbosity = 2;
}  // namespace

StopCommand::StopCommand(int* exit_code)
    : BaseCommand("stop", "Stops a streaming session", exit_code) {}

StopCommand::~StopCommand() = default;

void StopCommand::RegisterCommandLineFlags(lyra::command& cmd) {
  verbosity_ = kDefaultVerbosity;
  cmd.add_argument(lyra::opt(verbosity_, "num")
                       .name("--verbosity")
                       .help("Verbosity of the log output, default: " +
                             std::to_string(kDefaultVerbosity) +
                             ". Increase to make logs more verbose."));

  service_port_ = SessionManagementServer::kDefaultServicePort;
  cmd.add_argument(
      lyra::opt(service_port_, "port")
          .name("--service-port")
          .help("Local port to use while connecting to the local "
                "asset stream service, default: " +
                std::to_string(SessionManagementServer::kDefaultServicePort)));

  cmd.add_argument(
      lyra::arg(PosArgValidator(&user_host_dir_), "[user@]host:src-dir")
          .required()
          .help("Linux host and directory to stream to"));
}

absl::Status StopCommand::Run() {
  LogLevel level = Log::VerbosityToLogLevel(verbosity_);
  ScopedLog scoped_log(std::make_unique<ConsoleLog>(level));
  LocalAssetsStreamManagerClient client(service_port_);

  std::string user_host, mount_dir;
  RETURN_IF_ERROR(LocalAssetsStreamManagerClient::ParseUserHostDir(
      user_host_dir_, &user_host, &mount_dir));

  absl::Status status = client.StopSession(user_host, mount_dir);
  if (status.ok()) {
    LOG_INFO("Stopped streaming session to '%s:%s'", user_host, mount_dir);
  }

  return status;
}

}  // namespace cdc_ft
