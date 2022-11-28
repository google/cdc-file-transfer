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

#include "asset_stream_manager/start_command.h"

#include <memory>

#include "asset_stream_manager/local_assets_stream_manager_client.h"
#include "asset_stream_manager/session_management_server.h"
#include "common/log.h"
#include "common/path.h"
#include "common/remote_util.h"
#include "common/status_macros.h"
#include "lyra/lyra.hpp"

namespace cdc_ft {
namespace {
constexpr int kDefaultVerbosity = 2;
}  // namespace

StartCommand::StartCommand(int* exit_code)
    : BaseCommand("start",
                  "Start streaming files from a Windows to a Linux device",
                  exit_code) {}

StartCommand::~StartCommand() = default;

void StartCommand::RegisterCommandLineFlags(lyra::command& cmd) {
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

  ssh_port_ = RemoteUtil::kDefaultSshPort;
  cmd.add_argument(
      lyra::opt(ssh_port_, "port")
          .name("--ssh-port")
          .help("Port to use while connecting to the remote instance being "
                "streamed to, default: " +
                std::to_string(RemoteUtil::kDefaultSshPort)));

  path::GetEnv("CDC_SSH_COMMAND", &ssh_command_).IgnoreError();
  cmd.add_argument(
      lyra::opt(ssh_command_, "ssh_command")
          .name("--ssh-command")
          .help("Path and arguments of ssh command to use, e.g. "
                "\"C:\\path\\to\\ssh.exe -F config_file\". Can also be "
                "specified by the CDC_SSH_COMMAND environment variable."));

  path::GetEnv("CDC_SCP_COMMAND", &scp_command_).IgnoreError();
  cmd.add_argument(
      lyra::opt(scp_command_, "scp_command")
          .name("--scp-command")
          .help("Path and arguments of scp command to use, e.g. "
                "\"C:\\path\\to\\scp.exe -F config_file\". Can also be "
                "specified by the CDC_SCP_COMMAND environment variable."));

  cmd.add_argument(lyra::arg(PosArgValidator(&src_dir_), "dir")
                       .required()
                       .help("Windows directory to stream"));

  cmd.add_argument(
      lyra::arg(PosArgValidator(&user_host_dir_), "[user@]host:src-dir")
          .required()
          .help("Linux host and directory to stream to"));
}

absl::Status StartCommand::Run() {
  LogLevel level = Log::VerbosityToLogLevel(verbosity_);
  ScopedLog scoped_log(std::make_unique<ConsoleLog>(level));
  LocalAssetsStreamManagerClient client(service_port_);

  std::string full_src_dir = path::GetFullPath(src_dir_);
  std::string user_host, mount_dir;
  RETURN_IF_ERROR(LocalAssetsStreamManagerClient::ParseUserHostDir(
      user_host_dir_, &user_host, &mount_dir));

  absl::Status status =
      client.StartSession(full_src_dir, user_host, ssh_port_, mount_dir,
                          ssh_command_, scp_command_);
  if (status.ok()) {
    LOG_INFO("Started streaming directory '%s' to '%s:%s'", src_dir_, user_host,
             mount_dir);
  }

  return status;
}

}  // namespace cdc_ft
