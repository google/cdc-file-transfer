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

#include "cdc_stream/start_command.h"

#include <memory>

#include "cdc_stream/background_service_client.h"
#include "cdc_stream/local_assets_stream_manager_client.h"
#include "cdc_stream/session_management_server.h"
#include "common/log.h"
#include "common/path.h"
#include "common/process.h"
#include "common/remote_util.h"
#include "common/status_macros.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/support/channel_arguments.h"
#include "lyra/lyra.hpp"

namespace cdc_ft {
namespace {
constexpr int kDefaultVerbosity = 2;
}  // namespace

namespace {
// Time to poll until the streaming service becomes healthy.
constexpr double kServiceStartupTimeoutSec = 20.0;

std::shared_ptr<grpc::Channel> CreateChannel(uint16_t service_port) {
  std::string client_address = absl::StrFormat("localhost:%u", service_port);
  return grpc::CreateCustomChannel(client_address,
                                   grpc::InsecureChannelCredentials(),
                                   grpc::ChannelArguments());
}

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

  path::GetEnv("CDC_SSH_COMMAND", &ssh_command_).IgnoreError();
  cmd.add_argument(
      lyra::opt(ssh_command_, "cmd")
          .name("--ssh-command")
          .help("Path and arguments of ssh command to use, e.g. "
                "\"C:\\path\\to\\ssh.exe -F config_file -p 1234\". Can also be "
                "specified by the CDC_SSH_COMMAND environment variable."));

  path::GetEnv("CDC_SFTP_COMMAND", &sftp_command_).IgnoreError();
  cmd.add_argument(
      lyra::opt(sftp_command_, "cmd")
          .name("--sftp-command")
          .help(
              "Path and arguments of sftp command to use, e.g. "
              "\"C:\\path\\to\\sftp.exe -F config_file -P 1234\". Can also be "
              "specified by the CDC_SFTP_COMMAND environment variable."));

  path::GetEnv("CDC_SCP_COMMAND", &deprecated_scp_command_).IgnoreError();
  cmd.add_argument(
      lyra::opt(deprecated_scp_command_, "cmd")
          .name("--scp-command")
          .help("[Deprecated, use --sftp-command] Path and arguments of scp "
                "command to "
                "use, e.g. "
                "\"C:\\path\\to\\scp.exe -F config_file -P 1234\". Can also be "
                "specified by the CDC_SCP_COMMAND environment variable."));

  cmd.add_argument(lyra::arg(PosArgValidator(&src_dir_), "dir")
                       .required()
                       .help("Windows directory to stream"));

  cmd.add_argument(
      lyra::arg(PosArgValidator(&user_host_dir_), "[user@]host:dir")
          .required()
          .help("Linux host and directory to stream to"));
}

absl::Status StartCommand::Run() {
  LogLevel level = Log::VerbosityToLogLevel(verbosity_);
  ScopedLog scoped_log(std::make_unique<ConsoleLog>(level));

  std::string full_src_dir = path::GetFullPath(src_dir_);
  std::string user_host, mount_dir;
  RETURN_IF_ERROR(LocalAssetsStreamManagerClient::ParseUserHostDir(
      user_host_dir_, &user_host, &mount_dir));

  // Backwards compatibility after switching from scp to sftp.
  if (sftp_command_.empty() && !deprecated_scp_command_.empty()) {
    LOG_WARNING(
        "The CDC_SCP_COMMAND environment variable and the --scp-command flag "
        "are deprecated. Please set CDC_SFTP_COMMAND or --sftp-command "
        "instead.");

    sftp_command_ = RemoteUtil::ScpToSftpCommand(deprecated_scp_command_);
    if (!sftp_command_.empty()) {
      LOG_WARNING("Converted scp command '%s' to sftp command '%s'.",
                  deprecated_scp_command_, sftp_command_);
    } else {
      LOG_WARNING("Failed to convert scp command '%s' to sftp command.",
                  deprecated_scp_command_);
    }
  }

  LocalAssetsStreamManagerClient client(CreateChannel(service_port_));
  absl::Status status = client.StartSession(full_src_dir, user_host, mount_dir,
                                            ssh_command_, sftp_command_);

  if (absl::IsUnavailable(status)) {
    LOG_DEBUG("StartSession status: %s", status.ToString());
    LOG_INFO("Streaming service is unavailable. Starting it...");
    status = StartStreamingService();

    if (status.ok()) {
      LOG_INFO("Streaming service successfully started");

      // Recreate client. The old channel might still be in a transient failure
      // state.
      LocalAssetsStreamManagerClient new_client(CreateChannel(service_port_));
      status = new_client.StartSession(full_src_dir, user_host, mount_dir,
                                       ssh_command_, sftp_command_);
    }
  }

  if (status.ok()) {
    LOG_INFO("Started streaming directory '%s' to '%s:%s'", src_dir_, user_host,
             mount_dir);
  }

  return status;
}

absl::Status StartCommand::StartStreamingService() {
  std::string exe_dir;
  RETURN_IF_ERROR(path::GetExeDir(&exe_dir),
                  "Failed to get executable directory");
  std::string exe_path = path::Join(exe_dir, "cdc_stream");

  // Try starting the service first.
  WinProcessFactory process_factory;
  ProcessStartInfo start_info;
  start_info.command =
      absl::StrFormat("%s start-service --verbosity=%i --service-port=%i",
                      exe_path, verbosity_, service_port_);
  start_info.flags = ProcessFlags::kDetached;
  std::unique_ptr<Process> service_process = process_factory.Create(start_info);
  RETURN_IF_ERROR(service_process->Start(),
                  "Failed to start asset streaming service");

  // Poll until the service becomes healthy.
  LOG_INFO("Streaming service initializing...");
  Stopwatch sw;
  while (sw.ElapsedSeconds() < kServiceStartupTimeoutSec) {
    // The channel is in some transient failure state, and it's faster to
    // reconnect instead of waiting for it to return.
    BackgroundServiceClient bg_client(CreateChannel(service_port_));
    absl::Status status = bg_client.IsHealthy();
    if (status.ok()) {
      return absl::OkStatus();
    }
    LOG_DEBUG("Health check result: %s", status.ToString());
    Util::Sleep(100);
  }

  // Kill the process.
  service_process->Terminate();
  return absl::DeadlineExceededError(
      absl::StrFormat("Timed out after %0.0f seconds waiting for the asset "
                      "streaming service to become healthy",
                      kServiceStartupTimeoutSec));
}

}  // namespace cdc_ft
