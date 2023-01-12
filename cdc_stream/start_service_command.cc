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

#include "cdc_stream/start_service_command.h"

#include "cdc_stream/background_service_impl.h"
#include "cdc_stream/local_assets_stream_manager_service_impl.h"
#include "cdc_stream/session_management_server.h"
#include "cdc_stream/session_manager.h"
#include "common/clock.h"
#include "common/grpc_status.h"
#include "common/log.h"
#include "common/path.h"
#include "common/process.h"
#include "common/status_macros.h"
#include "lyra/lyra.hpp"
#include "metrics/metrics.h"

namespace cdc_ft {
namespace {

std::string GetLogPath(const char* log_dir, const char* log_base_name) {
  DefaultSystemClock* clock = DefaultSystemClock::GetInstance();
  std::string timestamp_ext = clock->FormatNow(".%Y%m%d-%H%M%S.log", false);
  return path::Join(log_dir, log_base_name + timestamp_ext);
}

}  // namespace

StartServiceCommand::StartServiceCommand(int* exit_code)
    : BaseCommand("start-service", "Start the streaming service", exit_code) {}
StartServiceCommand::~StartServiceCommand() = default;

void StartServiceCommand::RegisterCommandLineFlags(lyra::command& cmd) {
  config_file_ = "%APPDATA%\\cdc-file-transfer\\cdc_stream.json";
  cmd.add_argument(
      lyra::opt(config_file_, "path")
          .name("--config-file")
          .help("Json configuration file, default: " + config_file_));

  log_dir_ = "%APPDATA%\\cdc-file-transfer\\logs";
  cmd.add_argument(
      lyra::opt(log_dir_, "dir")
          .name("--log-dir")
          .help("Directory to store log files, default: " + log_dir_));

  cfg_.RegisterCommandLineFlags(cmd, *this);
}

absl::Status StartServiceCommand::Run() {
  // Set up config. Allow overriding this config with |config_file|.
  absl::Status cfg_load_status = path::ExpandPathVariables(&config_file_);
  cfg_load_status.Update(cfg_.LoadFromFile(config_file_));

  std::unique_ptr<Log> logger;
  ASSIGN_OR_RETURN(logger, GetLogger());
  cdc_ft::ScopedLog scoped_log(std::move(logger));

  // Log status of loaded configuration. Errors are not critical.
  if (cfg_load_status.ok()) {
    LOG_INFO("Successfully loaded configuration file at '%s'", config_file_);
  } else if (absl::IsNotFound(cfg_load_status)) {
    LOG_INFO("No configuration file found at '%s'", config_file_);
  } else {
    LOG_ERROR("%s", cfg_load_status.message());
  }

  std::string flags_read = cfg_.GetFlagsReadFromFile();
  if (!flags_read.empty()) {
    LOG_INFO(
        "The following settings were read from the configuration file and "
        "override the corresponding command line flags if set: %s",
        flags_read);
  }

  std::string flag_errors = cfg_.GetFlagReadErrors();
  if (!flag_errors.empty()) {
    LOG_WARNING("%s", flag_errors);
  }

  LOG_DEBUG("Configuration:\n%s", cfg_.ToString());

  absl::Status status = RunService();
  if (!status.ok()) {
    LOG_ERROR("%s", status.ToString());
  } else {
    LOG_INFO("Streaming service shut down successfully.");
  }

  return status;
}

absl::StatusOr<std::unique_ptr<Log>> StartServiceCommand::GetLogger() {
  LogLevel level = Log::VerbosityToLogLevel(cfg_.session_cfg().verbosity);
  if (cfg_.log_to_stdout()) {
    // Log to stdout.
    return std::make_unique<ConsoleLog>(level);
  }

  // Log to file.
  if (!path::ExpandPathVariables(&log_dir_).ok() ||
      !path::CreateDirRec(log_dir_).ok()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Failed to create log directory '%s'", log_dir_));
  }

  return std::make_unique<FileLog>(
      level, GetLogPath(log_dir_.c_str(), "cdc_stream").c_str());
}

// Runs the session management service and returns when it finishes.
absl::Status StartServiceCommand::RunService() {
  WinProcessFactory process_factory;
  metrics::MetricsService metrics_service;

  SessionManager session_manager(cfg_.session_cfg(), &process_factory,
                                 &metrics_service);
  BackgroundServiceImpl background_service;
  LocalAssetsStreamManagerServiceImpl session_service(
      &session_manager, &process_factory, &metrics_service);

  SessionManagementServer sm_server(&session_service, &background_service,
                                    &session_manager);
  background_service.SetExitCallback(
      [&sm_server]() { return sm_server.Shutdown(); });

  if (!cfg_.dev_src_dir().empty()) {
    localassetsstreammanager::StartSessionRequest request;
    request.set_workstation_directory(cfg_.dev_src_dir());
    request.set_user_host(cfg_.dev_target().user_host);
    request.set_mount_dir(cfg_.dev_target().mount_dir);
    request.set_ssh_command(cfg_.dev_target().ssh_command);
    request.set_sftp_command(cfg_.dev_target().sftp_command);
    localassetsstreammanager::StartSessionResponse response;
    RETURN_ABSL_IF_ERROR(
        session_service.StartSession(nullptr, &request, &response));
  }
  RETURN_IF_ERROR(sm_server.Start(cfg_.service_port()));
  sm_server.RunUntilShutdown();
  return absl::OkStatus();
}

}  // namespace cdc_ft
