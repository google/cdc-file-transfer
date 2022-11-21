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

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl_helper/jedec_size_flag.h"
#include "asset_stream_manager/asset_stream_config.h"
#include "asset_stream_manager/background_service_impl.h"
#include "asset_stream_manager/local_assets_stream_manager_service_impl.h"
#include "asset_stream_manager/session_management_server.h"
#include "asset_stream_manager/session_manager.h"
#include "common/grpc_status.h"
#include "common/log.h"
#include "common/path.h"
#include "common/process.h"
#include "common/status_macros.h"
#include "data_store/data_provider.h"
#include "data_store/disk_data_store.h"
#include "metrics/metrics.h"

namespace cdc_ft {
namespace {

constexpr int kSessionManagementPort = 44432;

absl::Status Run(const AssetStreamConfig& cfg) {
  WinProcessFactory process_factory;
  metrics::MetricsService metrics_service;

  SessionManager session_manager(cfg.session_cfg(), &process_factory,
                                 &metrics_service);
  BackgroundServiceImpl background_service;
  LocalAssetsStreamManagerServiceImpl session_service(
      &session_manager, &process_factory, &metrics_service);

  SessionManagementServer sm_server(&session_service, &background_service,
                                    &session_manager);
  background_service.SetExitCallback(
      [&sm_server]() { return sm_server.Shutdown(); });

  if (!cfg.dev_src_dir().empty()) {
    localassetsstreammanager::StartSessionRequest request;
    request.set_workstation_directory(cfg.dev_src_dir());
    request.set_user_host(cfg.dev_target().user_host);
    request.set_mount_dir(cfg.dev_target().mount_dir);
    request.set_port(cfg.dev_target().ssh_port);
    request.set_ssh_command(cfg.dev_target().ssh_command);
    request.set_scp_command(cfg.dev_target().scp_command);
    localassetsstreammanager::StartSessionResponse response;
    RETURN_ABSL_IF_ERROR(
        session_service.StartSession(nullptr, &request, &response));
  }
  RETURN_IF_ERROR(sm_server.Start(kSessionManagementPort));
  sm_server.RunUntilShutdown();
  return absl::OkStatus();
}

std::string GetLogPath(const char* log_dir, const char* log_base_name) {
  DefaultSystemClock* clock = DefaultSystemClock::GetInstance();
  std::string timestamp_ext = clock->FormatNow(".%Y%m%d-%H%M%S.log", false);
  return path::Join(log_dir, log_base_name + timestamp_ext);
}

void InitLogging(std::string& log_dir, bool log_to_stdout, int verbosity) {
  LogLevel level = cdc_ft::Log::VerbosityToLogLevel(verbosity);
  if (log_to_stdout) {
    cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(level));
  } else {
    if (path::ExpandPathVariables(&log_dir).ok() &&
        path::CreateDirRec(log_dir).ok()) {
      cdc_ft::Log::Initialize(std::make_unique<cdc_ft::FileLog>(
          level, GetLogPath(log_dir.c_str(), "assets_stream_manager").c_str()));
    } else {
      LOG_WARNING("Failed to create log directory or expand directory path %s",
                  log_dir);
      cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(level));
      LOG_WARNING("Started to write console log");
    }
  }
}

// Declare AS20 flags, so that AS30 can be used on older SDKs simply by
// replacing the binary. Note that the RETIRED_FLAGS macro can't be used
// because the flags contain dashes. This code mimics the macro.
absl::flags_internal::RetiredFlag<int> RETIRED_FLAGS_port;
absl::flags_internal::RetiredFlag<std::string> RETIRED_FLAGS_session_ports;
absl::flags_internal::RetiredFlag<std::string> RETIRED_FLAGS_gm_mount_point;
absl::flags_internal::RetiredFlag<bool> RETIRED_FLAGS_allow_edge;
const auto RETIRED_FLAGS_REG_port =
    (RETIRED_FLAGS_port.Retire("port"),
     ::absl::flags_internal::FlagRegistrarEmpty{});
const auto RETIRED_FLAGS_REG_session_ports =
    (RETIRED_FLAGS_session_ports.Retire("session-ports"),
     ::absl::flags_internal::FlagRegistrarEmpty{});
const auto RETIRED_FLAGS_REG_gm_mount_point =
    (RETIRED_FLAGS_gm_mount_point.Retire("gamelet-mount-point"),
     ::absl::flags_internal::FlagRegistrarEmpty{});
const auto RETIRED_FLAGS_REG_allow_edge =
    (RETIRED_FLAGS_allow_edge.Retire("allow-edge"),
     ::absl::flags_internal::FlagRegistrarEmpty{});

}  // namespace
}  // namespace cdc_ft

ABSL_FLAG(int, verbosity, 2, "Verbosity of the log output");
ABSL_FLAG(bool, debug, false, "Run FUSE filesystem in debug mode");
ABSL_FLAG(bool, singlethreaded, false,
          "Run FUSE filesystem in singlethreaded mode");
ABSL_FLAG(bool, stats, false,
          "Collect and print detailed streaming statistics");
ABSL_FLAG(bool, quiet, false,
          "Do not print any output except errors and stats");
ABSL_FLAG(int, manifest_updater_threads, 4,
          "Number of threads used to compute file hashes on the workstation.");
ABSL_FLAG(int, file_change_wait_duration_ms, 500,
          "Time in milliseconds to wait until pushing a file change to the "
          "instance after detecting it");
ABSL_FLAG(bool, check, false, "Check FUSE consistency and log check results");
ABSL_FLAG(bool, log_to_stdout, false, "Log to stdout instead of to a file");
ABSL_FLAG(cdc_ft::JedecSize, cache_capacity,
          cdc_ft::JedecSize(cdc_ft::DiskDataStore::kDefaultCapacity),
          "Cache capacity. Supports common unit suffixes K, M, G");
ABSL_FLAG(uint32_t, cleanup_timeout, cdc_ft::DataProvider::kCleanupTimeoutSec,
          "Period in seconds at which instance cache cleanups are run");
ABSL_FLAG(uint32_t, access_idle_timeout, cdc_ft::DataProvider::kAccessIdleSec,
          "Do not run instance cache cleanups for this many seconds after the "
          "last file access");
ABSL_FLAG(std::string, config_file,
          "%APPDATA%\\cdc-file-transfer\\assets_stream_manager.json",
          "Json configuration file for asset stream manager");
ABSL_FLAG(std::string, log_dir, "%APPDATA%\\cdc-file-transfer\\logs",
          "Directory to store log files for asset stream manager");

// Development args.
ABSL_FLAG(std::string, dev_src_dir, "",
          "Start a streaming session immediately from the given Windows path. "
          "Used during development. Must also specify --dev_user_host and "
          "--dev_mount_dir and possibly other --dev flags, depending on the "
          "SSH setup");
ABSL_FLAG(std::string, dev_user_host, "",
          "Username and host to stream to, of the form [user@]host. Used "
          "during development. See --dev_src_dir for more info.");
ABSL_FLAG(uint16_t, dev_ssh_port, cdc_ft::RemoteUtil::kDefaultSshPort,
          "SSH port to use for the connection to the host. Used during "
          "development. See --dev_src_dir for more info.");
ABSL_FLAG(std::string, dev_ssh_command, "",
          "Ssh command and extra flags to use for the connection to the host. "
          "Used during development. See --dev_src_dir for more info.");
ABSL_FLAG(std::string, dev_scp_command, "",
          "Scp command and extra flags to use for the connection to the host. "
          "Used during development. See --dev_src_dir for more info.");
ABSL_FLAG(std::string, dev_mount_dir, "",
          "Directory on the host to stream to. Used during development. See "
          "--dev_src_dir for more info.");

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  // Set up config. Allow overriding this config |config_file|.
  cdc_ft::AssetStreamConfig cfg;
  std::string config_file = absl::GetFlag(FLAGS_config_file);
  absl::Status cfg_load_status =
      cdc_ft::path::ExpandPathVariables(&config_file);
  if (cfg_load_status.ok()) {
    cfg_load_status = cfg.LoadFromFile(config_file);
  }

  std::string log_dir = absl::GetFlag(FLAGS_log_dir);
  cdc_ft::InitLogging(log_dir, cfg.log_to_stdout(),
                      cfg.session_cfg().verbosity);

  // Log status of loaded configuration. Errors are not critical.
  if (cfg_load_status.ok()) {
    LOG_INFO("Successfully loaded configuration file at '%s'", config_file);
  } else if (absl::IsNotFound(cfg_load_status)) {
    LOG_INFO("No configuration file found at '%s'", config_file);
  } else {
    LOG_ERROR("%s", cfg_load_status.message());
  }

  std::string flags_read = cfg.GetFlagsReadFromFile();
  if (!flags_read.empty()) {
    LOG_INFO(
        "The following settings were read from the configuration file and "
        "override the corresponding command line flags if set: %s",
        flags_read);
  }

  std::string flag_errors = cfg.GetFlagReadErrors();
  if (!flag_errors.empty()) {
    LOG_WARNING("%s", flag_errors);
  }

  LOG_DEBUG("Configuration:\n%s", cfg.ToString());

  absl::Status status = cdc_ft::Run(cfg);
  if (!status.ok()) {
    LOG_ERROR("%s", status.ToString());
  } else {
    LOG_INFO("Asset stream manager shut down successfully.");
  }

  cdc_ft::Log::Shutdown();
  static_assert(static_cast<int>(absl::StatusCode::kOk) == 0, "kOk not 0");
  return static_cast<int>(status.code());
}
