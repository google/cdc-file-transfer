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

#include "cdc_stream/asset_stream_config.h"

#include <sstream>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl_helper/jedec_size_flag.h"
#include "cdc_stream/base_command.h"
#include "cdc_stream/multi_session.h"
#include "cdc_stream/session_management_server.h"
#include "common/buffer.h"
#include "common/path.h"
#include "common/status_macros.h"
#include "data_store/data_provider.h"
#include "data_store/disk_data_store.h"
#include "json/json.h"
#include "lyra/lyra.hpp"

namespace cdc_ft {
namespace {
constexpr int kDefaultVerbosity = 2;
constexpr uint32_t kDefaultManifestUpdaterThreads = 4;
constexpr uint32_t kDefaultFileChangeWaitDurationMs = 500;
}  // namespace

AssetStreamConfig::AssetStreamConfig() = default;

AssetStreamConfig::~AssetStreamConfig() = default;

void AssetStreamConfig::RegisterCommandLineFlags(lyra::command& cmd,
                                                 BaseCommand& base_command) {
  service_port_ = SessionManagementServer::kDefaultServicePort;
  cmd.add_argument(lyra::opt(service_port_, "port")
                       .name("--service-port")
                       .help("Local port to use while connecting to the local "
                             "asset stream service, default: " +
                             std::to_string(service_port_)));

  session_cfg_.forward_port_first = MultiSession::kDefaultForwardPortFirst;
  session_cfg_.forward_port_last = MultiSession::kDefaultForwardPortLast;
  cmd.add_argument(
      lyra::opt(base_command.PortRangeParser("--forward-port",
                                             &session_cfg_.forward_port_first,
                                             &session_cfg_.forward_port_last),
                "port")
          .name("--forward-port")
          .help("TCP port or range used for SSH port forwarding, default: " +
                std::to_string(MultiSession::kDefaultForwardPortFirst) + "-" +
                std::to_string(MultiSession::kDefaultForwardPortLast) +
                ". If a range is specified, searches for available ports "
                "(slower)."));

  session_cfg_.verbosity = kDefaultVerbosity;
  cmd.add_argument(lyra::opt(session_cfg_.verbosity, "num")
                       .name("--verbosity")
                       .help("Verbosity of the log output, default: " +
                             std::to_string(kDefaultVerbosity) +
                             ". Increase to make logs more verbose."));

  cmd.add_argument(
      lyra::opt(session_cfg_.stats)
          .name("--stats")
          .help("Collect and print detailed streaming statistics"));

  cmd.add_argument(
      lyra::opt(session_cfg_.quiet)
          .name("--quiet")
          .help("Do not print any output except errors and stats"));

  session_cfg_.manifest_updater_threads = kDefaultManifestUpdaterThreads;
  cmd.add_argument(lyra::opt(session_cfg_.manifest_updater_threads, "count")
                       .name("--manifest-updater-threads")
                       .help("Number of threads used to compute file hashes on "
                             "the workstation, default: " +
                             std::to_string(kDefaultManifestUpdaterThreads)));

  session_cfg_.file_change_wait_duration_ms = kDefaultFileChangeWaitDurationMs;
  cmd.add_argument(
      lyra::opt(session_cfg_.file_change_wait_duration_ms, "ms")
          .name("--file-change-wait-duration-ms")
          .help("Time in milliseconds to wait until pushing a file change "
                "to the instance after detecting it, default: " +
                std::to_string(kDefaultFileChangeWaitDurationMs)));

  cmd.add_argument(lyra::opt(session_cfg_.fuse_debug)
                       .name("--debug")
                       .help("Run FUSE filesystem in debug mode"));

  cmd.add_argument(lyra::opt(session_cfg_.fuse_singlethreaded)
                       .name("--singlethreaded")
                       .optional()
                       .help("Run FUSE filesystem in single-threaded mode"));

  cmd.add_argument(lyra::opt(session_cfg_.fuse_check)
                       .name("--check")
                       .help("Check FUSE consistency and log check results"));

  session_cfg_.fuse_cache_capacity = DiskDataStore::kDefaultCapacity;
  cmd.add_argument(
      lyra::opt(base_command.JedecParser("--cache-capacity",
                                         &session_cfg_.fuse_cache_capacity),
                "bytes")
          .name("--cache-capacity")
          .help("FUSE cache capacity, default: " +
                std::to_string(DiskDataStore::kDefaultCapacity) +
                ". Supports common unit suffixes K, M, G."));

  session_cfg_.fuse_cleanup_timeout_sec = DataProvider::kCleanupTimeoutSec;
  cmd.add_argument(
      lyra::opt(session_cfg_.fuse_cleanup_timeout_sec, "sec")
          .name("--cleanup-timeout")
          .help("Period in seconds at which instance cache cleanups are run, "
                "default: " +
                std::to_string(DataProvider::kCleanupTimeoutSec)));

  session_cfg_.fuse_access_idle_timeout_sec = DataProvider::kAccessIdleSec;
  cmd.add_argument(
      lyra::opt(session_cfg_.fuse_access_idle_timeout_sec, "sec")
          .name("--access-idle-timeout")
          .help("Do not run instance cache cleanups for this long after the "
                "last file access, default: " +
                std::to_string(DataProvider::kAccessIdleSec)));

  cmd.add_argument(lyra::opt(log_to_stdout_)
                       .name("--log-to-stdout")
                       .help("Log to stdout instead of to a file"));
  cmd.add_argument(
      lyra::opt(dev_src_dir_, "dir")
          .name("--dev-src-dir")
          .help("Start a streaming session immediately from the given Windows "
                "path. Used during development. Must also specify other --dev "
                "flags."));

  cmd.add_argument(
      lyra::opt(dev_target_.user_host, "[user@]host")
          .name("--dev-user-host")
          .help("Username and host to stream to. See also --dev-src-dir."));

  cmd.add_argument(
      lyra::opt(dev_target_.ssh_command, "cmd")
          .name("--dev-ssh-command")
          .help("Ssh command and extra flags to use for the "
                "connection to the host. See also --dev-src-dir."));

  cmd.add_argument(
      lyra::opt(dev_target_.sftp_command, "cmd")
          .name("--dev-sftp-command")
          .help("Sftp command and extra flags to use for the "
                "connection to the host. See also --dev-src-dir."));

  cmd.add_argument(
      lyra::opt(dev_target_.mount_dir, "dir")
          .name("--dev-mount-dir")
          .help("Directory on the host to stream to. See also --dev-src-dir."));
}

absl::Status AssetStreamConfig::LoadFromFile(const std::string& path) {
  Buffer buffer;
  RETURN_IF_ERROR(path::ReadFile(path, &buffer));

  Json::Value config;
  Json::Reader reader;
  if (!reader.parse(buffer.data(), buffer.data() + buffer.size(), config,
                    false)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Failed to parse config file '%s': %s", path,
                        reader.getFormattedErrorMessages()));
  }

#define ASSIGN_VAR(var, flag, type)       \
  do {                                    \
    if (config.isMember(flag)) {          \
      var = config[flag].as##type();      \
      flags_read_from_file_.insert(flag); \
    }                                     \
  } while (0)

  ASSIGN_VAR(service_port_, "service-port", Int);
  ASSIGN_VAR(session_cfg_.forward_port_first, "forward-port-first", Int);
  ASSIGN_VAR(session_cfg_.forward_port_last, "forward-port-last", Int);
  ASSIGN_VAR(session_cfg_.verbosity, "verbosity", Int);
  ASSIGN_VAR(session_cfg_.fuse_debug, "debug", Bool);
  ASSIGN_VAR(session_cfg_.fuse_singlethreaded, "singlethreaded", Bool);
  ASSIGN_VAR(session_cfg_.stats, "stats", Bool);
  ASSIGN_VAR(session_cfg_.quiet, "quiet", Bool);
  ASSIGN_VAR(session_cfg_.fuse_check, "check", Bool);
  ASSIGN_VAR(log_to_stdout_, "log-to-stdout", Bool);
  ASSIGN_VAR(session_cfg_.fuse_cleanup_timeout_sec, "cleanup-timeout", Int);
  ASSIGN_VAR(session_cfg_.fuse_access_idle_timeout_sec, "access-idle-timeout",
             Int);
  ASSIGN_VAR(session_cfg_.manifest_updater_threads, "manifest-updater-threads",
             Int);
  ASSIGN_VAR(session_cfg_.file_change_wait_duration_ms,
             "file-change-wait-duration-ms", Int);

  // cache_capacity requires Jedec size conversion.
  constexpr char kCacheCapacity[] = "cache-capacity";
  if (config.isMember(kCacheCapacity)) {
    JedecSize cache_capacity;
    std::string error;
    if (AbslParseFlag(config[kCacheCapacity].asString(), &cache_capacity,
                      &error)) {
      session_cfg_.fuse_cache_capacity = cache_capacity.Size();
      flags_read_from_file_.insert(kCacheCapacity);
    } else {
      // Note that |error| can't be logged here since this code runs before
      // logging is initialized.
      flag_read_errors_[kCacheCapacity] = error;
    }
  }

#undef ASSIGN_VAR

  return absl::OkStatus();
}  // namespace cdc_ft

std::string AssetStreamConfig::ToString() {
  std::ostringstream ss;
  ss << "service-port                 = " << service_port_ << std::endl;
  ss << "forward-port                 = " << session_cfg_.forward_port_first
     << "-" << session_cfg_.forward_port_last << std::endl;
  ss << "verbosity                    = " << session_cfg_.verbosity
     << std::endl;
  ss << "debug                        = " << session_cfg_.fuse_debug
     << std::endl;
  ss << "singlethreaded               = " << session_cfg_.fuse_singlethreaded
     << std::endl;
  ss << "stats                        = " << session_cfg_.stats << std::endl;
  ss << "quiet                        = " << session_cfg_.quiet << std::endl;
  ss << "check                        = " << session_cfg_.fuse_check
     << std::endl;
  ss << "log-to-stdout                = " << log_to_stdout_ << std::endl;
  ss << "cache-capacity               = " << session_cfg_.fuse_cache_capacity
     << std::endl;
  ss << "cleanup-timeout              = "
     << session_cfg_.fuse_cleanup_timeout_sec << std::endl;
  ss << "access-idle-timeout          = "
     << session_cfg_.fuse_access_idle_timeout_sec << std::endl;
  ss << "manifest-updater-threads     = "
     << session_cfg_.manifest_updater_threads << std::endl;
  ss << "file-change-wait-duration-ms = "
     << session_cfg_.file_change_wait_duration_ms << std::endl;
  ss << "dev-src-dir                  = " << dev_src_dir_ << std::endl;
  ss << "dev-user-host                = " << dev_target_.user_host << std::endl;
  ss << "dev-ssh-command              = " << dev_target_.ssh_command
     << std::endl;
  ss << "dev-sftp-command             = " << dev_target_.sftp_command
     << std::endl;
  ss << "dev-mount-dir                = " << dev_target_.mount_dir << std::endl;
  return ss.str();
}

std::string AssetStreamConfig::GetFlagsReadFromFile() {
  return absl::StrJoin(flags_read_from_file_, ", ");
}

std::string AssetStreamConfig::GetFlagReadErrors() {
  std::string error_str;
  for (const auto& [flag, error] : flag_read_errors_)
    error_str += absl::StrFormat("%sFailed to read '%s': %s",
                                 error_str.empty() ? "" : "\n", flag, error);
  return error_str;
}

}  // namespace cdc_ft
