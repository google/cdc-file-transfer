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

#include "asset_stream_manager/asset_stream_config.h"

#include <sstream>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl_helper/jedec_size_flag.h"
#include "common/buffer.h"
#include "common/path.h"
#include "common/status_macros.h"
#include "json/json.h"

ABSL_DECLARE_FLAG(std::string, src_dir);
ABSL_DECLARE_FLAG(std::string, instance_ip);
ABSL_DECLARE_FLAG(uint16_t, instance_port);
ABSL_DECLARE_FLAG(int, verbosity);
ABSL_DECLARE_FLAG(bool, debug);
ABSL_DECLARE_FLAG(bool, singlethreaded);
ABSL_DECLARE_FLAG(bool, stats);
ABSL_DECLARE_FLAG(bool, quiet);
ABSL_DECLARE_FLAG(bool, check);
ABSL_DECLARE_FLAG(bool, log_to_stdout);
ABSL_DECLARE_FLAG(cdc_ft::JedecSize, cache_capacity);
ABSL_DECLARE_FLAG(uint32_t, cleanup_timeout);
ABSL_DECLARE_FLAG(uint32_t, access_idle_timeout);
ABSL_DECLARE_FLAG(int, manifest_updater_threads);
ABSL_DECLARE_FLAG(int, file_change_wait_duration_ms);

// Declare AS20 flags, so that AS30 can be used on older SDKs simply by
// replacing the binary. Note that the RETIRED_FLAGS macro can't be used
// because the flags contain dashes. This code mimics the macro.
absl::flags_internal::RetiredFlag<std::string> RETIRED_FLAGS_session_ports;
absl::flags_internal::RetiredFlag<std::string> RETIRED_FLAGS_gm_mount_point;
absl::flags_internal::RetiredFlag<bool> RETIRED_FLAGS_allow_edge;

const auto RETIRED_FLAGS_REG_session_ports =
    (RETIRED_FLAGS_session_ports.Retire("session-ports"),
     ::absl::flags_internal::FlagRegistrarEmpty{});
const auto RETIRED_FLAGS_REG_gm_mount_point =
    (RETIRED_FLAGS_gm_mount_point.Retire("gamelet-mount-point"),
     ::absl::flags_internal::FlagRegistrarEmpty{});
const auto RETIRED_FLAGS_REG_allow_edge =
    (RETIRED_FLAGS_allow_edge.Retire("allow-edge"),
     ::absl::flags_internal::FlagRegistrarEmpty{});

namespace cdc_ft {

AssetStreamConfig::AssetStreamConfig() {
  src_dir_ = absl::GetFlag(FLAGS_src_dir);
  instance_ip_ = absl::GetFlag(FLAGS_instance_ip);
  instance_port_ = absl::GetFlag(FLAGS_instance_port);
  session_cfg_.verbosity = absl::GetFlag(FLAGS_verbosity);
  session_cfg_.fuse_debug = absl::GetFlag(FLAGS_debug);
  session_cfg_.fuse_singlethreaded = absl::GetFlag(FLAGS_singlethreaded);
  session_cfg_.stats = absl::GetFlag(FLAGS_stats);
  session_cfg_.quiet = absl::GetFlag(FLAGS_quiet);
  session_cfg_.fuse_check = absl::GetFlag(FLAGS_check);
  log_to_stdout_ = absl::GetFlag(FLAGS_log_to_stdout);
  session_cfg_.fuse_cache_capacity = absl::GetFlag(FLAGS_cache_capacity).Size();
  session_cfg_.fuse_cleanup_timeout_sec = absl::GetFlag(FLAGS_cleanup_timeout);
  session_cfg_.fuse_access_idle_timeout_sec =
      absl::GetFlag(FLAGS_access_idle_timeout);
  session_cfg_.manifest_updater_threads =
      absl::GetFlag(FLAGS_manifest_updater_threads);
  session_cfg_.file_change_wait_duration_ms =
      absl::GetFlag(FLAGS_file_change_wait_duration_ms);
}

AssetStreamConfig::~AssetStreamConfig() = default;

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

#define ASSIGN_VAR(var, flag, type)        \
  do {                                     \
    if (config.isMember(#flag)) {          \
      var = config[#flag].as##type();      \
      flags_read_from_file_.insert(#flag); \
    }                                      \
  } while (0)

  ASSIGN_VAR(src_dir_, src_dir, String);
  ASSIGN_VAR(session_cfg_.verbosity, verbosity, Int);
  ASSIGN_VAR(session_cfg_.fuse_debug, debug, Bool);
  ASSIGN_VAR(session_cfg_.fuse_singlethreaded, singlethreaded, Bool);
  ASSIGN_VAR(session_cfg_.stats, stats, Bool);
  ASSIGN_VAR(session_cfg_.quiet, quiet, Bool);
  ASSIGN_VAR(session_cfg_.fuse_check, check, Bool);
  ASSIGN_VAR(log_to_stdout_, log_to_stdout, Bool);
  ASSIGN_VAR(session_cfg_.fuse_cleanup_timeout_sec, cleanup_timeout, Int);
  ASSIGN_VAR(session_cfg_.fuse_access_idle_timeout_sec, access_idle_timeout,
             Int);
  ASSIGN_VAR(session_cfg_.manifest_updater_threads, manifest_updater_threads,
             Int);
  ASSIGN_VAR(session_cfg_.file_change_wait_duration_ms,
             file_change_wait_duration_ms, Int);

  // cache_capacity requires Jedec size conversion.
  constexpr char kCacheCapacity[] = "cache_capacity";
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
  ss << "src_dir                      = " << src_dir_ << std::endl;
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
  ss << "log_to_stdout                = " << log_to_stdout_ << std::endl;
  ss << "cache_capacity               = " << session_cfg_.fuse_cache_capacity
     << std::endl;
  ss << "cleanup_timeout              = "
     << session_cfg_.fuse_cleanup_timeout_sec << std::endl;
  ss << "access_idle_timeout          = "
     << session_cfg_.fuse_access_idle_timeout_sec << std::endl;
  ss << "manifest_updater_threads     = "
     << session_cfg_.manifest_updater_threads << std::endl;
  ss << "file_change_wait_duration_ms = "
     << session_cfg_.file_change_wait_duration_ms << std::endl;
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
