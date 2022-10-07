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

#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl_helper/jedec_size_flag.h"
#include "cdc_fuse_fs/cdc_fuse_fs.h"
#include "cdc_fuse_fs/constants.h"
#include "common/gamelet_component.h"
#include "common/log.h"
#include "common/path.h"
#include "data_store/data_provider.h"
#include "data_store/disk_data_store.h"
#include "data_store/grpc_reader.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/support/channel_arguments.h"

namespace cdc_ft {
namespace {

constexpr char kFuseFilename[] = "cdc_fuse_fs";
constexpr char kLibFuseFilename[] = "libfuse.so";

absl::StatusOr<bool> IsUpToDate(const std::string& components_arg) {
  // Components are expected to reside in the same dir as the executable.
  std::string component_dir;
  RETURN_IF_ERROR(path::GetExeDir(&component_dir));

  std::vector<GameletComponent> components =
      GameletComponent::FromCommandLineArgs(components_arg);
  std::vector<GameletComponent> our_components;
  absl::Status status =
      GameletComponent::Get({path::Join(component_dir, kFuseFilename),
                             path::Join(component_dir, kLibFuseFilename)},
                            &our_components);
  if (!status.ok() || components != our_components) {
    return false;
  }

  return true;
}

}  // namespace
}  // namespace cdc_ft

ABSL_FLAG(std::string, instance, "", "Gamelet instance id");
ABSL_FLAG(
    std::string, components, "",
    "Whitespace-separated triples filename, size and timestamp of the "
    "workstation version of this binary and dependencies. Used for a fast "
    "up-to-date check.");
ABSL_FLAG(uint16_t, port, 0, "Port to connect to on localhost");
ABSL_FLAG(cdc_ft::JedecSize, prefetch_size, cdc_ft::JedecSize(512 << 10),
          "Additional data to request from the server when a FUSE read of "
          "maximum size is detected. This amount is added to the original "
          "request. Supports common unit suffixes K, M, G");
ABSL_FLAG(std::string, cache_dir, "/var/cache/asset_streaming",
          "Cache directory to store data chunks.");
ABSL_FLAG(int, cache_dir_levels, 2,
          "Fanout of sub-directories to create within the cache directory.");
ABSL_FLAG(int, verbosity, 0, "Log verbosity");
ABSL_FLAG(bool, stats, false, "Enable statistics");
ABSL_FLAG(bool, check, false, "Execute consistency check");
ABSL_FLAG(cdc_ft::JedecSize, cache_capacity,
          cdc_ft::JedecSize(cdc_ft::DiskDataStore::kDefaultCapacity),
          "Cache capacity. Supports common unit suffixes K, M, G.");
ABSL_FLAG(uint32_t, cleanup_timeout, cdc_ft::DataProvider::kCleanupTimeoutSec,
          "Period in seconds at which instance cache cleanups are run");
ABSL_FLAG(uint32_t, access_idle_timeout, cdc_ft::DataProvider::kAccessIdleSec,
          "Do not run instance cache cleanups for this many seconds after the "
          "last file access");

static_assert(static_cast<int>(absl::StatusCode::kOk) == 0, "kOk != 0");

// Usage: cdc_fuse_fs <ABSL_FLAGs> -- mount_dir [-d|-s|..]
// Any args after --  are FUSE args, search third_party/fuse for FUSE_OPT_KEY or
// FUSE_LIB_OPT (there doesn't seem to be a place where they're all described).
int main(int argc, char* argv[]) {
  // Parse absl flags.
  std::vector<char*> mount_args = absl::ParseCommandLine(argc, argv);
  std::string instance = absl::GetFlag(FLAGS_instance);
  std::string components = absl::GetFlag(FLAGS_components);
  uint16_t port = absl::GetFlag(FLAGS_port);
  std::string cache_dir = absl::GetFlag(FLAGS_cache_dir);
  int cache_dir_levels = absl::GetFlag(FLAGS_cache_dir_levels);
  int verbosity = absl::GetFlag(FLAGS_verbosity);
  bool stats = absl::GetFlag(FLAGS_stats);
  bool consistency_check = absl::GetFlag(FLAGS_check);
  uint64_t cache_capacity = absl::GetFlag(FLAGS_cache_capacity).Size();
  unsigned int dp_cleanup_timeout = absl::GetFlag(FLAGS_cleanup_timeout);
  unsigned int dp_access_idle_timeout =
      absl::GetFlag(FLAGS_access_idle_timeout);

  // Log to console. Logs are streamed back to the workstation through the SSH
  // session.
  cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(
      cdc_ft::Log::VerbosityToLogLevel(verbosity)));

  // Perform up-to-date check.
  absl::StatusOr<bool> is_up_to_date = cdc_ft::IsUpToDate(components);
  if (!is_up_to_date.ok()) {
    LOG_ERROR("Failed to check file system freshness: %s",
              is_up_to_date.status().ToString());
    return static_cast<int>(is_up_to_date.status().code());
  }
  if (!*is_up_to_date) {
    printf("%s\n", cdc_ft::kFuseNotUpToDate);
    return 0;
  }
  printf("%s\n", cdc_ft::kFuseUpToDate);
  fflush(stdout);

  // Create fs. The rest of the flags are mount flags, so pass them along.
  absl::Status status = cdc_ft::cdc_fuse_fs::Initialize(
      static_cast<int>(mount_args.size()), mount_args.data());
  if (!status.ok()) {
    LOG_ERROR("Failed to initialize file system: %s", status.ToString());
    return static_cast<int>(status.code());
  }

  // Create disk data store.
  absl::StatusOr<std::unique_ptr<cdc_ft::DiskDataStore>> store =
      cdc_ft::DiskDataStore::Create(cache_dir_levels, cache_dir, false);
  if (!store.ok()) {
    LOG_ERROR("Failed to initialize the chunk cache in directory '%s': %s",
              absl::GetFlag(FLAGS_cache_dir), store.status().ToString());
    return 1;
  }
  LOG_INFO("Setting cache capacity to '%u'", cache_capacity);
  store.value()->SetCapacity(cache_capacity);
  LOG_INFO("Caching chunks in '%s'", store.value()->RootDir());

  // Start a gRpc client.
  std::string client_address = absl::StrFormat("localhost:%u", port);
  grpc::ChannelArguments channel_args;
  channel_args.SetMaxReceiveMessageSize(-1);
  std::shared_ptr<grpc::Channel> grpc_channel = grpc::CreateCustomChannel(
      client_address, grpc::InsecureChannelCredentials(), channel_args);
  std::vector<std::unique_ptr<cdc_ft::DataStoreReader>> readers;
  readers.emplace_back(
      std::make_unique<cdc_ft::GrpcReader>(grpc_channel, stats));
  cdc_ft::GrpcReader* grpc_reader =
      static_cast<cdc_ft::GrpcReader*>(readers[0].get());

  // Send all cached content ids to the client if statistics are enabled.
  if (stats) {
    LOG_INFO("Sending all cached content ids");
    absl::StatusOr<std::vector<cdc_ft::ContentIdProto>> ids =
        store.value()->List();
    if (!ids.ok()) {
      LOG_ERROR("Failed to get all cached content ids: %s",
                ids.status().ToString());
      return 1;
    }
    status = grpc_reader->SendCachedContentIds(*ids);
    if (!status.ok()) {
      LOG_ERROR("Failed to send all cached content ids: %s", status.ToString());
      return 1;
    }
  }

  // Create data provider.
  size_t prefetch_size = absl::GetFlag(FLAGS_prefetch_size).Size();
  cdc_ft::DataProvider data_provider(std::move(*store), std::move(readers),
                                     prefetch_size, dp_cleanup_timeout,
                                     dp_access_idle_timeout);

  if (!cdc_ft::cdc_fuse_fs::StartConfigClient(instance, grpc_channel).ok()) {
    LOG_ERROR("Could not start reading configuration updates'");
    return 1;
  }

  // Run FUSE.
  LOG_INFO("Running filesystem");
  status = cdc_ft::cdc_fuse_fs::Run(&data_provider, consistency_check);
  if (!status.ok()) {
    LOG_ERROR("Filesystem stopped with error: %s", status.ToString());
  }
  LOG_INFO("Filesystem ran successfully and shuts down");

  data_provider.Shutdown();
  cdc_ft::cdc_fuse_fs::Shutdown();
  cdc_ft::Log::Shutdown();

  static_assert(static_cast<int>(absl::StatusCode::kOk) == 0, "kOk != 0");
  return static_cast<int>(status.code());
}
