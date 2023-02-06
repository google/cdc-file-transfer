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

#include "cdc_stream/cdc_fuse_manager.h"

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "cdc_fuse_fs/constants.h"
#include "common/gamelet_component.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status.h"
#include "common/status_macros.h"

namespace cdc_ft {
namespace {

constexpr char kExeFilename[] = "cdc_stream.exe";
constexpr char kFuseFilename[] = "cdc_fuse_fs";
constexpr char kLibFuseFilename[] = "libfuse.so";
constexpr char kFuseStdoutPrefix[] = "cdc_fuse_fs_stdout";
constexpr char kRemoteToolsBinDir[] = ".cache/cdc-file-transfer/bin/";

// Cache directory on the gamelet to store data chunks.
constexpr char kCacheDir[] = "~/.cache/cdc-file-transfer/chunks";

}  // namespace

CdcFuseManager::CdcFuseManager(std::string instance,
                               ProcessFactory* process_factory,
                               RemoteUtil* remote_util)
    : instance_(std::move(instance)),
      process_factory_(process_factory),
      remote_util_(remote_util) {}

CdcFuseManager::~CdcFuseManager() = default;

absl::Status CdcFuseManager::Deploy() {
  assert(!fuse_process_);

  LOG_INFO("Deploying FUSE...");

  std::string exe_dir;
  RETURN_IF_ERROR(path::GetExeDir(&exe_dir), "Failed to get exe directory");

  // Create the remote tools bin dir if it doesn't exist yet.
  // This assumes that sftp's remote startup directory is the home directory.
  std::vector<std::string> dir_parts =
      absl::StrSplit(kRemoteToolsBinDir, '/', absl::SkipEmpty());
  std::string sftp_commands;
  for (const std::string& dir : dir_parts) {
    // Use -mkdir to ignore errors if the directory already exists.
    sftp_commands += absl::StrFormat("-mkdir %s\ncd %s\n", dir, dir);
  }

  sftp_commands += absl::StrFormat("put %s\n", kFuseFilename);
  sftp_commands += absl::StrFormat("put %s\n", kLibFuseFilename);
  sftp_commands += absl::StrFormat("chmod 755 %s\n", kFuseFilename);

  LOG_DEBUG("Deploying FUSE");
  RETURN_IF_ERROR(
      remote_util_->Sftp(sftp_commands, exe_dir, /*compress=*/false),
      "Failed to deploy FUSE");
  LOG_DEBUG("Deploying FUSE succeeded");

  return absl::OkStatus();
}

absl::Status CdcFuseManager::Start(const std::string& mount_dir,
                                   uint16_t local_port, uint16_t remote_port,
                                   int verbosity, bool debug,
                                   bool singlethreaded, bool enable_stats,
                                   bool check, uint64_t cache_capacity,
                                   uint32_t cleanup_timeout_sec,
                                   uint32_t access_idle_timeout_sec) {
  assert(!fuse_process_);

  // Gather stats for the FUSE gamelet component to determine whether a
  // re-deploy is necessary.
  std::string exe_dir;
  RETURN_IF_ERROR(path::GetExeDir(&exe_dir), "Failed to get exe directory");
  std::vector<GameletComponent> components;
  absl::Status status =
      GameletComponent::Get({path::Join(exe_dir, kFuseFilename),
                             path::Join(exe_dir, kLibFuseFilename)},
                            &components);
  if (!status.ok()) {
    return absl::NotFoundError(absl::StrFormat(
        "Required gamelet component not found. Make sure the files %s and %s "
        "reside in the same folder as %s.",
        kFuseFilename, kLibFuseFilename, kExeFilename));
  }
  std::string component_args = GameletComponent::ToCommandLineArgs(components);

  // Build the remote command.
  std::string remotePath = path::JoinUnix(kRemoteToolsBinDir, kFuseFilename);
  std::string remote_command = absl::StrFormat(
      "LD_LIBRARY_PATH=%s %s "
      "--instance=%s "
      "--components=%s --port=%i --cache_dir=%s "
      "--verbosity=%i --cleanup_timeout=%i --access_idle_timeout=%i --stats=%i "
      "--check=%i --cache_capacity=%u -- -o allow_root -o ro -o nonempty -o "
      "auto_unmount %s%s%s",
      kRemoteToolsBinDir, remotePath, RemoteUtil::QuoteForSsh(instance_),
      RemoteUtil::QuoteForSsh(component_args), remote_port, kCacheDir,
      verbosity, cleanup_timeout_sec, access_idle_timeout_sec, enable_stats,
      check, cache_capacity, debug ? "-d " : "", singlethreaded ? "-s " : "",
      RemoteUtil::QuoteForSsh(mount_dir));

  bool needs_deploy = false;
  RETURN_IF_ERROR(
      RunFuseProcess(local_port, remote_port, remote_command, &needs_deploy));
  if (needs_deploy) {
    // Deploy and try again.
    RETURN_IF_ERROR(Deploy());
    RETURN_IF_ERROR(
        RunFuseProcess(local_port, remote_port, remote_command, &needs_deploy));
  }

  return absl::OkStatus();
}

absl::Status CdcFuseManager::RunFuseProcess(uint16_t local_port,
                                            uint16_t remote_port,
                                            const std::string& remote_command,
                                            bool* needs_deploy) {
  assert(!fuse_process_);
  assert(needs_deploy);
  *needs_deploy = false;

  LOG_DEBUG("Running FUSE process");
  ProcessStartInfo start_info =
      remote_util_->BuildProcessStartInfoForSshPortForwardAndCommand(
          local_port, remote_port, true, remote_command,
          ArchType::kLinux_x86_64);
  start_info.name = kFuseFilename;

  // Capture stdout to determine whether a deploy is required.
  fuse_stdout_.clear();
  fuse_startup_finished_ = false;
  start_info.stdout_handler = [this, needs_deploy](const char* data,
                                                   size_t size) {
    return HandleFuseStdout(data, size, needs_deploy);
  };
  fuse_process_ = process_factory_->Create(start_info);
  RETURN_IF_ERROR(fuse_process_->Start(), "Failed to start FUSE process");
  LOG_DEBUG("FUSE process started. Waiting for startup to finish.");

  // Run until process exits or startup finishes.
  auto startup_finished = [this]() { return fuse_startup_finished_.load(); };
  RETURN_IF_ERROR(fuse_process_->RunUntil(startup_finished),
                  "Failed to run FUSE process");
  LOG_DEBUG("FUSE process startup complete.");

  // If the FUSE process exited before it could perform its up-to-date check, it
  // most likely happens because the binary does not exist and needs to be
  // deployed.
  *needs_deploy |= !fuse_startup_finished_ && fuse_process_->HasExited() &&
                   fuse_process_->ExitCode() != 0;
  if (*needs_deploy) {
    LOG_DEBUG("FUSE needs to be (re-)deployed.");
    fuse_process_.reset();
    return absl::OkStatus();
  }

  return absl::OkStatus();
}

absl::Status CdcFuseManager::Stop() {
  if (!fuse_process_) {
    return absl::OkStatus();
  }

  LOG_DEBUG("Terminating FUSE process");
  absl::Status status = fuse_process_->Terminate();
  fuse_process_.reset();
  return status;
}

bool CdcFuseManager::IsHealthy() const {
  return fuse_process_ && !fuse_process_->HasExited();
}

absl::Status CdcFuseManager::HandleFuseStdout(const char* data, size_t size,
                                              bool* needs_deploy) {
  assert(needs_deploy);

  // Don't capture stdout beyond startup.
  if (!fuse_startup_finished_) {
    fuse_stdout_.append(data, size);
    // The gamelet component prints some magic strings to stdout to indicate
    // whether it's up-to-date.
    if (absl::StrContains(fuse_stdout_, kFuseUpToDate)) {
      fuse_startup_finished_ = true;
    } else if (absl::StrContains(fuse_stdout_, kFuseNotUpToDate)) {
      fuse_startup_finished_ = true;
      *needs_deploy = true;
    }
  }

  if (!remote_util_->Quiet()) {
    // Forward to logging.
    return LogOutput(kFuseStdoutPrefix, data, size);
  }
  return absl::OkStatus();
}

}  // namespace cdc_ft
