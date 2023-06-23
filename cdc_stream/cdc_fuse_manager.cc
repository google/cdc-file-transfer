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

// Parses the port from the FUSE stdout when FUSE is up-to-date. In that case,
// the expected stdout is similar to "Port 12345 cdc_fuse_fs is up-to-date".
absl::StatusOr<int> ParsePort(const std::string& fuse_stdout) {
  // Search backwards until we find "Port ".
  size_t port_pos = fuse_stdout.find(kFusePortPrefix);
  if (port_pos == std::string::npos) {
    return MakeStatus("Failed to find '%s' marker in server output '%s'",
                      kFusePortPrefix, fuse_stdout);
  }
  int port =
      atoi(fuse_stdout.substr(port_pos + strlen(kFusePortPrefix)).c_str());
  if (port == 0) {
    return MakeStatus("Failed to parse port from server output '%s'",
                      fuse_stdout);
  }
  return port;
}

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
                                   uint16_t local_port, int verbosity,
                                   bool debug, bool singlethreaded,
                                   bool enable_stats, bool check,
                                   uint64_t cache_capacity,
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
      "--components=%s --cache_dir=%s "
      "--verbosity=%i --cleanup_timeout=%i --access_idle_timeout=%i --stats=%i "
      "--check=%i --cache_capacity=%u -- -o allow_root -o ro -o nonempty -o "
      "auto_unmount %s%s%s",
      kRemoteToolsBinDir, remotePath, RemoteUtil::QuoteForSsh(instance_),
      RemoteUtil::QuoteForSsh(component_args), kCacheDir, verbosity,
      cleanup_timeout_sec, access_idle_timeout_sec, enable_stats, check,
      cache_capacity, debug ? "-d " : "", singlethreaded ? "-s " : "",
      RemoteUtil::QuoteForSsh(mount_dir));

  bool needs_deploy = false;
  int remote_port;
  ASSIGN_OR_RETURN(remote_port, RunFuseProcess(remote_command, &needs_deploy));
  if (needs_deploy) {
    // Deploy and try again.
    RETURN_IF_ERROR(Deploy());
    ASSIGN_OR_RETURN(remote_port,
                     RunFuseProcess(remote_command, &needs_deploy));
  }

  // Start port forwarding.
  RETURN_IF_ERROR(RunPortForwardingProcess(local_port, remote_port));

  // Wait until port forwarding is up and FUSE can connect to |remote_port|.
  RETURN_IF_ERROR(WaitForFuseConnected());

  return absl::OkStatus();
}

absl::StatusOr<int> CdcFuseManager::RunFuseProcess(
    const std::string& remote_command, bool* needs_deploy) {
  assert(!fuse_process_);
  assert(needs_deploy);
  *needs_deploy = false;

  LOG_DEBUG("Running FUSE process");
  ProcessStartInfo start_info = remote_util_->BuildProcessStartInfoForSsh(
      remote_command, ArchType::kLinux_x86_64);
  start_info.name = kFuseFilename;

  // Capture stdout to determine whether a deploy is required.
  fuse_stdout_.clear();
  fuse_port_ = 0;
  fuse_not_up_to_date_ = false;
  fuse_update_check_finished_ = false;
  fuse_connected_ = false;
  start_info.stdout_handler = [this](const char* data, size_t size) {
    return HandleFuseStdout(data, size);
  };
  fuse_process_ = process_factory_->Create(start_info);
  RETURN_IF_ERROR(fuse_process_->Start(), "Failed to start FUSE process");
  LOG_DEBUG("FUSE process started. Waiting for startup to finish.");

  // Run until process exits or startup finishes.
  RETURN_IF_ERROR(fuse_process_->RunUntil(
                      [this]() { return fuse_update_check_finished_.load(); }),
                  "Failed to run FUSE process");
  LOG_DEBUG("FUSE process update check complete.");

  // If the FUSE process exited before it could perform its up-to-date check, it
  // most likely happens because the binary does not exist and needs to be
  // deployed.
  *needs_deploy = fuse_not_up_to_date_ ||
                  (!fuse_update_check_finished_ && fuse_process_->HasExited() &&
                   fuse_process_->ExitCode() != 0);
  if (*needs_deploy) {
    LOG_DEBUG("FUSE needs to be (re-)deployed.");
    fuse_process_.reset();
  }

  return fuse_port_;
}

absl::Status CdcFuseManager::RunPortForwardingProcess(int local_port,
                                                      int remote_port) {
  assert(fuse_process_);
  assert(!forwarding_process_);

  LOG_DEBUG(
      "Running reverse port forwarding process, local port %i, remote port %i",
      local_port, remote_port);
  ProcessStartInfo start_info =
      remote_util_->BuildProcessStartInfoForSshPortForward(
          local_port, remote_port, /*reverse=*/true);
  forwarding_process_ = process_factory_->Create(start_info);
  RETURN_IF_ERROR(forwarding_process_->Start(),
                  "Failed to start port forwarding process");

  return absl::OkStatus();
}

absl::Status CdcFuseManager::WaitForFuseConnected() {
  assert(fuse_process_);
  assert(forwarding_process_);

  RETURN_IF_ERROR(
      fuse_process_->RunUntil([this]() { return fuse_connected_.load(); }),
      "Failed to run FUSE process");
  LOG_DEBUG("FUSE process connected.");

  if (!fuse_connected_ && fuse_process_->HasExited()) {
    return MakeStatus("FUSE exited during startup with code %u",
                      fuse_process_->ExitCode());
  }

  return absl::OkStatus();
}

absl::Status CdcFuseManager::Stop() {
  if (!fuse_process_) {
    return absl::OkStatus();
  }

  LOG_DEBUG("Terminating FUSE and port forwarding processes");
  absl::Status status = fuse_process_->Terminate();
  status.Update(forwarding_process_->Terminate());
  fuse_process_.reset();
  forwarding_process_.reset();
  return status;
}

bool CdcFuseManager::IsHealthy() const {
  return fuse_process_ && !fuse_process_->HasExited() && forwarding_process_ &&
         !forwarding_process_->HasExited();
}

absl::Status CdcFuseManager::HandleFuseStdout(const char* data, size_t size) {
  // Don't capture stdout beyond startup.
  if (!fuse_connected_) {
    fuse_stdout_.append(data, size);

    // The remote component prints some magic strings to stdout to indicate
    // whether it's up-to-date.
    if (absl::StrContains(fuse_stdout_, kFuseUpToDate)) {
      ASSIGN_OR_RETURN(fuse_port_, ParsePort(fuse_stdout_));
      fuse_update_check_finished_ = true;
    } else if (absl::StrContains(fuse_stdout_, kFuseNotUpToDate)) {
      fuse_not_up_to_date_ = true;
      fuse_update_check_finished_ = true;
    }

    // It also prints stuff when it can connect to its port.
    if (absl::StrContains(fuse_stdout_, kFuseConnected)) {
      fuse_connected_ = true;
    }
  }

  if (!remote_util_->Quiet()) {
    // Forward to logging.
    return LogOutput(kFuseStdoutPrefix, data, size);
  }
  return absl::OkStatus();
}

}  // namespace cdc_ft
