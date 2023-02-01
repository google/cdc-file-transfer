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

#include "cdc_stream/session.h"

#include "cdc_stream/cdc_fuse_manager.h"
#include "common/log.h"
#include "common/port_manager.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "metrics/enums.h"
#include "metrics/messages.h"

namespace cdc_ft {
namespace {

// Timeout for initial gamelet connection.
constexpr double kInstanceConnectionTimeoutSec = 60.0f;

metrics::DeveloperLogEvent GetEventWithHeartBeatData(size_t bytes,
                                                     size_t chunks) {
  metrics::DeveloperLogEvent evt;
  evt.as_manager_data = std::make_unique<metrics::AssetStreamingManagerData>();
  evt.as_manager_data->session_data = std::make_unique<metrics::SessionData>();
  evt.as_manager_data->session_data->byte_count = bytes;
  evt.as_manager_data->session_data->chunk_count = chunks;
  return std::move(evt);
}

}  // namespace

Session::Session(std::string instance_id, const SessionTarget& target,
                 SessionConfig cfg, ProcessFactory* process_factory,
                 std::unique_ptr<SessionMetricsRecorder> metrics_recorder)
    : instance_id_(std::move(instance_id)),
      mount_dir_(target.mount_dir),
      cfg_(std::move(cfg)),
      process_factory_(process_factory),
      remote_util_(target.user_host, cfg_.verbosity, cfg_.quiet,
                   process_factory,
                   /*forward_output_to_logging=*/true),
      metrics_recorder_(std::move(metrics_recorder)) {
  assert(metrics_recorder_);
  if (!target.ssh_command.empty()) {
    remote_util_.SetSshCommand(target.ssh_command);
  }
  if (!target.sftp_command.empty()) {
    remote_util_.SetSftpCommand(target.sftp_command);
  }
}

Session::~Session() {
  absl::Status status = Stop();
  if (!status.ok()) {
    LOG_ERROR("Failed to stop session for instance '%s': %s", instance_id_,
              status.ToString());
  }
}

absl::Status Session::Start(int local_port, int first_remote_port,
                            int last_remote_port) {
  // Find an available remote port.
  int remote_port = first_remote_port;
  if (first_remote_port < last_remote_port) {
    std::unordered_set<int> ports;
    ASSIGN_OR_RETURN(
        ports,
        PortManager::FindAvailableRemotePorts(
            first_remote_port, last_remote_port, ArchType::kLinux_x86_64,
            process_factory_, &remote_util_, kInstanceConnectionTimeoutSec),
        "Failed to find an available remote port in the range [%d, %d]",
        first_remote_port, last_remote_port);
    assert(!ports.empty());
    remote_port = *ports.begin();
  }

  assert(!fuse_);
  fuse_ = std::make_unique<CdcFuseManager>(instance_id_, process_factory_,
                                           &remote_util_);
  RETURN_IF_ERROR(
      fuse_->Start(mount_dir_, local_port, remote_port, cfg_.verbosity,
                   cfg_.fuse_debug, cfg_.fuse_singlethreaded, cfg_.stats,
                   cfg_.fuse_check, cfg_.fuse_cache_capacity,
                   cfg_.fuse_cleanup_timeout_sec,
                   cfg_.fuse_access_idle_timeout_sec),
      "Failed to start instance component");
  return absl::OkStatus();
}

absl::Status Session::Stop() {
  absl::ReaderMutexLock lock(&transferred_data_mu_);
  // TODO: Record error on session end.
  metrics_recorder_->RecordEvent(
      GetEventWithHeartBeatData(transferred_bytes_, transferred_chunks_),
      metrics::EventType::kSessionEnd);
  if (fuse_) {
    RETURN_IF_ERROR(fuse_->Stop());
    fuse_.reset();
  }

  return absl::OkStatus();
}

bool Session::IsHealthy() { return fuse_->IsHealthy(); }

void Session::RecordEvent(metrics::DeveloperLogEvent event,
                          metrics::EventType code) const {
  metrics_recorder_->RecordEvent(std::move(event), code);
}

void Session::OnContentSent(size_t bytes, size_t chunks) {
  absl::WriterMutexLock lock(&transferred_data_mu_);
  transferred_bytes_ += bytes;
  transferred_chunks_ += chunks;
}

void Session::RecordHeartBeatIfChanged() {
  absl::ReaderMutexLock lock(&transferred_data_mu_);
  if (transferred_bytes_ == last_read_bytes_ &&
      transferred_chunks_ == last_read_chunks_) {
    return;
  }
  last_read_bytes_ = transferred_bytes_;
  last_read_chunks_ = transferred_chunks_;
  metrics_recorder_->RecordEvent(
      GetEventWithHeartBeatData(last_read_bytes_, last_read_chunks_),
      metrics::EventType::kSessionHeartBeat);
}

}  // namespace cdc_ft
