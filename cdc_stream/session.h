/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CDC_STREAM_SESSION_H_
#define CDC_STREAM_SESSION_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "absl/status/status.h"
#include "cdc_stream/metrics_recorder.h"
#include "cdc_stream/session_config.h"
#include "common/remote_util.h"

namespace cdc_ft {

class CdcFuseManager;
class ProcessFactory;
class Process;

// Defines a remote target and how to connect to it.
struct SessionTarget {
  // SSH username and hostname of the remote target, formed as [user@]host.
  std::string user_host;
  // Ssh command to use to connect to the remote target.
  std::string ssh_command;
  // Sftp command to use to copy files to the remote target.
  std::string sftp_command;
  // Directory on the remote target where to mount the streamed directory.
  std::string mount_dir;
};

// Manages the connection of a workstation to a single remote instance.
class Session {
 public:
  // |instance_id| is a unique id for the remote instance.
  // |target| identifies the remote target and how to connect to it.
  // |cfg| contains generic configuration parameters for the session.
  // |process_factory| abstracts process creation.
  Session(std::string instance_id, const SessionTarget& target,
          SessionConfig cfg, ProcessFactory* process_factory,
          std::unique_ptr<SessionMetricsRecorder> metrics_recorder);
  ~Session();

  // Starts the CDC FUSE on the instance with established port forwarding.
  // |local_port| is the local reverse forwarding port to use.
  // [|first_remote_port|, |last_remote_port|] are the allowed remote ports.
  absl::Status Start(int local_port, int first_remote_port,
                     int last_remote_port);

  // Shuts down the connection to the instance.
  absl::Status Stop() ABSL_LOCKS_EXCLUDED(transferred_data_mu_);

  // Returns true if the FUSE process is running.
  bool IsHealthy();

  // Record an event for the session.
  void RecordEvent(metrics::DeveloperLogEvent event,
                   metrics::EventType code) const;

  // Is called when content was sent during the session.
  void OnContentSent(size_t bytes, size_t chunks)
      ABSL_LOCKS_EXCLUDED(transferred_data_mu_);

  // Records heart beat data if it has changed since last record.
  void RecordHeartBeatIfChanged() ABSL_LOCKS_EXCLUDED(transferred_data_mu_);

 private:
  const std::string instance_id_;
  const std::string mount_dir_;
  const SessionConfig cfg_;
  ProcessFactory* const process_factory_;

  RemoteUtil remote_util_;
  std::unique_ptr<CdcFuseManager> fuse_;
  std::unique_ptr<SessionMetricsRecorder> metrics_recorder_;

  absl::Mutex transferred_data_mu_;
  uint64_t transferred_bytes_ ABSL_GUARDED_BY(transferred_data_mu_) = 0;
  uint64_t transferred_chunks_ ABSL_GUARDED_BY(transferred_data_mu_) = 0;
  uint64_t last_read_bytes_ = 0;
  uint64_t last_read_chunks_ = 0;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_SESSION_H_
