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

#ifndef CDC_STREAM_CDC_FUSE_MANAGER_H_
#define CDC_STREAM_CDC_FUSE_MANAGER_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/remote_util.h"

namespace cdc_ft {

class Process;
class ProcessFactory;
class RemoteUtil;

// Manages the remote CDC FUSE filesystem process.
class CdcFuseManager {
 public:
  CdcFuseManager(std::string instance, ProcessFactory* process_factory,
                 RemoteUtil* remote_util);
  ~CdcFuseManager();

  CdcFuseManager(CdcFuseManager&) = delete;
  CdcFuseManager& operator=(CdcFuseManager&) = delete;

  // Starts the remote CDC FUSE process. Deploys the binary if necessary.
  //
  // |mount_dir| is the remote directory where to mount the FUSE.
  // |local_port| is the local port used for gRPC connections to the FUSE.
  // |verbosity| is the log verbosity used by the filesystem.
  // |debug| puts the filesystem into debug mode if set to true. This also
  // causes the process to run in the foreground, so that logs are piped through
  // SSH to stdout of the workstation process.
  // |singlethreaded| puts the filesystem into single-threaded mode if true.
  // |enable_stats| determines whether FUSE should send debug statistics.
  // |check| determines whether to execute FUSE consistency check.
  // |cache_capacity| defines the cache capacity in bytes.
  // |cleanup_timeout_sec| defines the data provider cleanup timeout in seconds.
  // |access_idle_timeout_sec| defines the number of seconds after which data
  // provider is considered to be access-idling.
  absl::Status Start(const std::string& mount_dir, uint16_t local_port,
                     int verbosity, bool debug, bool singlethreaded,
                     bool enable_stats, bool check, uint64_t cache_capacity,
                     uint32_t cleanup_timeout_sec,
                     uint32_t access_idle_timeout_sec);

  // Stops the CDC FUSE.
  absl::Status Stop();

  // Returns true if the FUSE process is running.
  bool IsHealthy() const;

 private:
  // Runs the remote FUSE process from the given |remote_command|. Returns the
  // remote port that the FUSE will connect to, once the port forwarding process
  // is up.
  //
  // If the FUSE is not up-to-date or does not exist, sets |needs_deploy| to
  // true and returns OK. In that case, Deploy() needs to be called and the FUSE
  // process should be run again.
  absl::StatusOr<int> RunFuseProcess(const std::string& remote_command,
                                     bool* needs_deploy);

  // Establishes a reverse SSH tunnel from |remote_port| to |local_port|.
  absl::Status RunPortForwardingProcess(int local_port, int remote_port);

  // Waits until FUSE can connect to its port. This essentially means that
  // port forwarding is up and running.
  absl::Status WaitForFuseConnected();

  // Deploys the remote components.
  absl::Status Deploy();

  // Output handler for FUSE's stdout.
  // Sets |fuse_not_up_to_date_| to true if the output contains a magic marker
  // to indicate that the binary has to be redeployed.
  // Sets |fuse_port_| to the remote gRPC port if the output contains a magic
  // marker that has the port and indicates that the binary is up-to-date.
  // Sets |fuse_update_check_finished_| to true if any of the above two markers
  // was set. Called in a background thread.
  // Sets |fuse_startup_finished_| to true if FUSE is connected to its port.
  absl::Status HandleFuseStdout(const char* data, size_t size);

  std::string instance_;
  ProcessFactory* const process_factory_;
  RemoteUtil* const remote_util_;

  std::unique_ptr<Process> fuse_process_;
  std::unique_ptr<Process> forwarding_process_;
  std::string fuse_stdout_;

  // Set by HandleFuseStdout
  int fuse_port_ = 0;
  bool fuse_not_up_to_date_ = false;
  std::atomic_bool fuse_update_check_finished_{false};
  std::atomic_bool fuse_connected_{false};
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_CDC_FUSE_MANAGER_H_
