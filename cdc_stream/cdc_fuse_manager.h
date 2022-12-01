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
#include "common/remote_util.h"

namespace cdc_ft {

class Process;
class ProcessFactory;
class RemoteUtil;

// Manages the gamelet-side CDC FUSE filesystem process.
class CdcFuseManager {
 public:
  CdcFuseManager(std::string instance, ProcessFactory* process_factory,
                 RemoteUtil* remote_util);
  ~CdcFuseManager();

  CdcFuseManager(CdcFuseManager&) = delete;
  CdcFuseManager& operator=(CdcFuseManager&) = delete;

  // Starts the CDC FUSE and establishes a reverse SSH tunnel from the gamelet's
  // |remote_port| to the workstation's |local_port|. Deploys the binary if
  // necessary.
  //
  // |mount_dir| is the remote directory where to mount the FUSE.
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
                     uint16_t remote_port, int verbosity, bool debug,
                     bool singlethreaded, bool enable_stats, bool check,
                     uint64_t cache_capacity, uint32_t cleanup_timeout_sec,
                     uint32_t access_idle_timeout_sec);

  // Stops the CDC FUSE.
  absl::Status Stop();

  // Returns true if the FUSE process is running.
  bool IsHealthy() const;

 private:
  // Runs the FUSE process on the gamelet from the given |remote_command| and
  // establishes a reverse SSH tunnel from the gamelet's |remote_port| to the
  // workstation's |local_port|.
  //
  // If the FUSE is not up-to-date or does not exist, sets |needs_deploy| to
  // true and returns OK. In that case, Deploy() needs to be called and the FUSE
  // process should be run again.
  absl::Status RunFuseProcess(uint16_t local_port, uint16_t remote_port,
                              const std::string& remote_command,
                              bool* needs_deploy);

  // Deploys the gamelet components.
  absl::Status Deploy();

  // Output handler for FUSE's stdout. Sets |needs_deploy| to true if the output
  // contains a magic marker to indicate that the binary has to be redeployed.
  // Called in a background thread.
  absl::Status HandleFuseStdout(const char* data, size_t size,
                                bool* needs_deploy);

  std::string instance_;
  ProcessFactory* const process_factory_;
  RemoteUtil* const remote_util_;

  std::unique_ptr<Process> fuse_process_;
  std::string fuse_stdout_;
  std::atomic<bool> fuse_startup_finished_{false};
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_CDC_FUSE_MANAGER_H_
