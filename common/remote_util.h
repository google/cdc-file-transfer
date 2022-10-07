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

#ifndef COMMON_REMOTE_UTIL_H_
#define COMMON_REMOTE_UTIL_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "common/process.h"
#include "common/sdk_util.h"

namespace cdc_ft {

// Utilities for executing remote commands on a gamelet through SSH.
// Windows-only.
class RemoteUtil {
 public:
  // If |verbosity| is > 0 and |quiet| is false, output from scp, ssh etc.
  // commands is shown.
  // If |quiet| is true, scp, ssh etc. commands use quiet mode.
  // If |forward_output_to_log| is true, process output is forwarded to logging
  // instead of this process's stdout/stderr.
  RemoteUtil(int verbosity, bool quiet, ProcessFactory* process_factory,
             bool forward_output_to_log);

  // Returns the initialization status. Should be OK unless in case of some rare
  // internal error. Should be checked before accessing any members.
  const absl::Status& GetInitStatus() const {
    return sdk_util_.GetInitStatus();
  }

  // Set IP of the remote instance and the ssh tunnel port.
  void SetIpAndPort(const std::string& gamelet_ip, int ssh_port);

  // Copies |source_filepaths| to the remote folder |dest| on the gamelet using
  // scp. Must call either InitSsh or SetGameletIp before calling this method.
  // If |compress| is true, compressed upload is used.
  absl::Status Scp(std::vector<std::string> source_filepaths,
                   const std::string& dest, bool compress);

  // Syncs |source_filepaths| to the remote folder |dest| on the gamelet using
  // cdc_rsync. Must call either InitSsh or SetGameletIp before calling this
  // method.
  absl::Status Sync(std::vector<std::string> source_filepaths,
                    const std::string& dest);

  // Calls 'chmod |mode| |remote_path|' on the gamelet.
  // Must call either InitSsh or SetGameletIp before calling this method.
  absl::Status Chmod(const std::string& mode, const std::string& remote_path,
                     bool quiet = false);

  // Calls 'rm [-f] |remote_path|' on the gamelet.
  // Must call either InitSsh or SetGameletIp before calling this method.
  absl::Status Rm(const std::string& remote_path, bool force);

  // Calls `mv |old_remote_path| |new_remote_path| on the gamelet.
  // Must call either InitSsh or SetGameletIp before calling this method.
  absl::Status Mv(const std::string& old_remote_path,
                  const std::string& new_remote_path);

  // Runs |remote_command| on the gamelet. The command must be properly escaped.
  // |name| is the name of the command displayed in the logs.
  // Must call either InitSsh or SetGameletIp before calling this method.
  absl::Status Run(std::string remote_command, std::string name);

  // Builds an ssh command that executes |remote_command| on the gamelet.
  ProcessStartInfo BuildProcessStartInfoForSsh(std::string remote_command);

  // Builds an ssh command that runs SSH port forwarding to the gamelet, using
  // the given |local_port| and |remote_port|.
  // If |reverse| is true, sets up reverse port forwarding.
  // Must call either InitSsh or SetGameletIp before calling this method.
  ProcessStartInfo BuildProcessStartInfoForSshPortForward(int local_port,
                                                          int remote_port,
                                                          bool reverse);

  // Builds an ssh command that executes |remote_command| on the gamelet, using
  // port forwarding with given |local_port| and |remote_port|.
  // If |reverse| is true, sets up reverse port forwarding.
  // Must call either InitSsh or SetGameletIp before calling this method.
  ProcessStartInfo BuildProcessStartInfoForSshPortForwardAndCommand(
      int local_port, int remote_port, bool reverse,
      std::string remote_command);

  // Returns whether output is suppressed.
  bool Quiet() const { return quiet_; }

 private:
  // Verifies that both |gamelet_ip_| and |ssh_port_| are set.
  absl::Status CheckIpPort();

  // Common code for BuildProcessStartInfoForSsh*.
  ProcessStartInfo BuildProcessStartInfoForSshInternal(
      std::string forward_arg, std::string remote_command);

  const int verbosity_;
  const bool quiet_;
  ProcessFactory* const process_factory_;
  const bool forward_output_to_log_;

  SdkUtil sdk_util_;
  std::string gamelet_ip_;
  int ssh_port_ = 0;
};

}  // namespace cdc_ft

#endif  // COMMON_REMOTE_UTIL_H_
