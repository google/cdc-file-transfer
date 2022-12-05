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

namespace cdc_ft {

// Utilities for executing remote commands on a gamelet through SSH.
// Windows-only.
class RemoteUtil {
 public:
  // |user_host| is the SSH [user@]host of the remote instance.
  // If |verbosity| is > 0 and |quiet| is false, output from scp, ssh etc.
  // commands is shown.
  // If |quiet| is true, scp, ssh etc. commands use quiet mode.
  // If |forward_output_to_log| is true, process output is forwarded to logging
  // instead of this process's stdout/stderr.
  RemoteUtil(std::string user_host, int verbosity, bool quiet,
             ProcessFactory* process_factory, bool forward_output_to_log);

  // Sets the SCP command binary path and additional arguments, e.g.
  //   C:\path\to\scp.exe -p 1234 -F <ssh_config> -i <key_file>
  //     -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""file"""
  // By default, searches scp.exe on the path environment variables.
  void SetScpCommand(std::string scp_command);

  // Sets the SSH command binary path and additional arguments, e.g.
  //   C:\path\to\ssh.exe -P 1234 -F <ssh_config> -i <key_file>
  //     -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""file"""
  // By default, searches ssh.exe on the path environment variables.
  void SetSshCommand(std::string ssh_command);

  // Copies |source_filepaths| to the remote folder |dest| on the gamelet using
  // scp. If |compress| is true, compressed upload is used.
  absl::Status Scp(std::vector<std::string> source_filepaths,
                   const std::string& dest, bool compress);

  // Calls 'chmod |mode| |remote_path|' on the gamelet.
  absl::Status Chmod(const std::string& mode, const std::string& remote_path,
                     bool quiet = false);

  // Runs |remote_command| on the gamelet. The command must be properly escaped.
  // |name| is the name of the command displayed in the logs.
  absl::Status Run(std::string remote_command, std::string name);

  // Builds an SSH command that executes |remote_command| on the gamelet.
  ProcessStartInfo BuildProcessStartInfoForSsh(std::string remote_command);

  // Builds an SSH command that runs SSH port forwarding to the gamelet, using
  // the given |local_port| and |remote_port|.
  // If |reverse| is true, sets up reverse port forwarding.
  ProcessStartInfo BuildProcessStartInfoForSshPortForward(int local_port,
                                                          int remote_port,
                                                          bool reverse);

  // Builds an SSH command that executes |remote_command| on the gamelet, using
  // port forwarding with given |local_port| and |remote_port|.
  // If |reverse| is true, sets up reverse port forwarding.
  ProcessStartInfo BuildProcessStartInfoForSshPortForwardAndCommand(
      int local_port, int remote_port, bool reverse,
      std::string remote_command);

  // Returns whether output is suppressed.
  bool Quiet() const { return quiet_; }

  // Quotes and escapes a command line argument following the convention
  // understood by the Microsoft command line parser.
  // Double quotes are backslash-escaped. One or more backslashes are backslash-
  // escaped if they are followed by a double quote, or if they occur at the end
  // of the string, e.g.
  // foo       -> "foo"
  // foo\bar   -> "foo\bar"
  // foo\      -> "foo\\"
  // foo\\"bar -> "foo\\\\\"bar".
  static std::string QuoteForWindows(const std::string& argument);

  // Quotes and escapes a command line arguments for use in SSH command. The
  // argument is first escaped and quoted for Linux using double quotes and then
  // it is escaped to be used by the Microsoft command line parser.
  // Properly supports path starting with ~ and ~username.
  // foo       -> "\"foo\""
  // foo\bar   -> "\"foo\bar\""
  // foo\      -> "\"foo\\\\\""
  // foo\"bar  -> "\"foo\\\\\\\"bar\"".
  // ~/foo     -> "~/\"foo\""
  // ~user/foo -> "~user/\"foo\""
  static std::string QuoteForSsh(const std::string& argument);

 private:
  // Common code for BuildProcessStartInfoForSsh*.
  ProcessStartInfo BuildProcessStartInfoForSshInternal(
      std::string forward_arg, std::string remote_command);

  const int verbosity_;
  const bool quiet_;
  ProcessFactory* const process_factory_;
  const bool forward_output_to_log_;

  std::string scp_command_ = "scp";
  std::string ssh_command_ = "ssh";
  std::string user_host_;
};

}  // namespace cdc_ft

#endif  // COMMON_REMOTE_UTIL_H_
