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
#include "common/arch_type.h"
#include "common/process.h"

namespace cdc_ft {

// Utilities for executing remote commands on a remote device through SSH.
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
  //   C:\path\to\scp.exe -P 1234 -i <key_file> -oUserKnownHostsFile=known_hosts
  // By default, searches scp on the path environment variables.
  void SetScpCommand(std::string scp_command);

  // Sets the SFTP command binary path and additional arguments, e.g.
  //   C:\path\to\sftp.exe -P 1234 -i <key_file>
  //   -oUserKnownHostsFile=known_hosts
  // By default, searches sftp on the path environment variables.
  void SetSftpCommand(std::string sftp_command);

  // Sets the SSH command binary path and additional arguments, e.g.
  //   C:\path\to\ssh.exe -p 1234 -i <key_file> -oUserKnownHostsFile=known_hosts
  // By default, searches ssh on the path environment variables.
  void SetSshCommand(std::string ssh_command);

  // Converts an scp command into an sftp command by simply replacing the first
  // occurrance of "scp.", "scp " or "scp\0" by sftp (case insensitive). This
  // adds backwards compatibility after a switch from scp to sftp in case users
  // still set CDC_SCP_COMMAND or --scp-command. Luckily, all relevant
  // parameters of sftp and scp match.
  // Returns an empty string if |scp_command| does not contain "scp".
  // Returns bad results for tricky strings like "C:\scp.path\scp.exe".
  static std::string ScpToSftpCommand(std::string scp_command);

  // Copies |source_filepaths| to the remote folder |dest| on the remove device
  // using scp. If |compress| is true, compressed upload is used.
  absl::Status Scp(std::vector<std::string> source_filepaths,
                   const std::string& dest, bool compress);

  // Creates an sftp connection to the remote instance and executes the
  // newline-separated SFTP |commands|. See
  //   https://man7.org/linux/man-pages/man1/sftp.1.html
  // for a list of available commands.
  // |initial_local_dir| sets the initial local directory in sftp. This is
  // useful since some sftp clients don't work with standard Windows paths and
  // require for instance /cygdrive paths.
  // If |compress| is true, compressed upload is used.
  // Example: Create nested directories and copying an executable file.
  //   -mkdir a
  //   cd a
  //   -mkdir b
  //   cd b
  //   put foo_executable
  //   chmod 755 foo_executable
  absl::Status Sftp(const std::string& commands,
                    const std::string& initial_local_dir, bool compress);

  // Calls 'chmod |mode| |remote_path|' on the remote device.
  absl::Status Chmod(const std::string& mode, const std::string& remote_path,
                     bool quiet = false);

  // Runs |remote_command| on the remote device. The command must be properly
  // escaped. |name| is the name of the command displayed in the logs.
  absl::Status Run(std::string remote_command, std::string name,
                   ArchType arch_type);

  // Same as Run(), but captures both stdout and stderr.
  // If |std_out| or |std_err| are nullptr, the output is not captured.
  absl::Status RunWithCapture(std::string remote_command, std::string name,
                              std::string* std_out, std::string* std_err,
                              ArchType arch_type);

  // Builds an SSH command that executes |remote_command| on the remote device.
  ProcessStartInfo BuildProcessStartInfoForSsh(std::string remote_command,
                                               ArchType arch_type);

  // Builds an SSH command that runs SSH port forwarding to the remote device,
  // using the given |local_port| and |remote_port|. If |reverse| is true, sets
  // up reverse port forwarding.
  ProcessStartInfo BuildProcessStartInfoForSshPortForward(int local_port,
                                                          int remote_port,
                                                          bool reverse);

  // Builds an SSH command that executes |remote_command| on the remote device,
  // using port forwarding with given |local_port| and |remote_port|. If
  // |reverse| is true, sets up reverse port forwarding.
  ProcessStartInfo BuildProcessStartInfoForSshPortForwardAndCommand(
      int local_port, int remote_port, bool reverse, std::string remote_command,
      ArchType arch_type);

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
      std::string forward_arg, std::string remote_command, ArchType arch_type);

  const int verbosity_;
  const bool quiet_;
  ProcessFactory* const process_factory_;
  const bool forward_output_to_log_;

  std::string scp_command_ = "scp";
  std::string sftp_command_ = "sftp";
  std::string ssh_command_ = "ssh";
  std::string user_host_;
};

}  // namespace cdc_ft

#endif  // COMMON_REMOTE_UTIL_H_
