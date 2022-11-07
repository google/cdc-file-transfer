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

#include "common/remote_util.h"

#include <regex>

#include "absl/strings/str_format.h"
#include "common/path.h"
#include "common/status.h"

namespace cdc_ft {
namespace {

// Escapes command line argument for the Microsoft command line parser in
// preparation for quoting. Double quotes are backslash-escaped. Literal
// backslashes are backslash-escaped if they are followed by a double quote, or
// if they are part of a sequence of backslashes that are followed by a double
// quote.
std::string EscapeForWindows(const std::string& argument) {
  std::string str =
      std::regex_replace(argument, std::regex(R"(\\*(?=""|$))"), "$1$1");
  return std::regex_replace(str, std::regex("\""), "\\\"");
}

// Quotes and escapes a command line argument following the convention
// understood by the Microsoft command line parser.
std::string QuoteArgument(const std::string& argument) {
  return absl::StrFormat("\"%s\"", EscapeForWindows(argument));
}

// Quotes and escapes a command line argument for usage in SSH.
std::string QuoteArgumentForSsh(const std::string& argument) {
  return absl::StrFormat(
    "'%s'", std::regex_replace(argument, std::regex("'"), "'\\''"));
}

// Quotes and escapes a command line arguments for use in ssh command. The
// argument is first escaped and quoted for Linux using single quotes and then
// it is escaped to be used by the Microsoft command line parser.
std::string QuoteAndEscapeArgumentForSsh(const std::string& argument) {
  return EscapeForWindows(QuoteArgumentForSsh(argument));
}

// Gets the argument for SSH (reverse) port forwarding, e.g. -L23:localhost:45.
std::string GetPortForwardingArg(int local_port, int remote_port,
                                 bool reverse) {
  if (reverse)
    return absl::StrFormat("-R%i:localhost:%i ", remote_port, local_port);
  return absl::StrFormat("-L%i:localhost:%i ", local_port, remote_port);
}

}  // namespace

RemoteUtil::RemoteUtil(int verbosity, bool quiet,
                       ProcessFactory* process_factory,
                       bool forward_output_to_log)
    : verbosity_(verbosity),
      quiet_(quiet),
      process_factory_(process_factory),
      forward_output_to_log_(forward_output_to_log) {}

void RemoteUtil::SetHostAndPort(std::string hostname, int port) {
  hostname_ = std::move(hostname);
  ssh_port_ = port;
}
void RemoteUtil::SetScpCommand(std::string scp_command) {
  scp_command_ = scp_command;
}

void RemoteUtil::SetSshCommand(std::string ssh_command) {
  ssh_command_ = ssh_command;
}

absl::Status RemoteUtil::Scp(std::vector<std::string> source_filepaths,
                             const std::string& dest, bool compress) {
  absl::Status status = CheckIpPort();
  if (!status.ok()) {
    return status;
  }

  std::string source_args;
  for (const std::string& sourceFilePath : source_filepaths) {
    // Workaround for scp thinking that C is a host in C:\path\to\foo.
    source_args += QuoteArgument("//./" + sourceFilePath) + " ";
  }

  // -p preserves timestamps. This enables timestamp-based up-to-date checks.
  ProcessStartInfo start_info;
  start_info.command = absl::StrFormat(
      "%s "
      "%s %s -p -T "
      "-P %i %s "
      "%s",
      scp_command_, quiet_ || verbosity_ < 2 ? "-q" : "", compress ? "-C" : "",
      ssh_port_, source_args, QuoteArgument(hostname_ + ":" + dest));
  start_info.name = "scp";
  start_info.forward_output_to_log = forward_output_to_log_;

  return process_factory_->Run(start_info);
}

absl::Status RemoteUtil::Sync(std::vector<std::string> source_filepaths,
                              const std::string& dest) {
  absl::Status status = CheckIpPort();
  if (!status.ok()) {
    return status;
  }

  std::string source_args;
  for (const std::string& sourceFilePath : source_filepaths) {
    source_args += QuoteArgument(sourceFilePath) + " ";
  }

  ProcessStartInfo start_info;
  start_info.command = absl::StrFormat(
      "cdc_rsync.exe --ip=%s --port=%i -z "
      "%s %s%s",
      QuoteArgument(hostname_), ssh_port_,
      quiet_ || verbosity_ < 2 ? "-q " : " ", source_args, QuoteArgument(dest));
  start_info.name = "cdc_rsync";
  start_info.forward_output_to_log = forward_output_to_log_;

  return process_factory_->Run(start_info);
}

absl::Status RemoteUtil::Chmod(const std::string& mode,
                               const std::string& remote_path, bool quiet) {
  std::string remote_command = absl::StrFormat(
      "chmod %s %s %s", QuoteArgument(mode),
    EscapeForWindows(remote_path), quiet ? "-f" : "");

  return Run(remote_command, "chmod");
}

absl::Status RemoteUtil::Rm(const std::string& remote_path, bool force) {
  std::string remote_command = absl::StrFormat(
      "rm %s %s", force ? "-f" : "", EscapeForWindows(remote_path));

  return Run(remote_command, "rm");
}

absl::Status RemoteUtil::Mv(const std::string& old_remote_path,
                            const std::string& new_remote_path) {
  std::string remote_command =
      absl::StrFormat("mv %s %s", EscapeForWindows(old_remote_path),
        EscapeForWindows(new_remote_path));

  return Run(remote_command, "mv");
}

absl::Status RemoteUtil::Run(std::string remote_command, std::string name) {
  absl::Status status = CheckIpPort();
  if (!status.ok()) {
    return status;
  }

  ProcessStartInfo start_info =
      BuildProcessStartInfoForSsh(std::move(remote_command));
  start_info.name = std::move(name);
  start_info.forward_output_to_log = forward_output_to_log_;

  return process_factory_->Run(start_info);
}

ProcessStartInfo RemoteUtil::BuildProcessStartInfoForSsh(
    std::string remote_command) {
  return BuildProcessStartInfoForSshInternal("", "-- " + remote_command);
}

ProcessStartInfo RemoteUtil::BuildProcessStartInfoForSshPortForward(
    int local_port, int remote_port, bool reverse) {
  // (internal): Usually, one would pass in -N here, but this makes the
  // connection terribly slow! As a workaround, don't use -N (will open a
  // shell), but simply eat the output.
  ProcessStartInfo si = BuildProcessStartInfoForSshInternal(
      GetPortForwardingArg(local_port, remote_port, reverse) + "-n ", "");
  si.stdout_handler = [](const void*, size_t) { return absl::OkStatus(); };
  return si;
}

ProcessStartInfo RemoteUtil::BuildProcessStartInfoForSshPortForwardAndCommand(
    int local_port, int remote_port, bool reverse, std::string remote_command) {
  return BuildProcessStartInfoForSshInternal(
      GetPortForwardingArg(local_port, remote_port, reverse),
      "-- " + remote_command);
}

ProcessStartInfo RemoteUtil::BuildProcessStartInfoForSshInternal(
    std::string forward_arg, std::string remote_command_arg) {
  ProcessStartInfo start_info;
  start_info.command = absl::StrFormat(
      "%s "
      "%s -tt "
      "-oServerAliveCountMax=6 "  // Number of lost msgs before ssh terminates
      "-oServerAliveInterval=5 "  // Time interval between alive msgs
      "%s %s -p %i %s",
      ssh_command_, quiet_ || verbosity_ < 2 ? "-q" : "", forward_arg,
      QuoteArgument(hostname_), ssh_port_, remote_command_arg);
  start_info.forward_output_to_log = forward_output_to_log_;
  return start_info;
}

absl::Status RemoteUtil::CheckIpPort() {
  if (hostname_.empty() || ssh_port_ == 0) {
    return MakeStatus("IP or port not set");
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
