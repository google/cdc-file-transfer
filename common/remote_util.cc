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

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "common/path.h"
#include "common/status.h"

namespace cdc_ft {
namespace {

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

void RemoteUtil::SetUserHostAndPort(std::string user_host, int port) {
  user_host_ = std::move(user_host);
  ssh_port_ = port;
}

void RemoteUtil::SetScpCommand(std::string scp_command) {
  scp_command_ = std::move(scp_command);
}

void RemoteUtil::SetSshCommand(std::string ssh_command) {
  ssh_command_ = std::move(ssh_command);
}

absl::Status RemoteUtil::Scp(std::vector<std::string> source_filepaths,
                             const std::string& dest, bool compress) {
  absl::Status status = CheckUserHostPort();
  if (!status.ok()) {
    return status;
  }

  std::string source_args;
  for (const std::string& sourceFilePath : source_filepaths) {
    // Workaround for scp thinking that C is a host in C:\path\to\foo.
    if (absl::StrContains(path::GetDrivePrefix(sourceFilePath), ":")) {
      source_args += QuoteForWindows("//./" + sourceFilePath) + " ";
    } else {
      source_args += QuoteForWindows(sourceFilePath) + " ";
    }
  }

  // -p preserves timestamps. This enables timestamp-based up-to-date checks.
  ProcessStartInfo start_info;
  start_info.flags = ProcessFlags::kNoWindow;
  start_info.command = absl::StrFormat(
      "%s "
      "%s %s -p -T "
      "-P %i %s "
      "%s:%s",
      QuoteForWindows(scp_command_), quiet_ || verbosity_ < 2 ? "-q" : "",
      compress ? "-C" : "", ssh_port_, source_args, QuoteForWindows(user_host_),
      QuoteForWindows(dest));
  start_info.name = "scp";
  start_info.forward_output_to_log = forward_output_to_log_;

  return process_factory_->Run(start_info);
}

absl::Status RemoteUtil::Chmod(const std::string& mode,
                               const std::string& remote_path, bool quiet) {
  std::string remote_command =
      absl::StrFormat("chmod %s %s %s", QuoteForSsh(mode),
                      QuoteForSsh(remote_path), quiet ? "-f" : "");

  return Run(remote_command, "chmod");
}

absl::Status RemoteUtil::Run(std::string remote_command, std::string name) {
  absl::Status status = CheckUserHostPort();
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
  // Usually, one would pass in -N here, but this makes the connection terribly
  // slow! As a workaround, don't use -N (will open a shell), but simply eat the
  // output.
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
      QuoteForWindows(ssh_command_), quiet_ || verbosity_ < 2 ? "-q" : "",
      forward_arg, QuoteForWindows(user_host_), ssh_port_, remote_command_arg);
  start_info.forward_output_to_log = forward_output_to_log_;
  start_info.flags = ProcessFlags::kNoWindow;
  return start_info;
}

std::string RemoteUtil::QuoteForWindows(const std::string& argument) {
  // Escape certain backslashes (see doc of this function).
  std::string escaped =
      std::regex_replace(argument, std::regex(R"(\\*(?="|$))"), "$&$&");
  // Escape " -> \".
  escaped = std::regex_replace(escaped, std::regex(R"(")"), R"(\")");
  // Quote.
  return absl::StrCat("\"", escaped, "\"");
}

std::string RemoteUtil::QuoteForSsh(const std::string& argument) {
  // Escape \ ->: \\.
  std::string escaped =
      std::regex_replace(argument, std::regex(R"(\\)"), R"(\\)");
  // Escape " -> \".
  escaped = std::regex_replace(escaped, std::regex(R"(")"), R"(\")");

  // Quote, but handle special case for ~.
  if (escaped.empty() || escaped[0] != '~') {
    return QuoteForWindows(absl::StrCat("\"", escaped, "\""));
  }

  // Simple special cases. Quote() isn't required, but called for consistency.
  if (escaped == "~" || escaped == "~/") {
    return QuoteForWindows(escaped);
  }

  // Check whether the username contains only valid characters.
  // E.g. ~user name/foo -> Quote(~user name/foo)
  size_t slash_pos = escaped.find('/');
  size_t username_end_pos =
      slash_pos == std::string::npos ? escaped.size() : slash_pos;
  if (username_end_pos > 1 &&
      !std::regex_match(escaped.substr(1, username_end_pos - 1),
                        std::regex("^[a-z][-a-z0-9]*"))) {
    return QuoteForWindows(absl::StrCat("\"", escaped, "\""));
  }

  if (slash_pos == std::string::npos) {
    // E.g. ~username -> Quote(~username)
    return QuoteForWindows(escaped);
  }

  // E.g.  or ~username/foo -> Quote(~username/"foo")
  return QuoteForWindows(absl::StrCat(escaped.substr(0, slash_pos + 1), "\"",
                                      escaped.substr(slash_pos + 1), "\""));
}

absl::Status RemoteUtil::CheckUserHostPort() {
  if (user_host_.empty() || ssh_port_ == 0) {
    return MakeStatus("IP or port not set");
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
