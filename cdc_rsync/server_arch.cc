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

#include "cdc_rsync/server_arch.h"

#include <filesystem>

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "common/path.h"
#include "common/remote_util.h"
#include "common/status_macros.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

constexpr char kErrorArchTypeUnhandled[] = "arch_type_unhandled";
constexpr char kUnsupportedArchErrorFmt[] =
    "Unsupported remote device architecture '%s'. If you think this is a "
    "bug, or if this combination should be supported, please file a bug at "
    "https://github.com/google/cdc-file-transfer.";

absl::StatusOr<ArchType> GetArchTypeFromUname(const std::string& uname_out) {
  // uname_out is "KERNEL MACHINE"
  // Possible values for KERNEL: Linux (not sure what else).
  // Possible values for MACHINE:
  // https://stackoverflow.com/questions/45125516/possible-values-for-uname-m
  // Relevant for us: x86_64, aarch64.
  if (absl::StartsWith(uname_out, "Linux ")) {
    // Linux kernel. Check CPU type.
    if (absl::StrContains(uname_out, "x86_64")) {
      return ArchType::kLinux_x86_64;
    }
  }

  if (absl::StartsWith(uname_out, "MSYS_")) {
    // Windows machine that happens to have Cygwin/MSYS on it. Check CPU type.
    if (absl::StrContains(uname_out, "x86_64")) {
      return ArchType::kWindows_x86_64;
    }
  }

  return absl::UnimplementedError(
      absl::StrFormat(kUnsupportedArchErrorFmt, uname_out));
}

absl::StatusOr<ArchType> GetArchTypeFromWinProcArch(
    const std::string& arch_out) {
  // Possible values: AMD64, IA64, ARM64, x86
  if (absl::StrContains(arch_out, "AMD64")) {
    return ArchType::kWindows_x86_64;
  }

  return absl::UnimplementedError(
      absl::StrFormat(kUnsupportedArchErrorFmt, arch_out));
}

}  // namespace

// static
ServerArch ServerArch::GuessFromDestination(const std::string& destination) {
  // Path starting with ~ or / -> Linux.
  if (absl::StartsWith(destination, "~") ||
      absl::StartsWith(destination, "/")) {
    LOG_DEBUG("Guessed server arch type Linux based on ~ or /");
    return ServerArch(ArchType::kLinux_x86_64, /*is_guess=*/true);
  }

  // Path starting with C: etc. -> Windows.
  if (!path::GetDrivePrefix(destination).empty()) {
    LOG_DEBUG("Guessed server arch type Windows based on drive prefix");
    return ServerArch(ArchType::kWindows_x86_64, /*is_guess=*/true);
  }

  // Path with only / -> Linux.
  if (absl::StrContains(destination, "/") &&
      !absl::StrContains(destination, "\\")) {
    LOG_DEBUG("Guessed server arch type Linux based on forward slashes");
    return ServerArch(ArchType::kLinux_x86_64, /*is_guess=*/true);
  }

  // Path with only \\ -> Windows.
  if (absl::StrContains(destination, "\\") &&
      !absl::StrContains(destination, "/")) {
    LOG_DEBUG("Guessed server arch type Windows based on backslashes");
    return ServerArch(ArchType::kWindows_x86_64, /*is_guess=*/true);
  }

  // Default to Linux.
  LOG_DEBUG("Guessed server arch type Linux as default");
  return ServerArch(ArchType::kLinux_x86_64, /*is_guess=*/true);
}

// static
ServerArch ServerArch::DetectFromLocalDevice() {
  LOG_DEBUG("Detected local device type %s",
            GetArchTypeStr(GetLocalArchType()));
  return ServerArch(GetLocalArchType(), /*is_guess=*/false);
}

// static
absl::StatusOr<ServerArch> ServerArch::DetectFromRemoteDevice(
    RemoteUtil* remote_util) {
  assert(remote_util);

  // Run uname, assuming it's a Linux machine.
  std::string uname_out;
  std::string linux_cmd = "uname -sm";
  absl::Status status =
      remote_util->RunWithCapture(linux_cmd, "uname", &uname_out, nullptr);
  if (status.ok()) {
    LOG_DEBUG("Uname returned '%s'", uname_out);
    absl::StatusOr<ArchType> type = GetArchTypeFromUname(uname_out);
    if (type.ok()) {
      LOG_DEBUG("Detected server arch type %s from uname",
                GetArchTypeStr(*type));
      return ServerArch(*type, /*is_guess=*/false);
    }
    status = type.status();
  }
  LOG_DEBUG("Failed to detect arch type from uname: %s", status.ToString());

  // Run echo %PROCESSOR_ARCHITECTURE%, assuming it's a Windows machine.
  // Note: That space after PROCESSOR_ARCHITECTURE is important or else Windows
  //       command magic interprets quotes as part of the string.
  std::string arch_out;
  std::string windows_cmd =
      RemoteUtil::QuoteForSsh("cmd /C set PROCESSOR_ARCHITECTURE ");
  status = remote_util->RunWithCapture(windows_cmd,
                                       "set PROCESSOR_ARCHITECTURE", &arch_out,
                                       nullptr, ArchType::kWindows_x86_64);
  if (status.ok()) {
    LOG_DEBUG("%PROCESSOR_ARCHITECTURE% is '%s'", arch_out);
    absl::StatusOr<ArchType> type = GetArchTypeFromWinProcArch(arch_out);
    if (type.ok()) {
      LOG_DEBUG("Detected server arch type %s from %%PROCESSOR_ARCHITECTURE%%",
                GetArchTypeStr(*type));
      return ServerArch(*type, /*is_guess=*/false);
    }
    status = type.status();
  }
  LOG_DEBUG("Failed to detect arch type from %%PROCESSOR_ARCHITECTURE%%: %s",
            status.ToString());

  return absl::InternalError("Failed to detect remote architecture");
}

// static
std::string ServerArch::CdcRsyncFilename() {
  switch (GetLocalArchType()) {
    case ArchType::kWindows_x86_64:
      return "cdc_rsync.exe";
    case ArchType::kLinux_x86_64:
      return "cdc_rsync";
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

ServerArch::ServerArch(ArchType type, bool is_guess)
    : type_(type), is_guess_(is_guess) {}

ServerArch::~ServerArch() {}

const char* ServerArch::GetTypeStr() const { return GetArchTypeStr(type_); }

std::string ServerArch::CdcServerFilename() const {
  switch (type_) {
    case ArchType::kWindows_x86_64:
      return "cdc_rsync_server.exe";
    case ArchType::kLinux_x86_64:
      return "cdc_rsync_server";
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

std::string ServerArch::RemoteToolsBinDir() const {
  if (IsWindowsArchType(type_)) {
    return "AppData\\Roaming\\cdc-file-transfer\\bin\\";
  }

  if (IsLinuxArchType(type_)) {
    return ".cache/cdc-file-transfer/bin/";
  }

  assert(!kErrorArchTypeUnhandled);
  return std::string();
}

std::string ServerArch::GetStartServerCommand(int exit_code_not_found,
                                              const std::string& args) const {
  std::string server_path = RemoteToolsBinDir() + CdcServerFilename();

  if (IsWindowsArchType(type_)) {
    // TODO(ljusten): On Windows, ssh does not seem to forward the Powershell
    // exit code (exit_code_not_found) to the process. However, that's really
    // a minor issue and means we display "Deploying server..." instead of
    // "Server not deployed. Deploying...";
    return RemoteUtil::QuoteForWindows(
        absl::StrFormat("powershell -Command \" "
                        "Set-StrictMode -Version 2; "
                        "$ErrorActionPreference = 'Stop'; "
                        "if (-not (Test-Path -Path '%s')) { "
                        "  exit %i; "
                        "} "
                        "%s %s "
                        "\"",
                        server_path, exit_code_not_found, server_path, args));
  }

  if (IsLinuxArchType(type_)) {
    return absl::StrFormat("if [ ! -f %s ]; then exit %i; fi; %s %s",
                           server_path, exit_code_not_found, server_path, args);
  }

  assert(!kErrorArchTypeUnhandled);
  return std::string();
}

std::string ServerArch::GetDeploySftpCommands() const {
  std::string commands;

  // Create the remote tools bin dir if it doesn't exist yet.
  // This assumes that sftp's remote startup directory is the home directory.
  const std::string server_dir = path::ToUnix(RemoteToolsBinDir());
  std::vector<std::string> dir_parts =
      absl::StrSplit(server_dir, '/', absl::SkipEmpty());
  for (const std::string& dir : dir_parts) {
    // Use -mkdir to ignore errors if the directory already exists.
    commands += absl::StrFormat("-mkdir %s\ncd %s\n", dir, dir);
  }

  // Copy the server binary to a temp location. This assumes that sftp's local
  // startup directory is cdc_rsync's exe dir.
  const std::string server_file = CdcServerFilename();
  const std::string server_temp_file = server_file + Util::GenerateUniqueId();
  commands += absl::StrFormat("put %s %s\n", server_file, server_temp_file);

  // Restore permissions in case they changed and propagate temp file.
  commands += absl::StrFormat("-chmod 755 %s\n", server_file);
  commands += absl::StrFormat("chmod 755 %s\n", server_temp_file);
  commands += absl::StrFormat("rename %s %s\n", server_temp_file, server_file);

  return commands;
}

}  // namespace cdc_ft
