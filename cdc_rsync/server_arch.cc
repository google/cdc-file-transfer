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
#include "common/path.h"
#include "common/remote_util.h"

namespace cdc_ft {

constexpr char kErrorFailedToGetKnownFolderPath[] =
    "error_failed_to_get_known_folder_path";
constexpr char kErrorArchTypeUnhandled[] = "arch_type_unhandled";

// static
ServerArch::Type ServerArch::Detect(const std::string& destination) {
  // Path starting with ~ or / -> Linux.
  if (absl::StartsWith(destination, "~") ||
      absl::StartsWith(destination, "/")) {
    return Type::kLinux;
  }

  // Path starting with C: etc. -> Windows.
  if (!path::GetDrivePrefix(destination).empty()) {
    return Type::kWindows;
  }

  // Path with only / -> Linux.
  if (absl::StrContains(destination, "/") &&
      !absl::StrContains(destination, "\\")) {
    return Type::kLinux;
  }

  // Path with only \\ -> Windows.
  if (absl::StrContains(destination, "\\") &&
      !absl::StrContains(destination, "/")) {
    return Type::kWindows;
  }

  // Default to Linux.
  return Type::kLinux;
}

ServerArch::ServerArch(Type type) : type_(type) {}

ServerArch::~ServerArch() {}

std::string ServerArch::CdcServerFilename() const {
  switch (type_) {
    case Type::kWindows:
      return "cdc_rsync_server.exe";
    case Type::kLinux:
      return "cdc_rsync_server";
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

std::string ServerArch::RemoteToolsBinDir(UseCase use_case) const {
  switch (type_) {
    case Type::kWindows: {
      // TODO(ljusten): Unfortunately, scp doesn't seem to support shell var
      // expansion, so %AppData% can't be used. This relies on scp copying
      // files relative to %UserProfile% and %AppData% mapping to
      // "AppData\\Roaming" relative to that.
      std::string app_data =
          use_case == UseCase::kScp ? "AppData\\Roaming" : "$env:appdata";
      return app_data + "\\cdc-file-transfer\\bin\\";
    }
    case Type::kLinux:
      return "~/.cache/cdc-file-transfer/bin/";
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

std::string ServerArch::GetStartServerCommand(int exit_code_not_found,
                                              const std::string& args) const {
  std::string server_dir = RemoteToolsBinDir(UseCase::kSsh);
  std::string server_path =
      RemoteUtil::QuoteForSsh(server_dir + CdcServerFilename());
  path::EnsureDoesNotEndWithPathSeparator(&server_dir);
  server_dir = RemoteUtil::QuoteForSsh(server_dir);

  switch (type_) {
    case Type::kWindows:
      // TODO(ljusten): On Windows, ssh does not seem to forward the Powershell
      // exit code (exit_code_not_found) to the process. However, that's really
      // a minor issue and means we display "Deploying server..." instead of
      // "Server not deployed. Deploying...";
      return RemoteUtil::QuoteForWindows(
          absl::StrFormat("Set-StrictMode -Version 2; "
                          "$ErrorActionPreference = 'Stop'; "
                          "$server_dir = %s; "
                          "$server_path = %s; "
                          "$u=New-Item $server_dir -ItemType Directory -Force; "
                          "if (-not (Test-Path -Path $server_path)) { "
                          "  exit %i; "
                          "} "
                          "& $server_path %s",
                          server_dir, server_path, exit_code_not_found, args));
    case Type::kLinux:
      return absl::StrFormat(
          "mkdir -p %s; if [ ! -f %s ]; then exit %i; fi; %s %s", server_dir,
          server_path, exit_code_not_found, server_path, args);
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

std::string ServerArch::GetDeployReplaceCommand(
    const std::string& old_server_path,
    const std::string& new_server_path) const {
  const std::string old_path = RemoteUtil::QuoteForSsh(old_server_path);
  const std::string new_path = RemoteUtil::QuoteForSsh(new_server_path);

  switch (type_) {
    case Type::kWindows:
      return RemoteUtil::QuoteForWindows(absl::StrFormat(
          "Set-StrictMode -Version 2; "
          "$ErrorActionPreference = 'Stop'; "
          "$old_path = %s; "
          "$new_path = %s; "
          "if (Test-Path -Path $old_path) { "
          "  Get-Item -Path $old_path | "
          "    Set-ItemProperty -Name IsReadOnly -Value $false; "
          "} "
          "Move-Item -Path $new_path -Destination $old_path -Force",
          old_path, new_path));
    case Type::kLinux:
      return absl::StrFormat(
          "([ ! -f %s ] || chmod u+w %s) && chmod a+x %s && mv %s %s", old_path,
          old_path, new_path, new_path, old_path);
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

}  // namespace cdc_ft
