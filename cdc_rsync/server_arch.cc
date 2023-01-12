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
#include "common/util.h"

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

std::string ServerArch::RemoteToolsBinDir() const {
  switch (type_) {
    case Type::kWindows: {
      return "AppData\\Roaming\\cdc-file-transfer\\bin\\";
    }
    case Type::kLinux:
      return ".cache/cdc-file-transfer/bin/";
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
}

std::string ServerArch::GetStartServerCommand(int exit_code_not_found,
                                              const std::string& args) const {
  std::string server_path = RemoteToolsBinDir() + CdcServerFilename();

  switch (type_) {
    case Type::kWindows:
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
    case Type::kLinux:
      return absl::StrFormat("if [ ! -f %s ]; then exit %i; fi; %s %s",
                             server_path, exit_code_not_found, server_path,
                             args);
    default:
      assert(!kErrorArchTypeUnhandled);
      return std::string();
  }
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
