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

#include "common/sdk_util.h"

#include <cassert>
#include <string>

#include "common/clock.h"
#include "common/path.h"
#include "common/status_macros.h"

namespace cdc_ft {

SdkUtil::SdkUtil() {
  init_status_ = path::GetKnownFolderPath(path::FolderId::kRoamingAppData,
                                          &roaming_appdata_path_);
  init_status_.Update(path::GetKnownFolderPath(path::FolderId::kProgramFiles,
                                               &program_files_path_));
  absl::Status status = path::GetEnv("GGP_SDK_PATH", &ggp_sdk_path_env_);
  if (absl::IsNotFound(status) || ggp_sdk_path_env_.empty())
    ggp_sdk_path_env_ = path::Join(program_files_path_, "GGP SDK");

  // Create an empty config file if it does not exist yet.
  const std::string ssh_config_path = GetSshConfigPath();
  if (!path::Exists(ssh_config_path)) {
    init_status_.Update(path::CreateDirRec(path::DirName(ssh_config_path)));
    init_status_.Update(path::WriteFile(ssh_config_path, nullptr, 0));
  }
}

SdkUtil::~SdkUtil() = default;

std::string SdkUtil::GetUserConfigPath() const {
  assert(init_status_.ok());
  return path::Join(roaming_appdata_path_, "GGP");
}

std::string SdkUtil::GetServicesConfigPath() const {
  return path::Join(GetUserConfigPath(), "services");
}

std::string SdkUtil::GetLogPath(const char* log_base_name) const {
  DefaultSystemClock clock;
  std::string timestamp_ext = clock.FormatNow(".%Y%m%d-%H%M%S.log", false);
  return path::Join(GetUserConfigPath(), "logs", log_base_name + timestamp_ext);
}

std::string SdkUtil::GetSshConfigPath() const {
  return path::Join(GetUserConfigPath(), "ssh", "config");
}

std::string SdkUtil::GetSshKeyFilePath() const {
  return path::Join(GetUserConfigPath(), "ssh", "id_rsa");
}

std::string SdkUtil::GetSshKnownHostsFilePath() const {
  return path::Join(GetUserConfigPath(), "ssh", "known_hosts");
}

std::string SdkUtil::GetSDKPath() const {
  assert(init_status_.ok());
  return ggp_sdk_path_env_;
}

std::string SdkUtil::GetDevBinPath() const {
  return path::Join(GetSDKPath(), "dev", "bin");
}

std::string SdkUtil::GetSshPath() const {
  return path::Join(GetSDKPath(), "tools", "OpenSSH-Win64");
}

std::string SdkUtil::GetSshExePath() const {
  return path::Join(GetSshPath(), "ssh.exe");
}

std::string SdkUtil::GetScpExePath() const {
  return path::Join(GetSshPath(), "scp.exe");
}

}  // namespace cdc_ft
