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
}

SdkUtil::~SdkUtil() = default;

std::string SdkUtil::GetDevBinPath() const {
  return path::Join(ggp_sdk_path_env_, "dev", "bin");
}

}  // namespace cdc_ft
