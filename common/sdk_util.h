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

#ifndef COMMON_SDK_UTIL_H_
#define COMMON_SDK_UTIL_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/platform.h"

#if !PLATFORM_WINDOWS
#error SdkUtil only supports Windows so far.
#endif

namespace cdc_ft {

// Provides paths to selected files in the Stadia Windows SDK.
class SdkUtil {
 public:
  SdkUtil();
  ~SdkUtil();

  // Returns the path of the dev tools that ship with the SDK, e.g.
  // C:\Program Files\GGP SDK\dev\bin.
  std::string GetDevBinPath() const;

 private:
  std::string roaming_appdata_path_;
  std::string program_files_path_;
  std::string ggp_sdk_path_env_;
  absl::Status init_status_;
  std::string full_sdk_version_;
};

}  // namespace cdc_ft

#endif  // COMMON_SDK_UTIL_H_
