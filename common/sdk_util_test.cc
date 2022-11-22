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

#include <stdlib.h>

#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr bool kCreateFile = true;
constexpr bool kDontCreateFile = false;

class SdkUtilTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }

  void TearDown() override {
    Log::Shutdown();
    for (std::string dir_path : test_created_directories_) {
      EXPECT_OK(path::RemoveDirRec(dir_path));
    }
  }

 protected:
  void CheckSdkPaths(const SdkUtil& sdk_util, const std::string& sdk_dir) {
    EXPECT_EQ(sdk_util.GetDevBinPath(), path::Join(sdk_dir, "dev", "bin"));
  }

  void SetupGetSdkVersion(const std::string& file_content,
                          bool create_version_file) {
    std::string ggp_sdk_path = std::tmpnam(nullptr);
    EXPECT_OK(path::SetEnv("GGP_SDK_PATH", ggp_sdk_path));
    test_created_directories_.push_back(ggp_sdk_path);
    EXPECT_OK(path::CreateDirRec(ggp_sdk_path));
    if (create_version_file) {
      std::string version_path = path::Join(ggp_sdk_path, "VERSION");
      EXPECT_OK(path::WriteFile(version_path, file_content.c_str(),
                                file_content.size()));
    }
  }

  // Contains assets which where created during test.
  // The assets are to be deleted in the end of the test.
  std::vector<std::string> test_created_directories_;
};

TEST_F(SdkUtilTest, CheckSdkPathsWithoutGgpSdkPathEnv) {
  // Clear environment variable and figure out default SDK dir.
  EXPECT_OK(path::SetEnv("GGP_SDK_PATH", ""));
  std::string program_files_dir;
  EXPECT_OK(path::GetKnownFolderPath(path::FolderId::kProgramFiles,
                                     &program_files_dir));
  const std::string sdk_dir = path::Join(program_files_dir, "GGP SDK");

  SdkUtil sdk_util;
  CheckSdkPaths(sdk_util, sdk_dir);
}

}  // namespace
}  // namespace cdc_ft
