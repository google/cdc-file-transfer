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

#include "absl/strings/match.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr auto kLinux = ServerArch::Type::kLinux;
constexpr auto kWindows = ServerArch::Type::kWindows;

TEST(ServerArchTest, DetectsLinuxIfPathStartsWithSlashOrTilde) {
  EXPECT_EQ(ServerArch::Detect("/linux/path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("/linux\\path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("~/linux/path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("~/linux\\path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("~\\linux\\path"), kLinux);
}

TEST(ServerArchTest, DetectsWindowsIfPathStartsWithDrive) {
  EXPECT_EQ(ServerArch::Detect("C:\\win\\path"), kWindows);
  EXPECT_EQ(ServerArch::Detect("D:win"), kWindows);
  EXPECT_EQ(ServerArch::Detect("Z:\\win/path"), kWindows);
}

TEST(ServerArchTest, DetectsLinuxIfPathOnlyHasForwardSlashes) {
  EXPECT_EQ(ServerArch::Detect("linux/path"), kLinux);
}

TEST(ServerArchTest, DetectsWindowsIfPathOnlyHasBackSlashes) {
  EXPECT_EQ(ServerArch::Detect("\\win\\path"), kWindows);
}

TEST(ServerArchTest, DetectsLinuxByDefault) {
  EXPECT_EQ(ServerArch::Detect("/mixed\\path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("/mixed\\path"), kLinux);
  EXPECT_EQ(ServerArch::Detect("C\\linux/path"), kLinux);
  EXPECT_EQ(ServerArch::Detect(""), kLinux);
}

TEST(ServerArchTest, CdcServerFilename) {
  EXPECT_FALSE(
      absl::StrContains(ServerArch(kLinux).CdcServerFilename(), "exe"));
  EXPECT_TRUE(
      absl::StrContains(ServerArch(kWindows).CdcServerFilename(), "exe"));
}

TEST(ServerArchTest, RemoteToolsBinDir) {
  const std::string linux_dir = ServerArch(kLinux).RemoteToolsBinDir();
  EXPECT_TRUE(absl::StrContains(linux_dir, ".cache/"));

  std::string win_dir = ServerArch(kWindows).RemoteToolsBinDir();
  EXPECT_TRUE(absl::StrContains(win_dir, "AppData\\Roaming\\"));
}

TEST(ServerArchTest, GetStartServerCommand) {
  std::string cmd = ServerArch(kWindows).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server.exe foo bar"));

  cmd = ServerArch(kLinux).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server foo bar"));
}

TEST(ServerArchTest, GetDeployReplaceCommand) {
  std::string cmd = ServerArch(kWindows).GetDeploySftpCommands();
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server.exe"));

  cmd = ServerArch(kLinux).GetDeploySftpCommands();
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server"));
}

}  // namespace
}  // namespace cdc_ft
