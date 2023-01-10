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
  const std::string linux_ssh_dir =
      ServerArch(kLinux).RemoteToolsBinDir(ServerArch::UseCase::kSsh);
  EXPECT_TRUE(absl::StrContains(linux_ssh_dir, "/"));

  const std::string linux_scp_dir =
      ServerArch(kLinux).RemoteToolsBinDir(ServerArch::UseCase::kScp);
  EXPECT_EQ(linux_ssh_dir, linux_scp_dir);

  const std::string win_ssh_dir =
      ServerArch(kWindows).RemoteToolsBinDir(ServerArch::UseCase::kSsh);
  EXPECT_TRUE(absl::StrContains(win_ssh_dir, "\\"));
  EXPECT_TRUE(absl::StrContains(win_ssh_dir, "$env:appdata"));

  std::string win_scp_dir =
      ServerArch(kWindows).RemoteToolsBinDir(ServerArch::UseCase::kScp);
  EXPECT_TRUE(absl::StrContains(win_scp_dir, "\\"));
  EXPECT_TRUE(absl::StrContains(win_scp_dir, "AppData\\Roaming"));
}

TEST(ServerArchTest, GetStartServerCommand) {
  std::string cmd = ServerArch(kWindows).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "foo bar"));
  EXPECT_TRUE(absl::StrContains(cmd, "New-Item "));

  cmd = ServerArch(kLinux).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "foo bar"));
  EXPECT_TRUE(absl::StrContains(cmd, "mkdir -p"));
}

TEST(ServerArchTest, GetDeployReplaceCommand) {
  std::string cmd = ServerArch(kWindows).GetDeployReplaceCommand("aaa", "bbb");
  EXPECT_TRUE(absl::StrContains(cmd, "aaa"));
  EXPECT_TRUE(absl::StrContains(cmd, "bbb"));
  EXPECT_TRUE(absl::StrContains(cmd, "Move-Item "));

  cmd = ServerArch(kLinux).GetDeployReplaceCommand("aaa", "bbb");
  EXPECT_TRUE(absl::StrContains(cmd, "aaa"));
  EXPECT_TRUE(absl::StrContains(cmd, "bbb"));
  EXPECT_TRUE(absl::StrContains(cmd, "mv "));
}

}  // namespace
}  // namespace cdc_ft
