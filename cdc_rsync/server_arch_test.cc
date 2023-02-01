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

constexpr auto kLinux = ArchType::kLinux_x86_64;
constexpr auto kWindows = ArchType::kWindows_x86_64;

constexpr bool kNoGuess = false;

TEST(ServerArchTest, GuessesLinuxIfPathStartsWithSlashOrTilde) {
  EXPECT_EQ(ServerArch::GuessFromDestination("/linux/path").GetType(), kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("/linux\\path").GetType(), kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("~/linux/path").GetType(), kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("~/linux\\path").GetType(),
            kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("~\\linux\\path").GetType(),
            kLinux);
}

TEST(ServerArchTest, GuessesWindowsIfPathStartsWithDrive) {
  EXPECT_EQ(ServerArch::GuessFromDestination("C:\\win\\path").GetType(),
            kWindows);
  EXPECT_EQ(ServerArch::GuessFromDestination("D:win").GetType(), kWindows);
  EXPECT_EQ(ServerArch::GuessFromDestination("Z:\\win/path").GetType(),
            kWindows);
}

TEST(ServerArchTest, GuessesLinuxIfPathOnlyHasForwardSlashes) {
  EXPECT_EQ(ServerArch::GuessFromDestination("linux/path").GetType(), kLinux);
}

TEST(ServerArchTest, GuessesWindowsIfPathOnlyHasBackSlashes) {
  EXPECT_EQ(ServerArch::GuessFromDestination("\\win\\path").GetType(),
            kWindows);
}

TEST(ServerArchTest, GuessesLinuxByDefault) {
  EXPECT_EQ(ServerArch::GuessFromDestination("/mixed\\path").GetType(), kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("/mixed\\path").GetType(), kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("C\\linux/path").GetType(),
            kLinux);
  EXPECT_EQ(ServerArch::GuessFromDestination("").GetType(), kLinux);
}

TEST(ServerArchTest, IsGuess) {
  EXPECT_TRUE(ServerArch::GuessFromDestination("foo").IsGuess());
  EXPECT_FALSE(ServerArch::DetectFromLocalDevice().IsGuess());
}

TEST(ServerArchTest, CdcServerFilename) {
  EXPECT_FALSE(absl::StrContains(
      ServerArch(kLinux, kNoGuess).CdcServerFilename(), "exe"));
  EXPECT_TRUE(absl::StrContains(
      ServerArch(kWindows, kNoGuess).CdcServerFilename(), "exe"));
}

TEST(ServerArchTest, RemoteToolsBinDir) {
  const std::string linux_dir =
      ServerArch(kLinux, kNoGuess).RemoteToolsBinDir();
  EXPECT_TRUE(absl::StrContains(linux_dir, ".cache/"));

  std::string win_dir = ServerArch(kWindows, kNoGuess).RemoteToolsBinDir();
  EXPECT_TRUE(absl::StrContains(win_dir, "AppData\\Roaming\\"));
}

TEST(ServerArchTest, GetStartServerCommand) {
  std::string cmd =
      ServerArch(kWindows, kNoGuess).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server.exe foo bar"));

  cmd = ServerArch(kLinux, kNoGuess).GetStartServerCommand(123, "foo bar");
  EXPECT_TRUE(absl::StrContains(cmd, "123"));
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server foo bar"));
}

TEST(ServerArchTest, GetDeployReplaceCommand) {
  std::string cmd = ServerArch(kWindows, kNoGuess).GetDeploySftpCommands();
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server.exe"));

  cmd = ServerArch(kLinux, kNoGuess).GetDeploySftpCommands();
  EXPECT_TRUE(absl::StrContains(cmd, "cdc_rsync_server"));
}

}  // namespace
}  // namespace cdc_ft
