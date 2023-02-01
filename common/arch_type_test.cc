// Copyright 2023 Google LLC
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

#include "common/arch_type.h"

#include "absl/strings/match.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

TEST(ArchTypeTest, GetLocalArchType) {
#if PLATFORM_WINDOWS
  EXPECT_TRUE(IsPlatformWindows(GetLocalArchType()));
#elif PLATFORM_LINUX
  EXPECT_TRUE(IsPlatformLinux(GetLocalArchType()));
#endif
}

TEST(ArchTypeTest, IsWindowsArchType) {
  EXPECT_TRUE(IsWindowsArchType(ArchType::kWindows_x86_64));
  EXPECT_FALSE(IsWindowsArchType(ArchType::kLinux_x86_64));
}

TEST(ArchTypeTest, IsLinuxArchType) {
  EXPECT_FALSE(IsLinuxArchType(ArchType::kWindows_x86_64));
  EXPECT_TRUE(IsLinuxArchType(ArchType::kLinux_x86_64));
}

TEST(ArchTypeTest, GetArchTypeStr) {
  EXPECT_TRUE(
      absl::StrContains(GetArchTypeStr(ArchType::kWindows_x86_64), "Windows"));
  EXPECT_TRUE(
      absl::StrContains(GetArchTypeStr(ArchType::kLinux_x86_64), "Linux"));
}

}  // namespace
}  // namespace cdc_ft
