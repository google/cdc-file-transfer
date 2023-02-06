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

#include "common/gamelet_component.h"

#include "absl/strings/str_split.h"
#include "common/build_version.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class GameletComponentTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  std::string base_dir_ = GetTestDataDir("gamelet_component");

  std::string valid_component_path_ =
      path::Join(base_dir_, "valid", "cdc_rsync_server");
  std::string invalid_component_path_ =
      path::Join(base_dir_, "invalid", "cdc_rsync_server");
  std::string other_component_path_ =
      path::Join(base_dir_, "other", "cdc_rsync_server");
};

TEST_F(GameletComponentTest, EqualityOperators_DevelopmentVersion) {
  constexpr uint64_t size1 = 1001;
  constexpr uint64_t size2 = 1002;

  constexpr int64_t modified_time1 = 5001;
  constexpr int64_t modified_time2 = 5002;

  GameletComponent a(DEV_BUILD_VERSION, "file1", size1, modified_time1);

  GameletComponent b = a;
  EXPECT_TRUE(a == b && !(a != b));

  b.filename = "file2";
  EXPECT_TRUE(!(a == b) && a != b);

  b = a;
  b.size = size2;
  EXPECT_TRUE(!(a == b) && a != b);

  b = a;
  b.modified_time = modified_time2;
  EXPECT_TRUE(!(a == b) && a != b);

  b = a;
  b.size = size2;
  b.build_version = "Specified";
  EXPECT_TRUE(!(a == b) && a != b);

  a.build_version = "Specified";
  EXPECT_TRUE(a == b && !(a != b));
}

TEST_F(GameletComponentTest, EqualityOperators_SpecifiedVersion) {
  constexpr uint64_t size1 = 1001;
  constexpr uint64_t size2 = 1002;

  constexpr int64_t modified_time1 = 5001;
  constexpr int64_t modified_time2 = 5002;

  GameletComponent a("Specified", "file1", size1, modified_time1);

  GameletComponent b = a;
  EXPECT_TRUE(a == b && !(a != b));

  b.filename = "file2";
  EXPECT_TRUE(!(a == b) && a != b);

  b = a;
  b.size = size2;
  EXPECT_TRUE(a == b && !(a != b));

  b = a;
  b.modified_time = modified_time2;
  EXPECT_TRUE(a == b && !(a != b));
}

TEST_F(GameletComponentTest, GetValidComponents) {
  std::vector<GameletComponent> components;
  EXPECT_OK(GameletComponent::Get({valid_component_path_}, &components));
  ASSERT_EQ(components.size(), 1);

  EXPECT_EQ(components[0].filename, "cdc_rsync_server");
  EXPECT_GT(components[0].size, 0);
  EXPECT_GT(components[0].modified_time, 0);
}

TEST_F(GameletComponentTest, GetInvalidComponents) {
  std::vector<GameletComponent> components;
  EXPECT_NOT_OK(GameletComponent::Get({invalid_component_path_}, &components));
}

TEST_F(GameletComponentTest, GetChangedComponents) {
  std::vector<GameletComponent> components;
  EXPECT_OK(GameletComponent::Get({valid_component_path_}, &components));

  std::vector<GameletComponent> other_components;
  EXPECT_OK(GameletComponent::Get({other_component_path_}, &other_components));

  // Force equal timestamps, so that we don't depend on when the files were
  // actually written to everyone's drives.
  // Also force set build_version to developer since otherwise we would skip
  // component size check.
  ASSERT_EQ(components.size(), other_components.size());
  for (size_t n = 0; n < components.size(); ++n) {
    other_components[n].modified_time = components[n].modified_time;
    other_components[n].build_version = DEV_BUILD_VERSION;

    EXPECT_NE(components, other_components);
  }
}

TEST_F(GameletComponentTest, GetChangedComponents_BuildVersionChanged) {
  std::vector<GameletComponent> components;
  EXPECT_OK(GameletComponent::Get({valid_component_path_}, &components));

  std::vector<GameletComponent> other_components;
  EXPECT_OK(GameletComponent::Get({other_component_path_}, &other_components));

  ASSERT_EQ(components.size(), other_components.size());
  for (size_t n = 0; n < components.size(); ++n) {
    other_components[n].modified_time = components[n].modified_time;
    other_components[n].size = components[n].size;
    components[n].build_version = "build_version";
    other_components[n].build_version = "other_build_version";

    EXPECT_NE(components, other_components);
  }
}

TEST_F(GameletComponentTest, Serialization) {
  std::vector<GameletComponent> components;
  EXPECT_OK(GameletComponent::Get({valid_component_path_}, &components));

  std::string args = GameletComponent::ToCommandLineArgs(components);

  // FromCommandLineArgs() for a single string arg.
  std::vector<GameletComponent> deserialized_components =
      GameletComponent::FromCommandLineArgs(args);
  EXPECT_EQ(components, deserialized_components);

  // FromCommandLineArgs() for argc/argv.
  std::vector<std::string> args_vec = absl::StrSplit(args, ' ');
  int argc = static_cast<int>(args_vec.size());
  std::vector<const char*> argv;
  for (const std::string& arg : args_vec) argv.push_back(arg.c_str());
  deserialized_components =
      GameletComponent::FromCommandLineArgs(argc, argv.data());
  EXPECT_EQ(components, deserialized_components);
}

}  // namespace
}  // namespace cdc_ft
