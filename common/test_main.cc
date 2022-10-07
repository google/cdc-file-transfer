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

#include "common/path.h"
#include "gtest/gtest.h"
#include "tools/cpp/runfiles/runfiles.h"

using bazel::tools::cpp::runfiles::Runfiles;

namespace cdc_ft {
namespace {
Runfiles* runfiles_ptr;
}

std::string GetTestDataDir(const char* test_data_dir) {
  std::string test_path =
      ::testing::UnitTest::GetInstance()->current_test_info()->file();
  std::string test_dir = path::DirName(test_path);
  std::string root_runfile = path::ToUnix(
      path::Join("cdc_file_transfer", test_dir, "testdata", "root.txt"));
  assert(runfiles_ptr != nullptr);
  std::string root_path = runfiles_ptr->Rlocation(root_runfile);
  path::FixPathSeparators(&root_path);
  EXPECT_TRUE(path::Exists(root_path))
      << path::Join(test_dir, "testdata") << " directory has no root.txt file";
  return path::Join(path::DirName(root_path), test_data_dir);
}

}  // namespace cdc_ft

int main(int argc, char** argv) {
  std::string error;
  std::unique_ptr<Runfiles> runfiles(Runfiles::Create(argv[0], &error));
  if (runfiles == nullptr) {
    std::cerr << "Failed to locate runtime files: " << error << std::endl;
    return 1;
  }
  cdc_ft::runfiles_ptr = runfiles.get();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
