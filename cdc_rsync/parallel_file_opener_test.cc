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

#include "cdc_rsync/parallel_file_opener.h"

#include "common/path.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class ParallelFileOpenerTest : public ::testing::Test {
 protected:
  std::string base_dir_ = GetTestDataDir("parallel_file_opener");

  // Args 2 (file size) and 3 (base dir len) are not used.
  std::vector<ClientFileInfo> files_ = {
      ClientFileInfo(path::Join(base_dir_, "file1.txt"), 0, 0),
      ClientFileInfo(path::Join(base_dir_, "file2.txt"), 0, 0),
      ClientFileInfo(path::Join(base_dir_, "file3.txt"), 0, 0),
  };

  std::string ReadAndClose(FILE* file) {
    char line[256] = {0};
    EXPECT_TRUE(fgets(line, sizeof(line) - 1, file));
    fclose(file);
    return line;
  }
};

TEST_F(ParallelFileOpenerTest, OpenNoFiles) {
  ParallelFileOpener file_opener(&files_, {});
  EXPECT_EQ(file_opener.GetNextOpenFile(), nullptr);
}

TEST_F(ParallelFileOpenerTest, OpenSingleFile) {
  ASSERT_GE(files_.size(), 3);
  ParallelFileOpener file_opener(&files_, {1});

  FILE* file2 = file_opener.GetNextOpenFile();
  ASSERT_NE(file2, nullptr);
  EXPECT_EQ(ReadAndClose(file2), "data2");

  EXPECT_EQ(file_opener.GetNextOpenFile(), nullptr);
}

TEST_F(ParallelFileOpenerTest, OpenManyFiles) {
  const int num_indices = 500;
  std::vector<uint32_t> indices;
  for (int n = 0; n < num_indices; ++n) {
    indices.push_back(n % files_.size());
  }

  ParallelFileOpener file_opener(&files_, indices);

  for (int n = 0; n < num_indices; ++n) {
    FILE* file = file_opener.GetNextOpenFile();
    ASSERT_NE(file, nullptr);
    EXPECT_EQ(ReadAndClose(file), "data" + std::to_string(indices[n] + 1));
  }

  EXPECT_EQ(file_opener.GetNextOpenFile(), nullptr);
}

}  // namespace
}  // namespace cdc_ft
