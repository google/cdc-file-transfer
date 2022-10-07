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

#include "cdc_rsync_server/file_finder.h"

#include "common/log.h"
#include "common/path.h"
#include "common/path_filter.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr bool kNonRecursive = false;
constexpr bool kRecursive = true;

class FileFinderTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  std::string base_dir_ =
      path::Join(GetTestDataDir("file_finder"), path::ToNative("base_dir/"));

  std::string copy_dest_ =
      path::Join(GetTestDataDir("file_finder"), path::ToNative("copy_dest/"));

  template <typename PathClass>
  static void ExpectMatch(
      const std::vector<PathClass>& paths,
      std::vector<std::pair<std::string, std::string>> base_dir_and_rel_path) {
    EXPECT_EQ(base_dir_and_rel_path.size(), paths.size());
    if (base_dir_and_rel_path.size() != paths.size()) return;

    for (size_t n = 0; n < paths.size(); ++n) {
      EXPECT_EQ(paths[n].base_dir, base_dir_and_rel_path[n].first);
      EXPECT_EQ(paths[n].filepath, base_dir_and_rel_path[n].second);
    }
  }

  PathFilter path_filter_;
  std::vector<FileInfo> files_;
  std::vector<DirInfo> dirs_;
};

TEST_F(FileFinderTest, FindSucceedsInvalidPath) {
  // Invalid paths are just ignored.
  std::string invalid_path = path::Join(base_dir_, "invalid");
  FileFinder finder;
  EXPECT_OK(finder.AddFiles(invalid_path, kNonRecursive, &path_filter_));
  finder.ReleaseFiles(&files_, &dirs_);
  EXPECT_TRUE(files_.empty());
  EXPECT_TRUE(dirs_.empty());
}

TEST_F(FileFinderTest, FindSucceedsNonRecursive) {
  FileFinder finder;
  EXPECT_OK(finder.AddFiles(base_dir_, kNonRecursive, &path_filter_));
  finder.ReleaseFiles(&files_, &dirs_);
  ExpectMatch(files_, {{base_dir_, "a.txt"}, {base_dir_, "b.txt"}});
  ExpectMatch(dirs_, {{base_dir_, "dir1"}, {base_dir_, "dir2"}});
}

TEST_F(FileFinderTest, FindSucceedsRecursive) {
  FileFinder finder;
  EXPECT_OK(finder.AddFiles(base_dir_, kRecursive, &path_filter_));
  finder.ReleaseFiles(&files_, &dirs_);
  ExpectMatch(files_, {{base_dir_, "a.txt"},
                       {base_dir_, "b.txt"},
                       {base_dir_, path::ToNative("dir1/c.txt")},
                       {base_dir_, path::ToNative("dir2/d.txt")}});
  ExpectMatch(dirs_, {{base_dir_, "dir1"}, {base_dir_, "dir2"}});
}

TEST_F(FileFinderTest, FindSucceedsRecursiveWithCopyDest) {
  FileFinder finder;
  EXPECT_OK(finder.AddFiles(base_dir_, kRecursive, &path_filter_));
  EXPECT_OK(finder.AddFiles(copy_dest_, kRecursive, &path_filter_));
  finder.ReleaseFiles(&files_, &dirs_);
  ExpectMatch(files_, {{base_dir_, "a.txt"},
                       {base_dir_, "b.txt"},
                       {base_dir_, path::ToNative("dir1/c.txt")},
                       {copy_dest_, path::ToNative("dir1/f.txt")},
                       {base_dir_, path::ToNative("dir2/d.txt")},
                       {copy_dest_, path::ToNative("dir3/d.txt")},
                       {copy_dest_, "e.txt"}});
  ExpectMatch(dirs_,
              {{base_dir_, "dir1"}, {base_dir_, "dir2"}, {copy_dest_, "dir3"}});
}

TEST_F(FileFinderTest, FindSucceedsWithFilter) {
  path_filter_.AddRule(PathFilter::Rule::Type::kExclude, "a.txt");

  FileFinder finder;
  EXPECT_OK(finder.AddFiles(base_dir_, kRecursive, &path_filter_));
  finder.ReleaseFiles(&files_, &dirs_);
  ExpectMatch(files_, {{base_dir_, "b.txt"},
                       {base_dir_, path::ToNative("dir1/c.txt")},
                       {base_dir_, path::ToNative("dir2/d.txt")}});
  ExpectMatch(dirs_, {{base_dir_, "dir1"}, {base_dir_, "dir2"}});
}

}  // namespace
}  // namespace cdc_ft
