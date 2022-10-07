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

#include "common/dir_iter.h"

#include <algorithm>
#include <iterator>

#include "common/path.h"
#include "common/platform.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class DirectoryIteratorTest : public ::testing::Test {
 public:
  using StringSet = std::set<std::string>;

  DirectoryIteratorTest() {}

  void SetUp() override {
    expected_files_.insert("a/aa/aaa1.txt");
    expected_files_.insert("a/aa/aaa2.txt");
    expected_files_.insert("a/aa1.txt");
    expected_files_.insert("a/aa2.txt");
    expected_files_.insert("a/ab/aab1.txt");
    expected_files_.insert("a/ab/aab2.txt");
    expected_files_.insert("b/ba/bba1.txt");
    expected_files_.insert("b/ba/bba2.txt");
    expected_files_.insert("b/bb/bbb1.txt");
    expected_files_.insert("b/bb/bbb2.txt");
    expected_files_.insert("c/c1.txt");
    expected_files_.insert("c/c2.txt");
    expected_files_.insert("d/d1.txt");
    expected_files_.insert("d/d2.txt");
    expected_files_.insert("root.txt");

    expected_dirs_.insert("a");
    expected_dirs_.insert("a/aa");
    expected_dirs_.insert("a/ab");
    expected_dirs_.insert("b");
    expected_dirs_.insert("b/ba");
    expected_dirs_.insert("b/bb");
    expected_dirs_.insert("c");
    expected_dirs_.insert("d");
  }

  static StringSet Union(const StringSet& a, const StringSet& b) {
    StringSet out(a);
    out.insert(b.begin(), b.end());
    return out;
  }

  static StringSet Filter(
      const StringSet& items,
      std::function<bool(const std::string& item)> filter_fn) {
    StringSet out;
    std::copy_if(items.begin(), items.end(), std::inserter(out, out.begin()),
                 [&](const std::string& f) { return !filter_fn(f); });
    return out;
  }

 protected:
  std::set<std::string> expected_files_;
  std::set<std::string> expected_dirs_;
  std::string test_data_dir_ = GetTestDataDir("dir_iter");
};

TEST_F(DirectoryIteratorTest, EmptyDirIterator) {
  DirectoryIterator dit;
  DirectoryEntry dent;
  EXPECT_OK(dit.Status());
  EXPECT_FALSE(dit.Valid());
  EXPECT_EQ(dit.Path(), std::string());
  EXPECT_FALSE(dit.NextEntry(&dent));
}

TEST_F(DirectoryIteratorTest, ValidDirIterator) {
  DirectoryIterator dit(test_data_dir_);
  DirectoryEntry dent;
  EXPECT_OK(dit.Status());
  EXPECT_TRUE(dit.Valid());
  EXPECT_EQ(dit.Path(), test_data_dir_);
  EXPECT_TRUE(dit.NextEntry(&dent));
}

TEST_F(DirectoryIteratorTest, FindFiles) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kFiles, false);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  std::set<std::string> expected = Filter(
      expected_files_,
      [](const std::string& f) { return f.find('/') != std::string::npos; });
  EXPECT_EQ(found, expected);
}

TEST_F(DirectoryIteratorTest, FindDirectories) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kDirectories,
                        false);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  std::set<std::string> expected = Filter(
      expected_dirs_,
      [](const std::string& f) { return f.find('/') != std::string::npos; });
  EXPECT_EQ(found, expected);
}

TEST_F(DirectoryIteratorTest, FindFilesAndDirectories) {
  DirectoryIterator dit(test_data_dir_,
                        DirectorySearchFlags::kFilesAndDirectories, false);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  std::set<std::string> expected = Filter(
      Union(expected_dirs_, expected_files_),
      [](const std::string& f) { return f.find('/') != std::string::npos; });
  EXPECT_EQ(found, expected);
}

TEST_F(DirectoryIteratorTest, FindFilesRecursive) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kFiles);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  EXPECT_EQ(found, expected_files_);
}

TEST_F(DirectoryIteratorTest, FindDirectoriesRecursive) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kDirectories);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  EXPECT_EQ(found, expected_dirs_);
}

TEST_F(DirectoryIteratorTest, FindFilesAndDirectoriesRecursive) {
  DirectoryIterator dit(test_data_dir_);
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_OK(dit.Status());
  EXPECT_EQ(found, Union(expected_dirs_, expected_files_));
}

TEST_F(DirectoryIteratorTest, DirNotFound) {
  DirectoryIterator dit("does/not/exist");
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_TRUE(found.empty());
  EXPECT_TRUE(absl::IsNotFound(dit.Status()));
}

TEST_F(DirectoryIteratorTest, HandleError) {
  // Trying to open a file instead of a directory.
  DirectoryIterator dit(path::Join(test_data_dir_, "root.txt"));
  DirectoryEntry dent;
  std::set<std::string> found;
  while (dit.NextEntry(&dent)) {
    found.insert(path::ToUnix(dent.RelPathName()));
  }
  EXPECT_TRUE(found.empty());
  EXPECT_NOT_OK(dit.Status());
}

TEST_F(DirectoryIteratorTest, EmptyDirEntry) {
  DirectoryEntry dent;
  EXPECT_FALSE(dent.Valid());
  EXPECT_FALSE(dent.IsDir());
  EXPECT_FALSE(dent.IsRegularFile());
  EXPECT_FALSE(dent.IsSymlink());
  EXPECT_EQ(dent.Name(), std::string());
  EXPECT_EQ(dent.RelPath(), std::string());
  EXPECT_EQ(dent.RelPathName(), std::string());
}

TEST_F(DirectoryIteratorTest, DirEntryForFile) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kFiles, false);
  DirectoryEntry dent;
  EXPECT_TRUE(dit.NextEntry(&dent));
  EXPECT_OK(dit.Status());
  EXPECT_TRUE(dent.Valid());
  EXPECT_FALSE(dent.IsDir());
  EXPECT_TRUE(dent.IsRegularFile());
#ifdef PLATFORM_WINDOWS
  // On Windows, Bazel does not copy nor link data dependencies.
  EXPECT_FALSE(dent.IsSymlink());
#else
  // On Linux, Bazel creates symlinks for files in data dependencies.
  EXPECT_TRUE(dent.IsSymlink());
#endif
  EXPECT_EQ(dent.Name(), "root.txt");
  EXPECT_EQ(dent.RelPath(), std::string());
  EXPECT_EQ(dent.RelPathName(), "root.txt");
}

TEST_F(DirectoryIteratorTest, DirEntryForDir) {
  std::set<std::string> expected_any = Filter(
      expected_dirs_,
      [](const std::string& f) { return f.find('/') != std::string::npos; });

  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kDirectories,
                        false);
  DirectoryEntry dent;
  EXPECT_TRUE(dit.NextEntry(&dent));
  EXPECT_OK(dit.Status());
  EXPECT_TRUE(dent.Valid());
  EXPECT_TRUE(dent.IsDir());
  EXPECT_FALSE(dent.IsRegularFile());
  EXPECT_FALSE(dent.IsSymlink());
  EXPECT_TRUE(expected_any.find(dent.Name()) != expected_any.end());
  EXPECT_EQ(dent.RelPath(), std::string());
  EXPECT_TRUE(expected_any.find(dent.RelPathName()) != expected_any.end());
}

TEST_F(DirectoryIteratorTest, DirEntryClear) {
  DirectoryIterator dit(test_data_dir_, DirectorySearchFlags::kFiles, false);
  DirectoryEntry dent;
  EXPECT_TRUE(dit.NextEntry(&dent));
  EXPECT_OK(dit.Status());
  dent.Clear();
  EXPECT_FALSE(dent.Valid());
  EXPECT_FALSE(dent.IsDir());
  EXPECT_FALSE(dent.IsRegularFile());
  EXPECT_FALSE(dent.IsSymlink());
  EXPECT_EQ(dent.Name(), std::string());
  EXPECT_EQ(dent.RelPath(), std::string());
  EXPECT_EQ(dent.RelPathName(), std::string());
}

}  // namespace
}  // namespace cdc_ft
