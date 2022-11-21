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

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <memory>

#include "absl/strings/match.h"
#include "common/buffer.h"
#include "common/log.h"
#include "common/platform.h"
#include "common/status.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "common/util.h"
#include "gtest/gtest.h"

#if PLATFORM_WINDOWS
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <windows.h>  // GetLongPathName
#undef ReplaceFile
#endif

namespace cdc_ft {
namespace {

// Clang-format tends to screw up the code following unicode string literals,
// so we just escape U+1F964 here.
constexpr char kUnicodeText[] = u8"\U0001F964\U0001F964\U0001F964";
constexpr char kTestText[] = "test text";
constexpr char kLinesText[] = R"!(
line
trailing whitespace  	


 	 leading	whitespace
)!";

constexpr char kBaseDirName[] = "__path_unittest_base";
constexpr char kSearchDirName[] = "search";
constexpr char kSubDirName[] = "subdir";

constexpr char kLinesFileName[] = "lines_test.txt";
constexpr char kTestFileName[] = "test.txt";
constexpr char kUnicodeTestFileName[] = u8"unicode test \U0001F964.txt";
constexpr char kSubDirFileName[] = "subdir_file.txt";

// Converts e.g. C:\Progra~1 to C:\Program Files on Windows.
std::string GetLongPath(std::string path) {
#if PLATFORM_WINDOWS
  std::wstring wpath = Util::Utf8ToWideStr(path);
  std::wstring long_wpath(4096, 0);
  size_t buf_len =
      GetLongPathName(wpath.c_str(), const_cast<wchar_t*>(long_wpath.c_str()),
                      long_wpath.size() * sizeof(wchar_t));
  EXPECT_GT(buf_len, 0);
  long_wpath.resize(buf_len);
  return Util::WideToUtf8Str(long_wpath);
#else
  return path;
#endif
}

class PathTest : public ::testing::Test {
 public:
  PathTest() {
    path::EnsureEndsWithPathSeparator(&base_dir_);
    path::EnsureEndsWithPathSeparator(&search_dir_);
    path::EnsureEndsWithPathSeparator(&subdir_);
  }

  static void SetUpTestSuite() {
    const std::string base_dir = path::Join(path::GetTempDir(), kBaseDirName);
    const std::string search_dir = path::Join(base_dir, kSearchDirName);
    const std::string subdir = path::Join(search_dir, kSubDirName);

    const std::string lines_filepath = path::Join(base_dir, kLinesFileName);
    const std::string test_filepath = path::Join(search_dir, kTestFileName);
    const std::string unicode_test_filepath =
        path::Join(search_dir, kUnicodeTestFileName);
    const std::string subdir_filepath = path::Join(subdir, kSubDirFileName);

    // Create test data. Note that since Bazel doesn't support unicode paths, we
    // can't use runfiles.
    EXPECT_OK(path::RemoveDirRec(base_dir));
    EXPECT_OK(path::CreateDirRec(subdir));
    WriteStringToFile(lines_filepath, kLinesText);
    WriteStringToFile(test_filepath, kTestText);
    WriteStringToFile(unicode_test_filepath, kUnicodeText);
    WriteStringToFile(subdir_filepath, kTestText);
  }

  static void TearDownTestSuite() {
    const std::string base_dir = path::Join(path::GetTempDir(), kBaseDirName);
    EXPECT_OK(path::RemoveDirRec(base_dir));
  }

  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));

    // Clean up possible left-overs from previous runs (in case TearDown()
    // didn't run, e.g. when the process was killed).
    ForceRemoveFile(tmp_path_1_);
    ForceRemoveFile(tmp_path_2_);
  }

  void TearDown() override {
    // Clean up.
    ForceRemoveFile(tmp_path_1_);
    ForceRemoveFile(tmp_path_2_);

    Log::Shutdown();
  }

 protected:
  struct File {
    std::string dir_;
    std::string filename_;
    bool is_directory_ = false;
    std::string Path() const { return path::Join(dir_, filename_); }
  };

  static void WriteStringToFile(const std::string& path,
                                const std::string& contents) {
    Buffer b;
    b.resize(contents.size());
    memcpy(b.data(), contents.data(), contents.size());
    EXPECT_OK(path::WriteFile(path, b));
  }

  // Loads "read_all_lines_test.txt" with path::ReadAllLines and the given
  // |flags| and verifies that the |expected| lines were read.
  void CheckReadAllLines(path::ReadFlags flags,
                         std::vector<std::string> expected) {
    std::vector<std::string> lines;
    EXPECT_OK(path::ReadAllLines(lines_filepath_, &lines, flags));
    ASSERT_EQ(lines.size(), expected.size());
    for (size_t n = 0; n < lines.size(); ++n) {
      EXPECT_EQ(lines[n], expected[n]);
    }
  }

  // Reads from |test_filepath_| in pieces of |buffer_size| bytes and verifies
  // that the data read matches |expected_data|. Uses StreamReadFileContents().
  void CheckStreamReadFileContents(
      size_t buffer_size, std::vector<std::string> expected_data) const;

  // Removes the write permissions from the file at |path|.
  void RemoveWritePermission(const std::string& path) {
    path::Stats stats;
    EXPECT_OK(path::GetStats(path, &stats));
    EXPECT_OK(path::ChangeMode(path, stats.mode & ~path::MODE_IWUSR));
  }

  // Removes the file at |path|, even if it is write-protected.
  void ForceRemoveFile(const std::string& path) {
    if (!path::Exists(path)) {
      return;
    }

    // Add write permission, so file can be removed.
    path::Stats stats;
    EXPECT_OK(path::GetStats(path, &stats));
    EXPECT_OK(path::ChangeMode(path, stats.mode | path::MODE_IWUSR));
    EXPECT_OK(path::RemoveFile(path));
  }

  std::vector<File> SearchFiles(const std::string& pattern, bool recursive);

  // Some tests get confused if the temp dir is a short filename
  // (e.g. RUNNER~1 instead of runneradmin - I'm looking at you, Github!).
  const std::string tmp_dir_ = GetLongPath(path::GetTempDir());
  std::string base_dir_ = path::Join(tmp_dir_, kBaseDirName);
  std::string search_dir_ = path::Join(base_dir_, kSearchDirName);
  std::string subdir_ = path::Join(search_dir_, kSubDirName);
  const std::string subdir_fulldirpath_ = subdir_;

  const std::string lines_filepath_ = path::Join(base_dir_, kLinesFileName);
  const std::string test_filepath_ = path::Join(search_dir_, kTestFileName);
  const std::string unicode_test_filepath_ =
      path::Join(search_dir_, kUnicodeTestFileName);
  const std::string subdir_filepath_ = path::Join(subdir_, kSubDirFileName);

  const std::string tmp_path_1_ = path::Join(base_dir_, "a.txt");
  const std::string tmp_path_2_ = path::Join(base_dir_, "b.txt");
};

TEST_F(PathTest, GetExeDir) {
  std::string exe_dir;
  EXPECT_OK(path::GetExeDir(&exe_dir));

  // We don't know the expected path, but at least we can test that we have a
  // valid path consisting of at least one directory and a file name.
  std::vector<absl::string_view> parts =
      SplitString(exe_dir, path::PathSeparator(), false);
  EXPECT_GE(parts.size(), 1) << exe_dir;
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, GetKnownFolderPath) {
  std::string program_files;
  EXPECT_OK(
      path::GetKnownFolderPath(path::FolderId::kProgramFiles, &program_files));
  EXPECT_NE(program_files.find("Program"), std::string::npos);

  std::string appdata;
  EXPECT_OK(
      path::GetKnownFolderPath(path::FolderId::kRoamingAppData, &appdata));
  EXPECT_NE(appdata.find("Roaming"), std::string::npos);
}
#endif

TEST_F(PathTest, ExpandPathVariables) {
#if PLATFORM_WINDOWS
  std::string path = u8"%userPROfile%\\fOo\U0001F964";
  EXPECT_OK(path::ExpandPathVariables(&path));
  EXPECT_TRUE(absl::StartsWith(path, "C:\\Users\\")) << path;
  EXPECT_TRUE(absl::EndsWith(path, u8"\\fOo\U0001F964")) << path;

  path = "%ProgramFiles(x86)%\\Foo";
  EXPECT_OK(path::ExpandPathVariables(&path));
  EXPECT_EQ(path, "C:\\Program Files (x86)\\Foo");

  path = "%unrelated%\\fOo";
  EXPECT_OK(path::ExpandPathVariables(&path));
  EXPECT_EQ(path, "%unrelated%\\fOo") << path;
#else
  std::string path = u8"fooU0001F964";
  EXPECT_OK(path::ExpandPathVariables(&path));
  EXPECT_EQ(path, u8"fooU0001F964");

  path = "~/foo";
  EXPECT_OK(path::ExpandPathVariables(&path));
  EXPECT_TRUE(absl::StrContains(path, "home")) << path;

  path = "*";
  EXPECT_ERROR_MSG(InvalidArgument, "multiple results",
                   path::ExpandPathVariables(&path));
#endif
}

TEST_F(PathTest, GetEnv_DoesNotExist) {
  std::string value;
  EXPECT_TRUE(absl::IsNotFound(
      path::GetEnv("PRETTY_SURE_THIS_DOES_NOT_EXIST", &value)));
  EXPECT_TRUE(value.empty());
}

TEST_F(PathTest, SetEnv_Empty) {
  const std::string name = "env name";
  const std::string empty_value;
  EXPECT_OK(path::SetEnv(name, empty_value));

  std::string value;
  EXPECT_OK(path::GetEnv(name, &value));
  EXPECT_EQ(value, empty_value);
}

TEST_F(PathTest, SetEnv_NonEmpty) {
  const std::string name = "env name";
  const std::string expected_value = u8"some unicode \U0001F964 value";
  EXPECT_OK(path::SetEnv(name, expected_value));

  std::string value;
  EXPECT_OK(path::GetEnv(name, &value));
  EXPECT_EQ(value, expected_value);
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, GetDrivePrefix) {
  EXPECT_EQ(path::GetDrivePrefix(""), "");
  EXPECT_EQ(path::GetDrivePrefix("\\"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\shr"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\shr"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\\\"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\\\"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\\\dir\\file"), "");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\shr"), "\\\\cpu\\shr");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\shr\\"), "\\\\cpu\\shr");
  EXPECT_EQ(path::GetDrivePrefix("\\\\cpu\\shr\\dir\\file"), "\\\\cpu\\shr");
  EXPECT_EQ(path::GetDrivePrefix("\\dir"), "");
  EXPECT_EQ(path::GetDrivePrefix("C"), "");
  EXPECT_EQ(path::GetDrivePrefix("C:"), "C:");
  EXPECT_EQ(path::GetDrivePrefix("C:\\"), "C:");
  EXPECT_EQ(path::GetDrivePrefix("C:\\dir"), "C:");
  EXPECT_EQ(path::GetDrivePrefix("C:\\dir\\file"), "C:");
}
#endif

TEST_F(PathTest, GetCwd) {
  // Hmm, how to check properly?
  std::string cwd = path::GetCwd();
  EXPECT_FALSE(cwd.empty());
}

TEST_F(PathTest, GetFullPath) {
  // Canonicalize the cwd to match the original directory capitalization.
  std::string cwd = path::GetFullPath(path::GetCwd());
  path::EnsureDoesNotEndWithPathSeparator(&cwd);

  std::string parent_dir = path::DirName(cwd);
  path::EnsureDoesNotEndWithPathSeparator(&parent_dir);

  EXPECT_EQ(path::GetFullPath(""), "");
  EXPECT_EQ(path::GetFullPath("."), cwd);
  EXPECT_EQ(path::GetFullPath(".."), parent_dir);
  EXPECT_EQ(path::GetFullPath("foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath(unicode_test_filepath_), unicode_test_filepath_);
  EXPECT_EQ(path::GetFullPath(kUnicodeTestFileName),
            path::Join(cwd, kUnicodeTestFileName));
  EXPECT_EQ(path::GetFullPath(path::Join(u8"\U0001F964", "foo")),
            path::Join(cwd, u8"\U0001F964", "foo"));

#if PLATFORM_WINDOWS
  std::string current_drive = path::GetDrivePrefix(cwd);
  ASSERT_NE(current_drive, std::string());
  ASSERT_EQ(current_drive.size(), 2) << current_drive;
  EXPECT_EQ(path::GetFullPath(current_drive), cwd);
  EXPECT_EQ(path::GetFullPath(current_drive + "foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath("V:foo"), "V:\\foo");
  EXPECT_EQ(path::GetFullPath("C:\\"), "C:\\");
  EXPECT_EQ(path::GetFullPath("C:\\foo"), "C:\\foo");
  EXPECT_EQ(path::GetFullPath("C:\\foo\\bar"), "C:\\foo\\bar");
  EXPECT_EQ(path::GetFullPath("foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath(".\\foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath(".\\.\\foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath("..\\foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath("..\\.\\foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath(".\\..\\foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath("foo\\.\\bar"), path::Join(cwd, "foo", "bar"));
  EXPECT_EQ(path::GetFullPath("foo\\..\\bar"), path::Join(cwd, "bar"));
  // It is possible to create directories with trailing spaces via APIs, but
  // other Windows APIs are trimming such spaces, most notably GetFullPathNameW.
  EXPECT_EQ(path::GetFullPath("trailing space "),
            path::Join(cwd, "trailing space "));
  EXPECT_EQ(path::GetFullPath(current_drive + "trailing space "),
            path::Join(cwd, "trailing space "));
  EXPECT_EQ(path::GetFullPath(current_drive + "\\trailing space "),
            current_drive + "\\trailing space ");
  EXPECT_EQ(path::GetFullPath("V:trailing space "), "V:\\trailing space ");
#else
  EXPECT_EQ(path::GetFullPath("/"), "/");
  EXPECT_EQ(path::GetFullPath("/tmp"), "/tmp");
  EXPECT_EQ(path::GetFullPath("/tmp/foo"), "/tmp/foo");
  EXPECT_EQ(path::GetFullPath("/tmp/foo/bar"), "/tmp/foo/bar");
  EXPECT_EQ(path::GetFullPath("./foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath("././foo"), path::Join(cwd, "foo"));
  EXPECT_EQ(path::GetFullPath("../foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath(".././foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath("./../foo"), path::Join(parent_dir, "foo"));
  EXPECT_EQ(path::GetFullPath("foo/./bar"), path::Join(cwd, "foo", "bar"));
  EXPECT_EQ(path::GetFullPath("foo/../bar"), path::Join(cwd, "bar"));
#endif
}

TEST_F(PathTest, IsAbsolute) {
  EXPECT_FALSE(path::IsAbsolute(""));
  EXPECT_FALSE(path::IsAbsolute("."));
  EXPECT_FALSE(path::IsAbsolute(".."));
  EXPECT_FALSE(path::IsAbsolute("foo"));
  EXPECT_TRUE(path::IsAbsolute(unicode_test_filepath_));
  EXPECT_FALSE(path::IsAbsolute(kUnicodeTestFileName));
  EXPECT_FALSE(path::IsAbsolute(path::Join(u8"\U0001F964", "foo")));

#if PLATFORM_WINDOWS
  EXPECT_FALSE(path::IsAbsolute("C:"));
  EXPECT_FALSE(path::IsAbsolute("C:foo"));
  EXPECT_FALSE(path::IsAbsolute("V:foo"));
  EXPECT_TRUE(path::IsAbsolute("C:\\"));
  EXPECT_TRUE(path::IsAbsolute("C:\\foo"));
  EXPECT_TRUE(path::IsAbsolute("C:\\foo\\bar"));
  EXPECT_TRUE(path::IsAbsolute("V:\\"));
  EXPECT_TRUE(path::IsAbsolute("V:\\foo"));
  EXPECT_TRUE(path::IsAbsolute("V:\\foo\\bar"));
  EXPECT_FALSE(path::IsAbsolute("foo"));
  EXPECT_FALSE(path::IsAbsolute(".\\foo"));
  EXPECT_FALSE(path::IsAbsolute(".\\.\\foo"));
  EXPECT_FALSE(path::IsAbsolute("..\\foo"));
  EXPECT_FALSE(path::IsAbsolute("..\\.\\foo"));
  EXPECT_FALSE(path::IsAbsolute(".\\..\\foo"));
  EXPECT_FALSE(path::IsAbsolute("foo\\.\\bar"));
  EXPECT_FALSE(path::IsAbsolute("foo\\..\\bar"));
#else
  EXPECT_TRUE(path::IsAbsolute("/"));
  EXPECT_TRUE(path::IsAbsolute("/foo"));
  EXPECT_TRUE(path::IsAbsolute("/foo/bar"));
  EXPECT_FALSE(path::IsAbsolute("foo"));
  EXPECT_FALSE(path::IsAbsolute("foo/bar"));
  EXPECT_FALSE(path::IsAbsolute("."));
  EXPECT_FALSE(path::IsAbsolute("./foo"));
  EXPECT_FALSE(path::IsAbsolute("./foo/bar"));
  EXPECT_FALSE(path::IsAbsolute("foo/./bar"));
  EXPECT_TRUE(path::IsAbsolute("/."));
  EXPECT_TRUE(path::IsAbsolute("/./foo"));
  EXPECT_TRUE(path::IsAbsolute("/./foo/bar"));
  EXPECT_TRUE(path::IsAbsolute("/foo/../bar"));
#endif
}

TEST_F(PathTest, DirName) {
  EXPECT_EQ(path::DirName(""), "");
  EXPECT_EQ(path::DirName("/"), "/");
  EXPECT_EQ(path::DirName("/\\//"), "/\\//");
  EXPECT_EQ(path::DirName("/foo"), "/");
  EXPECT_EQ(path::DirName("/foo/"), "/");
  EXPECT_EQ(path::DirName("//foo/"), "//");
  EXPECT_EQ(path::DirName("foo"), "");
  EXPECT_EQ(path::DirName("foo/"), "");
  EXPECT_EQ(path::DirName("foo/bar"), "foo");
  EXPECT_EQ(path::DirName("foo/bar/"), "foo");
  EXPECT_EQ(path::DirName("foo///bar///"), "foo");
  EXPECT_EQ(path::DirName("foo\\bar"), "foo");
  EXPECT_EQ(path::DirName("foo\\bar\\"), "foo");
  EXPECT_EQ(path::DirName("foo/bar/baz"), "foo/bar");
  EXPECT_EQ(path::DirName("C:\\foo\\bar"), "C:\\foo");
  EXPECT_EQ(path::DirName("C:\\foo\\bar\\"), "C:\\foo");
  EXPECT_EQ(path::DirName("C:/foo/bar"), "C:/foo");
  EXPECT_EQ(path::DirName("C:/foo\\bar"), "C:/foo");
  EXPECT_EQ(path::DirName("C:/foo\\\\bar//"), "C:/foo");
  EXPECT_EQ(path::DirName("C:\\foo\\bar\\baz"), "C:\\foo\\bar");
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, DirNameWinSpecials) {
  // Handling of the drive letter is special on Windows.
  EXPECT_EQ(path::DirName("C:\\"), "C:\\");
  EXPECT_EQ(path::DirName("C:"), "C:");
  EXPECT_EQ(path::DirName("C:foo"), "C:");
  EXPECT_EQ(path::DirName("C:\\foo"), "C:\\");
  EXPECT_EQ(path::DirName("C:\\foo\\"), "C:\\");
  // Network shares are treated like drive prefixes on Windows.
  EXPECT_EQ(path::DirName("\\\\comp\\share"), "\\\\comp\\share");
  EXPECT_EQ(path::DirName("\\\\comp\\share\\"), "\\\\comp\\share\\");
  EXPECT_EQ(path::DirName("\\\\comp\\share\\foo\\bar"), "\\\\comp\\share\\foo");
}
#endif

TEST_F(PathTest, BaseName) {
  EXPECT_EQ(path::BaseName(""), "");
  EXPECT_EQ(path::BaseName("/"), "");
  EXPECT_EQ(path::BaseName("//"), "");
  EXPECT_EQ(path::BaseName("\\"), "");
  EXPECT_EQ(path::BaseName("\\\\"), "");
  EXPECT_EQ(path::BaseName("\\/"), "");
  EXPECT_EQ(path::BaseName("foo/bar"), "bar");
  EXPECT_EQ(path::BaseName("foo/bar/"), "bar");
  EXPECT_EQ(path::BaseName("foo/bar///"), "bar");
  EXPECT_EQ(path::BaseName("foo\\bar"), "bar");
  EXPECT_EQ(path::BaseName("foo\\bar\\"), "bar");
  EXPECT_EQ(path::BaseName("foo\\\\bar\\\\\\"), "bar");
  EXPECT_EQ(path::BaseName("foo/bar\\"), "bar");
  EXPECT_EQ(path::BaseName("foo\\bar/"), "bar");
  EXPECT_EQ(path::BaseName("foo\\bar\\/"), "bar");
  EXPECT_EQ(path::BaseName("foo"), "foo");
  EXPECT_EQ(path::BaseName("foo/"), "foo");
  EXPECT_EQ(path::BaseName("/foo"), "foo");
  EXPECT_EQ(path::BaseName("/foo/"), "foo");
  EXPECT_EQ(path::BaseName("foo/bar/baz"), "baz");
  // Windows drives should probably be handled differently.
  EXPECT_EQ(path::BaseName("C:"), "C:");
  EXPECT_EQ(path::BaseName("C:\\"), "C:");
  EXPECT_EQ(path::BaseName("C:\\foo"), "foo");
  EXPECT_EQ(path::BaseName("C:\\foo\\bar"), "bar");
  EXPECT_EQ(path::BaseName("C:\\foo\\bar\\"), "bar");
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, Join) {
  const char* expected = "C:\\data\\file";
  EXPECT_EQ(path::Join("C:\\data", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data\\", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data/", "file"), "C:\\data/file");
  EXPECT_EQ(path::Join("", "file"), "file");
  EXPECT_EQ(path::Join("C:\\data", ""), "C:\\data");

  expected = "C:\\data\\dir\\file";
  EXPECT_EQ(path::Join("C:\\data", "dir", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data\\", "dir", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data", "dir\\", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data\\", "dir\\", "file"), expected);

  // Don't bother testing all 8.
  expected = "C:\\data\\dir1\\dir2\\file";
  EXPECT_EQ(path::Join("C:\\data", "dir1", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data\\", "dir1", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data", "dir1\\", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("C:\\data", "dir1", "dir2\\", "file"), expected);
}
#endif

#if PLATFORM_LINUX
TEST_F(PathTest, Join) {
  const char* expected = "/data/file";
  EXPECT_EQ(path::Join("/data", "file"), expected);
  EXPECT_EQ(path::Join("/data/", "file"), expected);
  EXPECT_EQ(path::Join("/data\\", "file"), "/data\\file");
  EXPECT_EQ(path::Join("", "file"), "file");
  EXPECT_EQ(path::Join("/data", ""), "/data");

  expected = "/data/dir/file";
  EXPECT_EQ(path::Join("/data", "dir", "file"), expected);
  EXPECT_EQ(path::Join("/data/", "dir", "file"), expected);
  EXPECT_EQ(path::Join("/data", "dir/", "file"), expected);
  EXPECT_EQ(path::Join("/data/", "dir/", "file"), expected);

  // Don't bother testing all 8.
  expected = "/data/dir1/dir2/file";
  EXPECT_EQ(path::Join("/data", "dir1", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("/data/", "dir1", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("/data", "dir1/", "dir2", "file"), expected);
  EXPECT_EQ(path::Join("/data", "dir1", "dir2/", "file"), expected);
}
#endif

#if PLATFORM_WINDOWS
TEST_F(PathTest, Append) {
  const char* expected = "C:\\data\\file";

  std::string path = "C:\\data";
  path::Append(&path, "file");
  EXPECT_EQ(path, expected);

  path = "C:\\data\\";
  path::Append(&path, "file");
  EXPECT_EQ(path, expected);

  path = "C:\\data/";
  path::Append(&path, "file");
  EXPECT_EQ(path, "C:\\data/file");

  path = "";
  path::Append(&path, "file");
  EXPECT_EQ(path, "file");

  path = "C:\\data";
  path::Append(&path, "");
  EXPECT_EQ(path, "C:\\data");
}
#endif

#if PLATFORM_LINUX
TEST_F(PathTest, Append) {
  const char* expected = "/data/file";

  std::string path = "/data";
  path::Append(&path, "file");
  EXPECT_EQ(path, expected);

  path = "/data/";
  path::Append(&path, "file");
  EXPECT_EQ(path, expected);

  path = "C:\\data\\";
  path::Append(&path, "file");
  EXPECT_EQ(path, "C:\\data\\file");

  path = "";
  path::Append(&path, "file");
  EXPECT_EQ(path, "file");

  path = "/data";
  path::Append(&path, "");
  EXPECT_EQ(path, "/data");
}
#endif

TEST_F(PathTest, AppendUnix) {
  const char* expected = "/data/file";

  std::string path = "/data";
  path::AppendUnix(&path, "file");
  EXPECT_EQ(path, expected);

  path = "/data/";
  path::AppendUnix(&path, "file");
  EXPECT_EQ(path, expected);

  path = "/data\\";
  path::AppendUnix(&path, "file");
  EXPECT_EQ(path, "/data\\file");

  path = "";
  path::AppendUnix(&path, "file");
  EXPECT_EQ(path, "file");

  path = "/data";
  path::AppendUnix(&path, "");
  EXPECT_EQ(path, "/data");
}

TEST_F(PathTest, EndsWithPathSeparator) {
  EXPECT_FALSE(path::EndsWithPathSeparator(""));
  EXPECT_FALSE(path::EndsWithPathSeparator("C:\\data"));
  EXPECT_FALSE(path::EndsWithPathSeparator("/data"));
  EXPECT_TRUE(path::EndsWithPathSeparator("C:\\data\\"));
  EXPECT_TRUE(path::EndsWithPathSeparator("C:\\data/"));
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, EnsureEndsWithPathSeparator) {
  std::string path = "C:\\data";
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data\\");
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data\\");

  path = "/data/";
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "/data/");
}
#endif

#if PLATFORM_LINUX
TEST_F(PathTest, EnsureEndsWithPathSeparator) {
  std::string path = "/data";
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "/data/");
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "/data/");

  path = "C:\\data\\";
  path::EnsureEndsWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data\\");
}
#endif

TEST_F(PathTest, EnsureDoesNotEndWithPathSeparator) {
  std::string path = "C:\\data\\\\";
  path::EnsureDoesNotEndWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data");
  path = "C:\\data\\";
  path::EnsureDoesNotEndWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data");
  path::EnsureDoesNotEndWithPathSeparator(&path);
  EXPECT_EQ(path, "C:\\data");
}

#if PLATFORM_WINDOWS
TEST_F(PathTest, FixPathSeparators) {
  std::string path = "dir1/dir2/file";
  path::FixPathSeparators(&path);
  EXPECT_EQ(path, "dir1\\dir2\\file");
}
#endif

#if PLATFORM_LINUX
TEST_F(PathTest, FixPathSeparators) {
  std::string path = "dir1\\dir2\\file";
  path::FixPathSeparators(&path);
  EXPECT_EQ(path, "dir1/dir2/file");
}
#endif

TEST_F(PathTest, ToUnix) {
  EXPECT_EQ(path::ToUnix(""), "");
  EXPECT_EQ(path::ToUnix("/"), "/");
  EXPECT_EQ(path::ToUnix("/foo/"), "/foo/");
  EXPECT_EQ(path::ToUnix("\\"), "/");
  EXPECT_EQ(path::ToUnix("\\foo"), "/foo");
  EXPECT_EQ(path::ToUnix("\\foo\\"), "/foo/");
  EXPECT_EQ(path::ToUnix("\\\\foo\\bar"), "//foo/bar");
  EXPECT_EQ(path::ToUnix("C:\\Windows\\"), "C:/Windows/");
  EXPECT_EQ(path::ToUnix("C:\\foo/bar"), "C:/foo/bar");
  EXPECT_EQ(path::ToUnix("C:/foo\\bar"), "C:/foo/bar");
}

#if PLATFORM_LINUX
TEST_F(PathTest, ToNative) {
  EXPECT_EQ(path::ToNative(""), "");
  EXPECT_EQ(path::ToNative("/"), "/");
  EXPECT_EQ(path::ToNative("/foo/"), "/foo/");
  EXPECT_EQ(path::ToNative("\\"), "/");
  EXPECT_EQ(path::ToNative("\\foo"), "/foo");
  EXPECT_EQ(path::ToNative("\\foo\\"), "/foo/");
  EXPECT_EQ(path::ToNative("\\\\foo\\bar"), "//foo/bar");
  EXPECT_EQ(path::ToNative("C:\\Windows\\"), "C:/Windows/");
  EXPECT_EQ(path::ToNative("C:\\foo/bar"), "C:/foo/bar");
  EXPECT_EQ(path::ToNative("C:/foo\\bar"), "C:/foo/bar");
}
#endif

#if PLATFORM_WINDOWS
TEST_F(PathTest, ToNative) {
  EXPECT_EQ(path::ToNative(""), "");
  EXPECT_EQ(path::ToNative("/"), "\\");
  EXPECT_EQ(path::ToNative("/foo/"), "\\foo\\");
  EXPECT_EQ(path::ToNative("\\"), "\\");
  EXPECT_EQ(path::ToNative("\\foo"), "\\foo");
  EXPECT_EQ(path::ToNative("\\foo\\"), "\\foo\\");
  EXPECT_EQ(path::ToNative("\\\\foo\\bar"), "\\\\foo\\bar");
  EXPECT_EQ(path::ToNative("C:/Windows/"), "C:\\Windows\\");
  EXPECT_EQ(path::ToNative("C:\\foo/bar"), "C:\\foo\\bar");
  EXPECT_EQ(path::ToNative("C:/foo\\bar"), "C:\\foo\\bar");
}
#endif

TEST_F(PathTest, OpenFile) {
  absl::StatusOr<FILE*> file = path::OpenFile(test_filepath_, "r");
  ASSERT_OK(file);
  EXPECT_TRUE(*file);
  char line[256] = {0};
  EXPECT_TRUE(fgets(line, sizeof(line) - 1, *file));
  EXPECT_STREQ(line, kTestText);
  fclose(*file);
}

TEST_F(PathTest, OpenFileFails) {
  absl::StatusOr<FILE*> status = path::OpenFile("does_not_exist", "r");
  EXPECT_TRUE(absl::IsNotFound(status.status()));
  EXPECT_TRUE(absl::StrContains(status.status().message(),
                                "Failed to open file 'does_not_exist':"));

  status = path::OpenFile("", "r");
  EXPECT_TRUE(
      absl::StrContains(status.status().message(), "Failed to open file '':"));
}

TEST_F(PathTest, OpenFile_Unicode) {
  absl::StatusOr<FILE*> file = path::OpenFile(unicode_test_filepath_, "r");
  ASSERT_OK(file);
  EXPECT_TRUE(*file);
  fclose(*file);
}

void PathTest::CheckStreamReadFileContents(
    size_t buffer_size, std::vector<std::string> expected_data) const {
  int index = 0;
  auto handler = [&expected_data, &index](const void* data, size_t data_size) {
    // ASSERTs would be better here, but they change the return type.
    EXPECT_LT(index, expected_data.size());
    if (static_cast<size_t>(index) >= expected_data.size()) {
      return MakeStatus("Bad index");
    }

    EXPECT_EQ(data_size, expected_data[index].size());
    if (data_size != expected_data[index].size()) {
      return MakeStatus("Bad data_size");
    }
    EXPECT_EQ(memcmp(data, expected_data[index].data(), data_size), 0);
    index++;
    return absl::OkStatus();
  };

  absl::StatusOr<FILE*> file = path::OpenFile(test_filepath_, "rb");
  ASSERT_OK(file);
  EXPECT_OK(path::StreamReadFileContents(*file, buffer_size, handler));
  fclose(*file);
}

std::vector<PathTest::File> PathTest::SearchFiles(const std::string& pattern,
                                                  bool recursive) {
  std::vector<File> files;

  auto handler = [&files](const std::string& dir, const std::string& filename,
                          int64_t modified_time, uint64_t size,
                          bool is_directory) {
    files.push_back({dir, filename, is_directory});
    if (is_directory) {
      EXPECT_EQ(size, 0);
    } else {
      EXPECT_GT(size, 0);
    }
    EXPECT_GT(modified_time, 0);
    return absl::OkStatus();
  };

  EXPECT_OK(path::SearchFiles(pattern, recursive, handler));

  // Linux and Windows yield a slightly different order, so sort first.
  std::sort(files.begin(), files.end(), [](const File& a, const File& b) {
    return a.filename_ < b.filename_;
  });

  return files;
}

TEST_F(PathTest, SearchFiles_NonRecursiveTrailingSeparator) {
  std::vector<File> files = SearchFiles(search_dir_, false);
#if PLATFORM_WINDOWS
  // Windows does not list elements in a directory unless they are globbed.
  EXPECT_TRUE(files.empty());
#else
  // We get all elements within the search_dir_, but not beyond.
  ASSERT_EQ(files.size(), 3);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, search_dir_);
  EXPECT_EQ(files[1].Path(), test_filepath_);
  EXPECT_FALSE(files[1].is_directory_);

  EXPECT_EQ(files[2].dir_, search_dir_);
  EXPECT_EQ(files[2].Path(), unicode_test_filepath_);
  EXPECT_FALSE(files[2].is_directory_);
#endif
}

TEST_F(PathTest, SearchFiles_NonRecursiveNoTrailingSeparator) {
  std::string search_dir(search_dir_.substr(0, search_dir_.size() - 1));
  std::vector<File> files = SearchFiles(search_dir, false);
#if PLATFORM_WINDOWS
  // Windows does not list elements in a directory unless they are globbed.
  EXPECT_TRUE(files.empty());
#else
  // We get all elements within the search_dir_, but not beyond.
  ASSERT_EQ(files.size(), 3);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, search_dir_);
  EXPECT_EQ(files[1].Path(), test_filepath_);
  EXPECT_FALSE(files[1].is_directory_);

  EXPECT_EQ(files[2].dir_, search_dir_);
  EXPECT_EQ(files[2].Path(), unicode_test_filepath_);
  EXPECT_FALSE(files[2].is_directory_);
#endif
}

TEST_F(PathTest, SearchFiles_RecursiveTrailingSeparator) {
  std::vector<File> files = SearchFiles(search_dir_, true);

  ASSERT_EQ(files.size(), 4);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, subdir_);
  EXPECT_EQ(files[1].Path(), subdir_filepath_);
  EXPECT_FALSE(files[1].is_directory_);

  EXPECT_EQ(files[2].dir_, search_dir_);
  EXPECT_EQ(files[2].Path(), test_filepath_);
  EXPECT_FALSE(files[2].is_directory_);

  EXPECT_EQ(files[3].dir_, search_dir_);
  EXPECT_EQ(files[3].Path(), unicode_test_filepath_);
  EXPECT_FALSE(files[3].is_directory_);
}

#if PLATFORM_WINDOWS
// Windows behaves differently when the search path does not end with a
// directory separator.
TEST_F(PathTest, SearchFiles_RecursiveNoTrailingSeparator) {
  std::string search_dir(search_dir_.substr(0, search_dir_.size() - 1));
  std::vector<File> files = SearchFiles(search_dir, true);

  ASSERT_EQ(files.size(), 5);

  EXPECT_EQ(files[0].dir_, base_dir_);
  EXPECT_EQ(files[0].Path(), search_dir);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, search_dir_);
  EXPECT_EQ(files[1].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[1].is_directory_);

  EXPECT_EQ(files[2].dir_, subdir_);
  EXPECT_EQ(files[2].Path(), subdir_filepath_);
  EXPECT_FALSE(files[2].is_directory_);

  EXPECT_EQ(files[3].dir_, search_dir_);
  EXPECT_EQ(files[3].Path(), test_filepath_);

  EXPECT_EQ(files[4].dir_, search_dir_);
  EXPECT_EQ(files[4].Path(), unicode_test_filepath_);
}
#endif

#if PLATFORM_WINDOWS
// Globbing is only supported on Windows.
TEST_F(PathTest, SearchFiles_NonRecursiveStar) {
  std::string search_dir(search_dir_ + "*");
  std::vector<File> files = SearchFiles(search_dir, false);

  ASSERT_EQ(files.size(), 3);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, search_dir_);
  EXPECT_EQ(files[1].Path(), test_filepath_);

  EXPECT_EQ(files[2].dir_, search_dir_);
  EXPECT_EQ(files[2].Path(), unicode_test_filepath_);
}
#endif

#if PLATFORM_WINDOWS
// Globbing is only supported on Windows.
TEST_F(PathTest, SearchFiles_RecursiveStar) {
  std::string search_dir(search_dir_ + "*");
  std::vector<File> files = SearchFiles(search_dir, true);

  ASSERT_EQ(files.size(), 4);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), subdir_fulldirpath_);
  EXPECT_TRUE(files[0].is_directory_);

  EXPECT_EQ(files[1].dir_, subdir_);
  EXPECT_EQ(files[1].Path(), subdir_filepath_);
  EXPECT_FALSE(files[1].is_directory_);

  EXPECT_EQ(files[2].dir_, search_dir_);
  EXPECT_EQ(files[2].Path(), test_filepath_);

  EXPECT_EQ(files[3].dir_, search_dir_);
  EXPECT_EQ(files[3].Path(), unicode_test_filepath_);
}
#endif

#if PLATFORM_WINDOWS
// Globbing is only supported on Windows.
TEST_F(PathTest, SearchFiles_Pattern) {
  const std::string pattern = path::Join(search_dir_, "*t.t*");
  std::vector<File> files = SearchFiles(pattern, false);

  ASSERT_EQ(files.size(), 1);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), test_filepath_);
}
#endif

#if PLATFORM_WINDOWS
// Globbing is only supported on Windows.
TEST_F(PathTest, SearchFiles_PatternRecursive) {
  const std::string pattern = path::Join(search_dir_, "*file*");
  std::vector<File> files = SearchFiles(pattern, true);

  ASSERT_EQ(files.size(), 1);

  EXPECT_EQ(files[0].dir_, subdir_);
  EXPECT_EQ(files[0].Path(), subdir_filepath_);
}
#endif

TEST_F(PathTest, SearchFiles_LastPartIsFile) {
  // If "something" is a file in path\to\something, return that file.
  std::vector<File> files = SearchFiles(unicode_test_filepath_, false);

  ASSERT_EQ(files.size(), 1);

  EXPECT_EQ(files[0].dir_, search_dir_);
  EXPECT_EQ(files[0].Path(), unicode_test_filepath_);
}

TEST_F(PathTest, SearchFiles_LastPartIsDir) {
  // If "something" is a dir in path\to\something, return nothing for a
  // non-recursive case.
  std::string dir = subdir_;
  path::EnsureDoesNotEndWithPathSeparator(&dir);
  std::vector<File> files = SearchFiles(dir, false);
#if PLATFORM_WINDOWS
  // Windows does not list elements in a directory unless they are globbed.
  EXPECT_TRUE(files.empty());
#else
  // We get the single element within the subdir_..
  ASSERT_EQ(files.size(), 1);

  std::string got_dir = files[0].dir_;
  path::EnsureDoesNotEndWithPathSeparator(&got_dir);
  EXPECT_EQ(got_dir, subdir_fulldirpath_);
  EXPECT_EQ(files[0].Path(), subdir_filepath_);
#endif
}

TEST_F(PathTest, SearchFiles_LastPartIsDir_Recursive) {
  std::string dir = search_dir_;
  path::EnsureDoesNotEndWithPathSeparator(&dir);
  std::vector<File> files = SearchFiles(dir, true);

#if PLATFORM_WINDOWS
  // 2 folders: search and subdir + 3 files: test.txt, u8"unicode test 🥤.txt,
  // and subdir\\subdir_file.txt
  ASSERT_EQ(files.size(), 5);
#else
  // 1 folder: subdir + 3 files: test.txt, u8"unicode test 🥤.txt,
  // and subdir\\subdir_file.txt
  ASSERT_EQ(files.size(), 4);
#endif
}

TEST_F(PathTest, SearchFiles_HandlerFails) {
  auto handler = [](const std::string&, const std::string&, int64_t, uint64_t,
                    bool) { return MakeStatus("bad"); };
  EXPECT_NOT_OK(path::SearchFiles(base_dir_, true, handler));
}

TEST_F(PathTest, StreamReadFileContents_PiecewiseRead) {
  // Check different buffer sizes.
  CheckStreamReadFileContents(2, {"te", "st", " t", "ex", "t", ""});
  CheckStreamReadFileContents(3, {"tes", "t t", "ext", ""});
  CheckStreamReadFileContents(9, {kTestText, ""});
  CheckStreamReadFileContents(10, {kTestText, ""});
  CheckStreamReadFileContents(65536, {kTestText, ""});
}

TEST_F(PathTest, StreamReadFileContents_HandlerFails) {
  auto handler = [](const void*, size_t) { return MakeStatus("invalid"); };
  absl::StatusOr<FILE*> file = path::OpenFile(test_filepath_, "rb");
  ASSERT_OK(file);
  EXPECT_NOT_OK(
      path::StreamReadFileContents(*file, /*buffer_size=*/32769, handler));
  fclose(*file);
}

TEST_F(PathTest, StreamReadFileContents_EndOfFileIndicatorIsBuffered) {
  absl::StatusOr<FILE*> file = path::OpenFile(test_filepath_, "rb");
  ASSERT_OK(file);
  auto handler = [file = *file](const void* data, size_t) {
    if (!data) {
      return absl::OkStatus();
    }
    // This operation resets feof(). It tests whether the calling code properly
    // buffers feof() before calling the handler.
    if (!feof(file)) {
      return MakeStatus("feof() not set");
    }
    int pos = ftell(file);
    fseek(file, 0, SEEK_SET);
    char c;
    fread(&c, 1, 1, file);
    fseek(file, pos, SEEK_SET);
    if (feof(file)) {
      return MakeStatus("feof() set");
    }
    return absl::OkStatus();
  };
  EXPECT_OK(
      path::StreamReadFileContents(*file, /*buffer_size=*/32769, handler));
  fclose(*file);
}

TEST_F(PathTest, StreamWriteFileContents_PiecewiseWrite) {
  // Create handler that writes |to_write| byte by byte.
  std::string to_write = "test abc";
  size_t index = 0;
  auto handler = [&index, &to_write](const void** data, size_t* size) {
    const bool done = index >= to_write.size();
    *data = !done ? &to_write[index] : nullptr;
    *size = !done ? 1 : 0;
    index++;
    return absl::OkStatus();
  };

  // Write to a temporary file.
  std::FILE* file = std::tmpfile();
  ASSERT_TRUE(file != nullptr);
  EXPECT_OK(path::StreamWriteFileContents(file, handler));

  // Verify contents.
  rewind(file);
  char line[256] = {0};
  EXPECT_TRUE(fgets(line, sizeof(line) - 1, file));
  EXPECT_STREQ(line, "test abc");
  fclose(file);
}

TEST_F(PathTest, StreamWriteFileContents_HandlerFails) {
  auto handler = [](const void**, size_t*) { return MakeStatus("invalid"); };
  std::FILE* file = std::tmpfile();
  ASSERT_TRUE(file != nullptr);
  EXPECT_NOT_OK(path::StreamWriteFileContents(file, handler));
  fclose(file);
}

TEST_F(PathTest, ReadFile_Success) {
  Buffer data;
  EXPECT_OK(path::ReadFile(test_filepath_, &data));
  Buffer want = {'t', 'e', 's', 't', ' ', 't', 'e', 'x', 't'};
  EXPECT_EQ(data, want);
}

TEST_F(PathTest, ReadFile_DoesNotExist) {
  Buffer data;
  absl::Status status = path::ReadFile("does_not_exist", &data);
  EXPECT_TRUE(absl::IsNotFound(status));
}

TEST_F(PathTest, ReadWholeFileWithOffsetLen_Success) {
  uint8_t data[9];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile(test_filepath_, data, 0, 9);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(*bytes_read, 9);
  std::vector<uint8_t> want = {'t', 'e', 's', 't', ' ', 't', 'e', 'x', 't'};
  EXPECT_TRUE(std::equal(std::begin(data), std::end(data), std::begin(want)));
}

TEST_F(PathTest, ReadFilePartWithOffsetLen_Success) {
  uint8_t data[3];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile(test_filepath_, data, 3, 3);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(*bytes_read, 3);
  std::vector<uint8_t> want = {'t', ' ', 't'};
  EXPECT_TRUE(std::equal(std::begin(data), std::end(data), std::begin(want)));
}

TEST_F(PathTest, ReadFileWithZeroLen_Success) {
  uint8_t data[9];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile(test_filepath_, data, 0, 0);
  ASSERT_OK(bytes_read);
  EXPECT_EQ(bytes_read.value(), 0);
}

TEST_F(PathTest, ReadFileWithWrongOffset_Success) {
  uint8_t data[9];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile(test_filepath_, data, 20, 5);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(*bytes_read, 0);
}

TEST_F(PathTest, ReadFileWithLargeLen_Success) {
  uint8_t data[9];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile(test_filepath_, data, 0, 40);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(*bytes_read, 9);
  std::vector<uint8_t> want = {'t', 'e', 's', 't', ' ', 't', 'e', 'x', 't'};
  EXPECT_TRUE(std::equal(std::begin(data), std::end(data), std::begin(want)));
}

TEST_F(PathTest, ReadFileWithOffsetLen_DoesNotExist) {
  uint8_t data[9];
  absl::StatusOr<size_t> bytes_read =
      path::ReadFile("does_not_exist", data, 0, 5);
  EXPECT_TRUE(absl::IsNotFound(bytes_read.status()));
}

TEST_F(PathTest, ReadFileString_Success) {
  absl::StatusOr<std::string> data = path::ReadFile(test_filepath_);
  ASSERT_OK(data);
  EXPECT_EQ(*data, "test text");
}

TEST_F(PathTest, ReadFileString_DoesNotExist) {
  EXPECT_ERROR(NotFound, path::ReadFile("does_not_exist"));
}

TEST_F(PathTest, ReadAllLines_DoesNotExist) {
  std::vector<std::string> lines;
  absl::Status status =
      path::ReadAllLines("does_not_exist", &lines, path::ReadFlags::kNone);
  EXPECT_TRUE(absl::IsNotFound(status));
}

TEST_F(PathTest, ReadAllLines_Success) {
  EXPECT_NO_FATAL_FAILURE(CheckReadAllLines(
      path::ReadFlags::kNone, {"", "line", "trailing whitespace  \t", "", "",
                               " \t leading\twhitespace", ""}));

  EXPECT_NO_FATAL_FAILURE(CheckReadAllLines(
      path::ReadFlags::kRemoveEmpty,
      {"line", "trailing whitespace  \t", " \t leading\twhitespace"}));

  EXPECT_NO_FATAL_FAILURE(CheckReadAllLines(
      path::ReadFlags::kTrimWhitespace,
      {"", "line", "trailing whitespace", "", "", "leading\twhitespace", ""}));

  EXPECT_NO_FATAL_FAILURE(CheckReadAllLines(
      path::ReadFlags::kRemoveEmpty | path::ReadFlags::kTrimWhitespace,
      {"line", "trailing whitespace", "leading\twhitespace"}));
}

TEST_F(PathTest, WriteFile_Success) {
  const std::string& path = tmp_path_1_;
  Buffer want = {'1', '2', '3'};
  EXPECT_OK(path::WriteFile(path, want));
  Buffer data;
  EXPECT_OK(path::ReadFile(path, &data));
  EXPECT_EQ(data, want);
}

TEST_F(PathTest, WriteFile_PermissionDenied) {
  const std::string& path = tmp_path_1_;
  Buffer want = {'1', '2', '3'};
  EXPECT_OK(path::WriteFile(path, want));

  // Check if overwrite works.
  EXPECT_OK(path::WriteFile(path, want));

  // Make it read-only and try to write again.
  RemoveWritePermission(path);
  absl::Status status = path::WriteFile(path, want);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), "Permission denied"));
}

// Creating symlinks require administrator permissions
#ifdef ENABLE_TESTS_REQUIRING_ADMIN_PERMISSIONS
TEST_F(PathTest, Symlink) {
  std::string file_link = path::Join(base_dir_, u8"file_linkÜäЇє");
  std::string target = u8"some_targetŨŴώ";
  ASSERT_OK(path::CreateSymlink(target, file_link, false));
  absl::StatusOr<std::string> symlink_target =
      path::GetSymlinkTarget(file_link);
  ASSERT_TRUE(symlink_target.ok());
  ASSERT_EQ(symlink_target.value(), target);

  std::string dir_link = path::Join(base_dir_, u8"dir_linkЂЄϸ");
  target = u8"some_dir_ϴϭ Ⱦ_target";
  ASSERT_OK(path::CreateSymlink(target, dir_link, true));
  symlink_target = path::GetSymlinkTarget(dir_link);
  ASSERT_OK(symlink_target);
  ASSERT_EQ(symlink_target.value(), target);

  symlink_target = path::GetSymlinkTarget("does not exist");
  ASSERT_NOT_OK(symlink_target);
}
#endif  // ENABLE_TESTS_REQUIRING_ADMIN_PERMISSIONS

TEST_F(PathTest, DirExists) {
  EXPECT_TRUE(path::DirExists(base_dir_));
  EXPECT_FALSE(path::DirExists(test_filepath_));
  EXPECT_FALSE(path::DirExists("does_not_exist"));
}

TEST_F(PathTest, FileExists) {
  EXPECT_FALSE(path::FileExists(base_dir_));
  EXPECT_TRUE(path::FileExists(test_filepath_));
  EXPECT_FALSE(path::FileExists("does_not_exist"));
}

TEST_F(PathTest, Exists) {
  EXPECT_TRUE(path::Exists(base_dir_));
  EXPECT_TRUE(path::Exists(test_filepath_));
  EXPECT_FALSE(path::Exists("does_not_exist"));
}

TEST_F(PathTest, CreateDir) {
  std::string dir = path::Join(tmp_dir_, "createdir_test");
  std::string unicode_dir = path::Join(dir, u8"\U0001F964\U0001F964\U0001F964");
  path::RemoveDirRec(dir).IgnoreError();
  EXPECT_NOT_OK(path::CreateDir(path::Join(dir, "subdir")));
  EXPECT_OK(path::CreateDir(dir));
  EXPECT_OK(path::CreateDir(dir));
  EXPECT_OK(path::CreateDir(unicode_dir));
  EXPECT_TRUE(path::DirExists(unicode_dir));
  // TODO: Uncomment once everything compiles on VS 2019.
  // I assume the C++17 standard implemented in VS 2019 does report an error
  // when creating a dir over an existing file, whereas VS 2017 does not.
  // See http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2019/p1164r1.pdf.
  // EXPECT_NOT_OK(path::CreateDir(test_filepath_));
  path::RemoveDirRec(dir).IgnoreError();
}

TEST_F(PathTest, CreateDirRec) {
  std::string dir = path::Join(tmp_dir_, "createdir_test", "subdir");
  std::string unicode_dir = path::Join(dir, u8"\U0001F964\U0001F964\U0001F964");
  path::RemoveDirRec(dir).IgnoreError();
  EXPECT_OK(path::CreateDirRec(dir));
  EXPECT_OK(path::CreateDirRec(dir));
  EXPECT_OK(path::CreateDirRec(unicode_dir));
  EXPECT_TRUE(path::DirExists(unicode_dir));
  // TODO: Uncomment once everything compiles on VS 2019. See above.
  // EXPECT_NOT_OK(path::CreateDirRec(test_filepath_));
  path::RemoveDirRec(dir).IgnoreError();
}

TEST_F(PathTest, RenameFile_Success) {
  EXPECT_OK(path::WriteFile(tmp_path_1_, "x"));
  EXPECT_TRUE(path::Exists(tmp_path_1_));
  EXPECT_FALSE(path::Exists(tmp_path_2_));

  EXPECT_OK(path::RenameFile(tmp_path_1_, tmp_path_2_));

  EXPECT_FALSE(path::Exists(tmp_path_1_));
  EXPECT_TRUE(path::Exists(tmp_path_2_));
}

TEST_F(PathTest, RenameFile_DoesNotExist) {
  absl::Status status = path::RenameFile(tmp_path_1_, tmp_path_2_);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), "No such file"));
}

TEST_F(PathTest, RenameFile_Exists) {
  EXPECT_OK(path::WriteFile(tmp_path_1_, "x"));
  EXPECT_OK(path::WriteFile(tmp_path_2_, "y"));

  absl::Status status = path::RenameFile(tmp_path_1_, tmp_path_2_);
#if PLATFORM_WINDOWS
  // Windows returns an error if the target exists.
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), "File exists"));
#else
  // Linux atomically replaces the target with the existing source file.
  EXPECT_OK(status);
#endif
}

TEST_F(PathTest, CopyFileRec_Success) {
  const std::string source_path_1_ = path::Join(base_dir_, kUnicodeText);
  EXPECT_OK(path::WriteFile(source_path_1_, "x"));
  EXPECT_TRUE(path::Exists(source_path_1_));
  EXPECT_FALSE(path::Exists(tmp_path_2_));

  EXPECT_OK(path::CopyFileRec(source_path_1_, tmp_path_2_));

  EXPECT_TRUE(path::Exists(source_path_1_));
  EXPECT_TRUE(path::Exists(tmp_path_2_));
}

TEST_F(PathTest, CopyFileRec_DoesNotExist) {
  EXPECT_ERROR(Internal, path::CopyFileRec(tmp_path_1_, tmp_path_2_));
}

TEST_F(PathTest, CopyFileRec_Exists) {
  EXPECT_OK(path::WriteFile(tmp_path_1_, "x"));
  EXPECT_OK(path::WriteFile(tmp_path_2_, "y"));

  EXPECT_OK(path::CopyFileRec(tmp_path_1_, tmp_path_2_));
  EXPECT_TRUE(path::Exists(tmp_path_1_));
  EXPECT_TRUE(path::Exists(tmp_path_2_));

  Buffer new_data;
  EXPECT_OK(path::ReadFile(tmp_path_2_, &new_data));
  EXPECT_EQ(new_data, Buffer{'x'});
}

TEST_F(PathTest, RemoveFile_Success) {
  EXPECT_OK(path::WriteFile(tmp_path_1_, "x"));
  EXPECT_TRUE(path::Exists(tmp_path_1_));

  EXPECT_OK(path::RemoveFile(tmp_path_1_));

  EXPECT_FALSE(path::Exists(tmp_path_1_));
}

TEST_F(PathTest, RemoveFile_NonExistentSuccess) {
  EXPECT_FALSE(path::Exists(tmp_path_1_));
  EXPECT_OK(path::RemoveFile(tmp_path_1_));
}

TEST_F(PathTest, RemoveFile_ReadOnly) {
  EXPECT_OK(path::WriteFile(tmp_path_1_, "x"));
  RemoveWritePermission(tmp_path_1_);

  absl::Status status = path::RemoveFile(tmp_path_1_);

#if PLATFORM_WINDOWS
  // Windows requires write permissions for a file to remove it.
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), "Permission denied"));
#else
  // Linux requires write permissions to the directory to remove a file.
  EXPECT_OK(status);
#endif
}

TEST_F(PathTest, GetStats_File) {
  path::Stats stats;
  EXPECT_OK(path::GetStats(test_filepath_, &stats));
  EXPECT_EQ(stats.mode & path::MODE_IFDIR, 0);
  EXPECT_NE(stats.modified_time, 0);
  EXPECT_EQ(stats.size, 9);
}

TEST_F(PathTest, GetStats_UnicodeFile) {
  path::Stats stats;
  EXPECT_OK(path::GetStats(unicode_test_filepath_, &stats));
  EXPECT_EQ(stats.mode & path::MODE_IFDIR, 0);
  EXPECT_NE(stats.modified_time, 0);
  EXPECT_EQ(stats.size, 12);
}

TEST_F(PathTest, GetStats_Dir) {
  path::Stats stats;
  EXPECT_OK(path::GetStats(base_dir_, &stats));
  EXPECT_NE(stats.mode & path::MODE_IFDIR, 0);
  EXPECT_NE(stats.modified_time, 0);
}

TEST_F(PathTest, GetStats_DoesNotExist) {
  path::Stats stats;
  absl::Status status = path::GetStats("does_not_exist", &stats);
  EXPECT_TRUE(absl::IsNotFound(status));
}

TEST_F(PathTest, GetFileSize) {
  uint64_t size;
  EXPECT_OK(path::FileSize(test_filepath_, &size));
  EXPECT_EQ(size, 9);
}

TEST_F(PathTest, ReplaceFile) {
  const std::string& old_file = tmp_path_1_;
  const std::string& new_file = tmp_path_2_;

  EXPECT_OK(path::WriteFile(old_file, "a"));
  EXPECT_OK(path::WriteFile(new_file, "b"));

  // Set up a different mode. Files initially have rw permissions.
  RemoveWritePermission(new_file);
  path::Stats old_stats, new_stats;
  EXPECT_OK(path::GetStats(old_file, &old_stats));
  EXPECT_OK(path::GetStats(new_file, &new_stats));
  EXPECT_NE(old_stats.mode, new_stats.mode);

  EXPECT_OK(path::ReplaceFile(old_file, new_file));

  Buffer old_replaced_by_new_data;
  EXPECT_OK(path::ReadFile(old_file, &old_replaced_by_new_data));
  EXPECT_EQ(old_replaced_by_new_data, Buffer{'b'});

  // Mode should match mode from NEW file.
  path::Stats old_replaced_by_new_stats;
  EXPECT_OK(path::GetStats(old_file, &old_replaced_by_new_stats));
  EXPECT_EQ(old_replaced_by_new_stats.mode, new_stats.mode);
}

TEST_F(PathTest, ReplaceFile_OldDoesNotExist) {
  const std::string& old_file = tmp_path_1_;
  const std::string& new_file = tmp_path_2_;

  absl::Status status = path::ReplaceFile(old_file, new_file);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), old_file));
  EXPECT_TRUE(absl::StrContains(status.message(), "No such file"));
}

TEST_F(PathTest, ReplaceFile_NewDoesNotExist) {
  const std::string& old_file = tmp_path_1_;
  const std::string& new_file = tmp_path_2_;

  EXPECT_OK(path::WriteFile(old_file, "a"));

  absl::Status status = path::ReplaceFile(old_file, new_file);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), old_file));
  EXPECT_TRUE(absl::StrContains(status.message(), new_file));
  EXPECT_TRUE(absl::StrContains(status.message(), "No such file"));
  EXPECT_FALSE(path::Exists(new_file));
}

TEST_F(PathTest, ChangeMode_Success) {
  const std::string& path = tmp_path_1_;
  EXPECT_OK(path::WriteFile(path, "a"));
  path::Stats stats;
  EXPECT_OK(path::GetStats(path, &stats));
  EXPECT_NE(stats.mode & path::MODE_IWUSR, 0);
  EXPECT_OK(path::ChangeMode(path, stats.mode & ~path::MODE_IWUSR));
  EXPECT_OK(path::GetStats(path, &stats));
  EXPECT_EQ(stats.mode & path::MODE_IWUSR, 0);
}

TEST_F(PathTest, ChangeMode_DoesNotExist) {
  const std::string& path = tmp_path_1_;
  EXPECT_FALSE(path::Exists(path));

  absl::Status status = path::ChangeMode(path, 0);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::StrContains(status.message(), "No such file"));
}

TEST_F(PathTest, RemoveDirRec_Success) {
  // Create a directory structure.
  std::string dir_path = path::Join(base_dir_, "dirrec\\");
  ASSERT_OK(path::CreateDirRec(dir_path));
  std::string subdir_path = path::Join(dir_path, "subdirrec\\");
  std::string subdir_path2 = path::Join(dir_path, "subdirrec2\\");
  ASSERT_OK(path::CreateDirRec(subdir_path));
  ASSERT_OK(path::CreateDirRec(subdir_path2));
  std::string file_path_1 = path::Join(subdir_path, "file.txt");
  std::string file_path_2 = path::Join(subdir_path2, "file.txt");
  EXPECT_OK(path::WriteFile(file_path_1, "x"));
  EXPECT_OK(path::WriteFile(file_path_2, "x"));
  EXPECT_OK(path::RemoveDirRec(dir_path));
  EXPECT_FALSE(path::Exists(dir_path));
}

TEST_F(PathTest, RemoveDirRec_Fail) {
  EXPECT_OK(path::RemoveDirRec("non_existing_file"));
}

TEST_F(PathTest, PathSeparator) {
#if PLATFORM_WINDOWS
  EXPECT_EQ('\\', path::PathSeparator());
  EXPECT_EQ('/', path::OtherPathSeparator());
#elif PLATFORM_LINUX
  EXPECT_EQ('/', path::PathSeparator());
  EXPECT_EQ('\\', path::OtherPathSeparator());
#endif
}

TEST_F(PathTest, SetFileTime_FileNotFound) {
  constexpr time_t kTimestamp = 1617270506;
  EXPECT_NOT_OK(path::SetFileTime("non_existing_file", kTimestamp));
}

TEST_F(PathTest, SetFileTime_GetFileTime_Success) {
  constexpr time_t kTimestamp = 1617270506;
  EXPECT_OK(path::WriteFile(tmp_path_1_, Buffer()));
  EXPECT_OK(path::SetFileTime(tmp_path_1_, kTimestamp));
  time_t mtime;
  EXPECT_OK(path::GetFileTime(tmp_path_1_, &mtime));
  EXPECT_EQ(mtime, kTimestamp);
}

TEST_F(PathTest, GetFileTime_File) {
  time_t mtime;
  EXPECT_OK(path::GetFileTime(test_filepath_, &mtime));
  EXPECT_NE(mtime, 0);
}

TEST_F(PathTest, GetFileTime_UnicodeFile) {
  time_t mtime;
  EXPECT_OK(path::GetFileTime(unicode_test_filepath_, &mtime));
  EXPECT_NE(mtime, 0);
}

TEST_F(PathTest, GetFileTime_Dir) {
  time_t mtime;
  EXPECT_OK(path::GetFileTime(base_dir_, &mtime));
  EXPECT_NE(mtime, 0);
}

TEST_F(PathTest, GetFileTime_FileNotFound) {
  time_t mtime;
  EXPECT_NOT_OK(path::GetFileTime("non_existing_file", &mtime));
}

TEST_F(PathTest, AreEqual) {
  EXPECT_TRUE(path::AreEqual("path/to/file", "path/to/file"));
  EXPECT_TRUE(path::AreEqual("path/other/../to/file", "path/to/file"));
  EXPECT_TRUE(
      path::AreEqual(path::Join("path/to", kUnicodeTestFileName),
                     path::Join("path/../path/to", kUnicodeTestFileName)));
  EXPECT_TRUE(path::AreEqual(path::Join("path/to", kUnicodeTestFileName),
                             path::Join("path/to/.", kUnicodeTestFileName)));
  EXPECT_TRUE(path::AreEqual(u8"path/\U0001F964/file",
                             u8"path/\U0001F964/../\U0001F964/file"));
  EXPECT_TRUE(path::AreEqual(u8"path/\U0001F964/file",
                             u8"path/\U0001F964/../../path/\U0001F964/./file"));
#if PLATFORM_WINDOWS
  // Backslash is not a valid directory separator on Linux.
  EXPECT_TRUE(path::AreEqual("c:\\dir\\sub_dir\\file.txt",
                             "C:\\dir\\more\\..\\..\\dir\\sub_dir\\file.txt"));
  EXPECT_FALSE(path::AreEqual("c:\\dir\\sub_dir\\file.txt",
                              "C:\\dir\\more\\..\\dir\\sub_dir\\file.txt"));
#endif
  EXPECT_FALSE(path::AreEqual("path/to/file", "path/file/to"));
}

}  // namespace
}  // namespace cdc_ft
