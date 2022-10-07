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

#include "cdc_rsync/file_finder_and_sender.h"

#include <algorithm>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "cdc_rsync/base/fake_socket.h"
#include "cdc_rsync/base/message_pump.h"
#include "common/log.h"
#include "common/path.h"
#include "common/path_filter.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Definitions to improve readability.
constexpr bool kNotRecursive = false;
constexpr bool kRecursive = true;

constexpr bool kNotRelative = false;
constexpr bool kRelative = true;

class FakeFindFilesProgress : public ReportFindFilesProgress {
 public:
  FakeFindFilesProgress() {}
  void ReportFileFound() override { num_files_++; }
  void ReportDirFound() override { num_dirs_++; }

  uint64_t num_files_ = 0;
  uint64_t num_dirs_ = 0;
};

class FileFinderAndSenderTest : public ::testing::Test {
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    message_pump_.StartMessagePump();
  }

  void TearDown() override {
    socket_.ShutdownSendingEnd();
    message_pump_.StopMessagePump();
    Log::Shutdown();
  }

 protected:
  struct ReceivedFile {
    std::string dir;
    std::string file;
    ReceivedFile(std::string dir, std::string file)
        : dir(std::move(dir)), file(std::move(file)) {}
  };

  struct ReceivedFileFormatter {
    void operator()(std::string* out, const ReceivedFile& val) const {
      absl::StrAppend(out, absl::StrFormat("{ %s, %s }", val.dir, val.file));
    }
  };

  void ExpectReceiveFiles(std::vector<ReceivedFile> expected_data,
                          std::vector<int> expected_batch_count = {}) {
    std::vector<ReceivedFile> data;
    std::vector<int> batch_count;
    AddFilesRequest request;
    for (;;) {
      EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kAddFiles, &request));
      if (request.files_size() == 0 && request.dirs_size() == 0) {
        // EOF.
        break;
      }

      batch_count.push_back(request.files_size() + request.dirs_size());
      for (const auto& file : request.files()) {
        data.emplace_back(request.directory(), file.filename());
      }
      for (const auto& dir : request.dirs()) {
        data.emplace_back(request.directory(), dir);
      }
    }

    // expected_batch_count can be empty for convenience.
    if (!expected_batch_count.empty()) {
      EXPECT_EQ(absl::StrJoin(batch_count, ", "),
                absl::StrJoin(expected_batch_count, ", "));
    }

    EXPECT_EQ(absl::StrJoin(data, ", ", ReceivedFileFormatter()),
              absl::StrJoin(expected_data, ", ", ReceivedFileFormatter()));
  }

  FakeSocket socket_;
  FakeFindFilesProgress progress_;
  PathFilter path_filter_;
  MessagePump message_pump_{&socket_, MessagePump::PacketReceivedDelegate()};

  std::string base_dir_ = GetTestDataDir("file_finder_and_sender");
};

TEST_F(FileFinderAndSenderTest, FindNonRecursive) {
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kNotRecursive, kNotRelative);

  EXPECT_OK(finder.FindAndSendFiles(base_dir_));
  EXPECT_EQ(progress_.num_files_, 0);
  std::vector<ClientFileInfo> files;
  EXPECT_OK(finder.Flush());
  finder.ReleaseFiles(&files);

  ASSERT_TRUE(files.empty());
  ExpectReceiveFiles({{}});
}

TEST_F(FileFinderAndSenderTest, FindRecursive) {
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kRecursive, kNotRelative);

  EXPECT_OK(finder.FindAndSendFiles(base_dir_));
  EXPECT_EQ(progress_.num_files_, 5);
  EXPECT_EQ(progress_.num_dirs_, 2);

  EXPECT_OK(finder.Flush());
  std::vector<ClientFileInfo> files;
  finder.ReleaseFiles(&files);
  std::vector<ClientDirInfo> dirs;
  finder.ReleaseDirs(&dirs);

  ASSERT_EQ(files.size(), 5);
  EXPECT_EQ(files[0].path, path::Join(base_dir_, "a.txt"));
  EXPECT_EQ(files[1].path, path::Join(base_dir_, "b.txt"));
  EXPECT_EQ(files[2].path, path::Join(base_dir_, "c.txt"));
  EXPECT_EQ(files[3].path, path::Join(base_dir_, "subdir", "d.txt"));
  EXPECT_EQ(files[4].path, path::Join(base_dir_, "subdir", "e.txt"));

  ASSERT_EQ(dirs.size(), 2);
  EXPECT_EQ(dirs[0].path, base_dir_);
  EXPECT_EQ(dirs[1].path, path::Join(base_dir_, "subdir"));

  // Verify that the data sent to the socket matches.
  ExpectReceiveFiles({{"", "file_finder_and_sender"},
                      {"file_finder_and_sender\\", "a.txt"},
                      {"file_finder_and_sender\\", "b.txt"},
                      {"file_finder_and_sender\\", "c.txt"},
                      {"file_finder_and_sender\\", "subdir"},
                      {"file_finder_and_sender\\subdir\\", "d.txt"},
                      {"file_finder_and_sender\\subdir\\", "e.txt"}},
                     {1, 4, 2});
}

TEST_F(FileFinderAndSenderTest, FindWithSmallerBatchSize) {
  // Tweak size threshold so that we get 2 files per batch for the base dir.
  // This tests that the batch gets flushed when the directory changes since
  // the base dir has 3 files.
  int request_byte_size_threshold = static_cast<int>(
      strlen("file_finder_and_sender\\") +  // directory
      sizeof(int64_t) + sizeof(uint64_t) +  // modified_time + size
      strlen("a.txt") + 3);                 // filename + some slack
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kRecursive, kNotRelative,
                             request_byte_size_threshold);

  EXPECT_OK(finder.FindAndSendFiles(base_dir_));
  EXPECT_OK(finder.Flush());
  EXPECT_EQ(progress_.num_files_, 5);
  ASSERT_EQ(progress_.num_dirs_, 2);

  // Note that the expected batch size is {1, 2, 2, 1, 1} here due to the
  // smaller request sizes.
  ExpectReceiveFiles({{"", "file_finder_and_sender"},
                      {"file_finder_and_sender\\", "a.txt"},
                      {"file_finder_and_sender\\", "b.txt"},
                      {"file_finder_and_sender\\", "c.txt"},
                      {"file_finder_and_sender\\", "subdir"},
                      {"file_finder_and_sender\\subdir\\", "d.txt"},
                      {"file_finder_and_sender\\subdir\\", "e.txt"}},
                     {1, 2, 2, 1, 1});
}

TEST_F(FileFinderAndSenderTest, FindWithFilter) {
  path_filter_.AddRule(PathFilter::Rule::Type::kExclude, "*b.txt");
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kNotRecursive, kNotRelative);

  EXPECT_OK(finder.FindAndSendFiles(path::Join(base_dir_, "*")));
  EXPECT_EQ(progress_.num_files_, 2);
  EXPECT_EQ(progress_.num_dirs_, 1);

  EXPECT_OK(finder.Flush());
  std::vector<ClientFileInfo> files;
  finder.ReleaseFiles(&files);
  std::vector<ClientDirInfo> dirs;
  finder.ReleaseDirs(&dirs);

  ASSERT_EQ(files.size(), 2);
  EXPECT_EQ(files[0].path, path::Join(base_dir_, "a.txt"));
  EXPECT_EQ(files[1].path, path::Join(base_dir_, "c.txt"));

  ASSERT_EQ(dirs.size(), 1);
  EXPECT_EQ(dirs[0].path, path::Join(base_dir_, "subdir"));

  ExpectReceiveFiles({{"", "a.txt"}, {"", "c.txt"}, {"", "subdir"}});
}

TEST_F(FileFinderAndSenderTest, FindWithDot) {
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kRecursive, kNotRelative);

  EXPECT_OK(finder.FindAndSendFiles(base_dir_ + "\\."));
  EXPECT_EQ(progress_.num_files_, 5);
  EXPECT_EQ(progress_.num_dirs_, 1);

  EXPECT_OK(finder.Flush());

  ExpectReceiveFiles({{"", "a.txt"},
                      {"", "b.txt"},
                      {"", "c.txt"},
                      {"", "subdir"},
                      {"subdir\\", "d.txt"},
                      {"subdir\\", "e.txt"}},
                     {});
}

TEST_F(FileFinderAndSenderTest, FindWithForwardSlash) {
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kRecursive, kNotRelative);

  std::string base_dir_forward = GetTestDataDir("file_finder_and_sender");
  std::replace(base_dir_forward.begin(), base_dir_forward.end(), '\\', '/');

  EXPECT_OK(finder.FindAndSendFiles(base_dir_forward + "/."));
  EXPECT_EQ(progress_.num_files_, 5);
  EXPECT_EQ(progress_.num_dirs_, 1);

  EXPECT_OK(finder.Flush());

  ExpectReceiveFiles({{"", "a.txt"},
                      {"", "b.txt"},
                      {"", "c.txt"},
                      {"", "subdir"},
                      {"subdir\\", "d.txt"},
                      {"subdir\\", "e.txt"}},
                     {});
}

TEST_F(FileFinderAndSenderTest, FindWithRelative) {
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_, "",
                             kNotRecursive, kRelative);

  std::vector<std::string> sources = {
      path::Join(base_dir_, "a.txt"), path::Join(base_dir_, ".", "b.txt"),
      path::Join(base_dir_, ".", "subdir", "d.txt"),
      path::Join(base_dir_, "subdir", ".", "e.txt")};

  for (const std::string& source : sources) {
    EXPECT_OK(finder.FindAndSendFiles(source));
  }

  EXPECT_EQ(progress_.num_files_, 4);
  std::vector<ClientFileInfo> files;
  EXPECT_OK(finder.Flush());
  finder.ReleaseFiles(&files);

  ASSERT_EQ(files.size(), 4);
  EXPECT_EQ(files[0].path, sources[0]);
  EXPECT_EQ(files[1].path, sources[1]);
  EXPECT_EQ(files[2].path, sources[2]);
  EXPECT_EQ(files[3].path, sources[3]);

  // The paths sent to the socket should have the correct relative paths.
  std::string rel =
      base_dir_.substr(path::GetDrivePrefix(base_dir_).size() + 1) + "\\";

  ExpectReceiveFiles(
      {{rel, "a.txt"}, {"", "b.txt"}, {"subdir\\", "d.txt"}, {"", "e.txt"}});
}

TEST_F(FileFinderAndSenderTest, FindWithRelativeAndSourcesDir) {
  // Just go to the parent directory, we just need some existing dir.
  std::string sources_dir = path::DirName(base_dir_);
  path::EnsureEndsWithPathSeparator(&sources_dir);
  std::string rel_dir = base_dir_.substr(sources_dir.size());

  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_,
                             sources_dir, kNotRecursive, kRelative);

  std::vector<std::string> sources = {
      path::Join(rel_dir, "a.txt"), path::Join(rel_dir, ".", "b.txt"),
      path::Join(rel_dir, "subdir", "d.txt"),
      path::Join(rel_dir, "subdir", ".", "e.txt")};

  for (const std::string& source : sources) {
    EXPECT_OK(finder.FindAndSendFiles(source));
  }

  EXPECT_EQ(progress_.num_files_, 4);
  std::vector<ClientFileInfo> files;
  EXPECT_OK(finder.Flush());
  finder.ReleaseFiles(&files);

  ASSERT_EQ(files.size(), 4);
  EXPECT_EQ(files[0].path, path::Join(sources_dir, sources[0]));
  EXPECT_EQ(files[1].path, path::Join(sources_dir, sources[1]));
  EXPECT_EQ(files[2].path, path::Join(sources_dir, sources[2]));
  EXPECT_EQ(files[3].path, path::Join(sources_dir, sources[3]));

  path::EnsureEndsWithPathSeparator(&rel_dir);
  ExpectReceiveFiles({{rel_dir, "a.txt"},
                      {"", "b.txt"},
                      {rel_dir + "subdir\\", "d.txt"},
                      {"", "e.txt"}});
}

TEST_F(FileFinderAndSenderTest,
       FindWithRelativeAndSourcesDirForwardSlashInSouceDir) {
  // Just go to the parent directory, we just need some existing dir.
  std::string sources_dir = path::DirName(base_dir_);
  path::EnsureEndsWithPathSeparator(&sources_dir);
  std::string rel_dir = base_dir_.substr(sources_dir.size());

  std::string sources_dir_forward(sources_dir);
  std::replace(sources_dir_forward.begin(), sources_dir_forward.end(), '\\',
               '/');
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_,
                             sources_dir_forward, kNotRecursive, kRelative);

  std::vector<std::string> sources = {
      path::Join(rel_dir, "a.txt"), path::Join(rel_dir, ".", "b.txt"),
      path::Join(rel_dir, "subdir", "d.txt"),
      path::Join(rel_dir, "subdir", ".", "e.txt")};

  for (const std::string& source : sources) {
    EXPECT_OK(finder.FindAndSendFiles(source));
  }

  EXPECT_EQ(progress_.num_files_, 4);
  std::vector<ClientFileInfo> files;
  EXPECT_OK(finder.Flush());
  finder.ReleaseFiles(&files);

  ASSERT_EQ(files.size(), 4);
  EXPECT_EQ(files[0].path, path::Join(sources_dir, sources[0]));
  EXPECT_EQ(files[1].path, path::Join(sources_dir, sources[1]));
  EXPECT_EQ(files[2].path, path::Join(sources_dir, sources[2]));
  EXPECT_EQ(files[3].path, path::Join(sources_dir, sources[3]));

  path::EnsureEndsWithPathSeparator(&rel_dir);
  ExpectReceiveFiles({{rel_dir, "a.txt"},
                      {"", "b.txt"},
                      {rel_dir + "subdir\\", "d.txt"},
                      {"", "e.txt"}});
}
TEST_F(FileFinderAndSenderTest,
       FindWithRelativeAndSourcesDirForwardSlashInSourceDirFiles) {
  // Just go to the parent directory, we just need some existing dir.
  std::string sources_dir = path::DirName(base_dir_);
  path::EnsureEndsWithPathSeparator(&sources_dir);
  std::string rel_dir = base_dir_.substr(sources_dir.size());

  std::string sources_dir_forward(sources_dir);
  std::replace(sources_dir_forward.begin(), sources_dir_forward.end(), '\\',
               '/');
  FileFinderAndSender finder(&path_filter_, &message_pump_, &progress_,
                             sources_dir_forward, kNotRecursive, kRelative);

  std::vector<std::string> sources = {rel_dir + "/a.txt", rel_dir + "/./b.txt",
                                      rel_dir + "/subdir/d.txt",
                                      rel_dir + "/subdir/./e.txt"};

  for (const std::string& source : sources) {
    EXPECT_OK(finder.FindAndSendFiles(source));
  }

  EXPECT_EQ(progress_.num_files_, 4);
  std::vector<ClientFileInfo> files;
  EXPECT_OK(finder.Flush());
  finder.ReleaseFiles(&files);

  ASSERT_EQ(files.size(), 4);
  for (std::string& source : sources) path::FixPathSeparators(&source);
  EXPECT_EQ(files[0].path, path::Join(sources_dir, sources[0]));
  EXPECT_EQ(files[1].path, path::Join(sources_dir, sources[1]));
  EXPECT_EQ(files[2].path, path::Join(sources_dir, sources[2]));
  EXPECT_EQ(files[3].path, path::Join(sources_dir, sources[3]));

  path::EnsureEndsWithPathSeparator(&rel_dir);
  ExpectReceiveFiles({{rel_dir, "a.txt"},
                      {"", "b.txt"},
                      {rel_dir + "subdir\\", "d.txt"},
                      {"", "e.txt"}});
}

}  // namespace
}  // namespace cdc_ft
