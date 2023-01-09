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

#include "common/file_watcher_win.h"

#define WIN32_LEAN_AND_MEAN

#include <chrono>
#include <string>
#include <thread>

#include "absl/strings/match.h"
#include "common/buffer.h"
#include "common/log.h"
#include "common/path.h"
#include "common/platform.h"
#include "common/status_test_macros.h"
#include "common/util.h"
#include "gtest/gtest.h"

namespace cdc_ft {

const char* ActionToString(const FileWatcherWin::FileAction& fa) {
  switch (fa) {
    case FileWatcherWin::FileAction::kAdded:
      return "ADDED";
    case FileWatcherWin::FileAction::kModified:
      return "MODIFIED";
    case FileWatcherWin::FileAction::kDeleted:
      return "DELETED";
    default:
      return "UNKNOWN";
  }
}

std::ostream& operator<<(std::ostream& os,
                         const FileWatcherWin::FileMap& files) {
  for (const auto& [path, fi] : files) {
    os << "path=" << path << ", action=" << ActionToString(fi.action)
       << ", is_dir=" << fi.is_dir << ", mtime=" << fi.mtime
       << ", size=" << fi.size << std::endl;
  }
  return os;
}

namespace {

constexpr char kWatcherTestDir[] = "watcher_test_dir";
constexpr char kWatcherWatchedDir[] = "watcher_watched_dir";
constexpr char kFirstData[] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
constexpr char kSecondData[] = {100, 101, 102, 103, 104, 105, 106, 107, 108,
                                100, 101, 102, 103, 104, 105, 106, 107, 108};
constexpr size_t kFirstDataSize = sizeof(kFirstData);
constexpr size_t kSecondDataSize = sizeof(kSecondData);

constexpr char kFirstFile[] = "first_test_file.txt";
constexpr char kSecondFile[] = "second_test_file.txt";
constexpr char kFirstDir[] = "first_test_dir";
constexpr char kSecondDir[] = "second_test_dir";

constexpr bool kFile = false;
constexpr bool kDir = true;

constexpr absl::Duration kWaitTimeout = absl::Seconds(5);
constexpr unsigned int kFWTimeout = 10;

using FileMap = FileWatcherWin::FileMap;
using FileAction = FileWatcherWin::FileAction;
using FileInfo = FileWatcherWin::FileInfo;

class FileWatcherParameterizedTest : public ::testing::TestWithParam<bool> {
 public:
  FileWatcherParameterizedTest() : watcher_(watcher_dir_path_) {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }
  ~FileWatcherParameterizedTest() { Log::Shutdown(); }

  void SetUp() override {
    legacyReadDirectoryChanges_ = GetParam();
    if (legacyReadDirectoryChanges_)
      watcher_.EnforceLegacyReadDirectoryChangesForTesting();
    EXPECT_OK(path::RemoveDirRec(watcher_dir_path_));
    EXPECT_OK(path::CreateDirRec(watcher_dir_path_));
  }

  void TearDown() override { EXPECT_OK(path::RemoveDirRec(watcher_dir_path_)); }

  // True if ReadDirectoryChangesW should be enforced (instead of *ExW).
  bool legacyReadDirectoryChanges_ = false;

 protected:
  void OnFilesChanged() {
    absl::MutexLock lock(&files_changed_mutex_);
    files_changed_ = true;
  }

  void OnDirRecreated() {
    absl::MutexLock lock(&files_changed_mutex_);
    dir_recreated_ = true;
  }

  bool WaitForChange(uint32_t min_event_count = 0) {
    absl::MutexLock lock(&files_changed_mutex_);
    bool changed = false;
    do {
      auto cond = [this]() { return files_changed_; };
      changed = files_changed_mutex_.AwaitWithTimeout(absl::Condition(&cond),
                                                      kWaitTimeout);
      files_changed_ = false;
    } while (changed && watcher_.GetEventCountForTesting() < min_event_count);
    return changed;
  }

  bool WaitForDirRecreated(uint32_t min_event_count = 0) {
    absl::MutexLock lock(&files_changed_mutex_);
    bool changed = false;
    do {
      auto cond = [this]() { return dir_recreated_; };
      changed = files_changed_mutex_.AwaitWithTimeout(absl::Condition(&cond),
                                                      kWaitTimeout);
      dir_recreated_ = false;
    } while (changed &&
             watcher_.GetDirRecreateEventCountForTesting() < min_event_count);
    return changed;
  }

  // Polls for a second until the watcher is running again.
  bool WaitForRunning() const {
    for (int n = 0; n < 1000; ++n) {
      if (watcher_.IsWatching()) return true;
      Util::Sleep(1);
    }
    return false;
  }

  FileMap GetChangedFiles(size_t number_of_files) {
    FileMap modified_files;

    // Wait for events, until they are processed.
    while (modified_files.size() < number_of_files) {
      if (!WaitForChange()) {
        LOG_ERROR("No change detected after %s",
                  absl::FormatDuration(kWaitTimeout));
        return modified_files;
      }
      for (const auto& [path, info] : watcher_.GetModifiedFiles())
        modified_files.insert_or_assign(path, info);
    }
    return modified_files;
  }

  void ExpectFile(const FileMap& modified_files, const std::string& path) {
    EXPECT_TRUE(modified_files.find(path) != modified_files.end())
        << path << " is missing from " << std::endl
        << modified_files;
  }

  void ExpectFileInfo(const FileMap& modified_files, const std::string& path,
                      FileAction action, bool is_dir, uint64_t size) {
    auto iter = modified_files.find(path);
    EXPECT_TRUE(iter != modified_files.end())
        << path << " is missing from " << std::endl
        << modified_files;
    if (iter != modified_files.end()) {
      EXPECT_EQ(iter->second.action, action);
      // is_dir and size are not available for the legacy ReadDirectoryChangesW,
      // but that data isn't needed, anyway.
      if (action != FileAction::kDeleted || !legacyReadDirectoryChanges_) {
        EXPECT_EQ(iter->second.is_dir, is_dir);
        EXPECT_EQ(iter->second.size, size);
      }
      // Don't bother checking mtime here, it's checked elsewhere.
    }
  }

  const std::string test_dir_path_ =
      path::Join(path::GetTempDir(), kWatcherTestDir);

  const std::string watcher_dir_path_ =
      path::Join(test_dir_path_, kWatcherWatchedDir);

  const std::string first_file_path_ =
      path::Join(watcher_dir_path_, kFirstFile);
  const std::string second_file_path_ =
      path::Join(watcher_dir_path_, kSecondFile);
  const std::string first_dir_path_ = path::Join(watcher_dir_path_, kFirstDir);
  const std::string second_dir_path_ =
      path::Join(watcher_dir_path_, kSecondDir);

  FileWatcherWin watcher_;

  bool files_changed_ ABSL_GUARDED_BY(files_changed_mutex_) = false;
  bool dir_recreated_ ABSL_GUARDED_BY(files_changed_mutex_) = false;
  absl::Mutex files_changed_mutex_;
};

TEST_P(FileWatcherParameterizedTest, DirDoesNotExist) {
  FileWatcherWin watcher("non-existing folder");
  if (legacyReadDirectoryChanges_)
    watcher_.EnforceLegacyReadDirectoryChangesForTesting();
  EXPECT_NOT_OK(watcher.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_FALSE(watcher.IsStarted());
  absl::Status status = watcher.GetStatus();
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::IsFailedPrecondition(status));
  EXPECT_TRUE(absl::StrContains(status.message(), "Could not start watching"));
}

TEST_P(FileWatcherParameterizedTest, CreateFile) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, CreateFileDelayed) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, CreateTwoFiles) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(path::WriteFile(second_file_path_, kFirstData, kFirstDataSize));

  FileMap modified_files = GetChangedFiles(2u);
  EXPECT_EQ(modified_files.size(), 2u);
  ExpectFile(modified_files, kFirstFile);
  ExpectFile(modified_files, kSecondFile);

  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, CreateDir) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::CreateDir(first_dir_path_));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstDir);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RenameDir) {
  EXPECT_OK(path::CreateDir(first_dir_path_));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  EXPECT_OK(path::RenameFile(first_dir_path_, second_dir_path_));

  FileMap modified_files = GetChangedFiles(2u);
  EXPECT_EQ(modified_files.size(), 2u);
  ExpectFileInfo(modified_files, kFirstDir, FileAction::kDeleted, kDir, 0);
  ExpectFileInfo(modified_files, kSecondDir, FileAction::kAdded, kDir, 0);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RenameFile) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  EXPECT_OK(path::RenameFile(first_file_path_, second_file_path_));

  FileMap modified_files = GetChangedFiles(2u);
  EXPECT_EQ(modified_files.size(), 2u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kDeleted, kFile,
                 kFirstDataSize);
  ExpectFileInfo(modified_files, kSecondFile, FileAction::kAdded, kFile,
                 kFirstDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RemoveDir) {
  EXPECT_OK(path::CreateDir(first_dir_path_));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  EXPECT_OK(path::RemoveDirRec(first_dir_path_));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstDir);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RemoveFile) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  EXPECT_OK(path::RemoveFile(first_file_path_));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ChangeFile) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kSecondData, kSecondDataSize));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, DirHierarchy) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  std::vector<std::string> files = {"1.txt", "2.txt", "3.txt", "4.txt",
                                    "5.txt", "6.txt", "7.txt", "8.txt"};
  EXPECT_OK(path::CreateDir(first_dir_path_));
  EXPECT_OK(path::CreateDir(second_dir_path_));
  for (const std::string& file : files) {
    for (const auto& path :
         {first_dir_path_, second_dir_path_, watcher_dir_path_})
      EXPECT_OK(
          path::WriteFile(path::Join(path, file), kFirstData, kFirstDataSize));
  }

  FileMap modified_files = GetChangedFiles(files.size() * 3 + 2);
  ASSERT_EQ(modified_files.size(), files.size() * 3 + 2);

  for (const std::string& file : files) {
    ExpectFile(modified_files, path::Join(kFirstDir, file));
    ExpectFile(modified_files, path::Join(kSecondDir, file));
    ExpectFile(modified_files, file);
  }
  ExpectFile(modified_files, kFirstDir);
  ExpectFile(modified_files, kSecondDir);

  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, NoReadDirChanges) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(watcher_.StopWatching());
  FileMap modified_files = GetChangedFiles(0u);
  EXPECT_EQ(modified_files.size(), 0u);
}

TEST_P(FileWatcherParameterizedTest, RestartWatchingNoChanges) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(watcher_.StopWatching());

  // Restart with reading.
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(watcher_.StopWatching());

  // Restart without reading.
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RestartWatchingWithChanges) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(watcher_.StopWatching());

  // first_test_dir should not be in the modification set.
  EXPECT_OK(path::CreateDir(first_dir_path_));

  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(second_file_path_, kSecondData, kSecondDataSize));

  FileMap modified_files = GetChangedFiles(2u);
  EXPECT_EQ(modified_files.size(), 2u);
  ExpectFile(modified_files, kFirstFile);
  ExpectFile(modified_files, kSecondFile);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ReadFileNoNotification) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  Buffer data;
  EXPECT_OK(path::ReadFile(first_file_path_, &data));

  FileMap modified_files = GetChangedFiles(0u);
  EXPECT_EQ(modified_files.size(), 0u);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, SearchFilesNoNotification) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));

  unsigned int counter = 0;
  auto handler = [&counter](const std::string& /*dir*/,
                            const std::string& /*filename*/,
                            int64_t /*modified_time*/, uint64_t /*size*/,
                            bool /*is_directory*/) {
    ++counter;
    return absl::OkStatus();
  };

  EXPECT_OK(path::SearchFiles(first_file_path_, true, handler));
  EXPECT_EQ(counter, 1u);

  FileMap modified_files = GetChangedFiles(0u);
  EXPECT_EQ(modified_files.size(), 0u);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionAdd) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/2));  // 1x add, 1x modify

  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kAdded, kFile,
                 kFirstDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionModify) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kSecondData, kSecondDataSize));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/2));  // 2x modify

  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kModified, kFile,
                 kSecondDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionAddModify) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(path::WriteFile(first_file_path_, kSecondData, kSecondDataSize));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/4));  // 1x add, 3x modify

  // Add + modify should not result in FileAction::kModified.
  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kAdded, kFile,
                 kSecondDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionDelete) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::RemoveFile(first_file_path_));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/1));  // 1x remove

  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kDeleted, kFile,
                 kFirstDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionAddModifyRemove) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_OK(path::WriteFile(first_file_path_, kSecondData, kSecondDataSize));
  EXPECT_OK(path::RemoveFile(first_file_path_));
  EXPECT_TRUE(
      WaitForChange(/*min_event_count=*/5));  // 1x add, 3x modify, 1x remove

  // The watcher should collapse add-modify-remove sequences and report no
  // changes.
  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 0u);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ActionModifyRemove) {
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kSecondData, kSecondDataSize));
  EXPECT_OK(path::RemoveFile(first_file_path_));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/3));  // 2x modify, 1x remove

  FileMap modified_files = watcher_.GetModifiedFiles();
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFileInfo(modified_files, kFirstFile, FileAction::kDeleted, kFile,
                 kSecondDataSize);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, ModifiedTime) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); }));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_TRUE(WaitForChange(/*min_event_count=*/2));  // 2x modify
  FileMap modified_files = watcher_.GetModifiedFiles();
  ASSERT_EQ(modified_files.size(), 1u);
  time_t mtime;
  EXPECT_OK(path::GetFileTime(first_file_path_, &mtime));
  const FileInfo& info = modified_files.begin()->second;
  EXPECT_EQ(info.mtime, mtime);
  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, DeleteWatchedDir) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); },
                                   [this]() { OnDirRecreated(); }, kFWTimeout));

  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));
  EXPECT_TRUE(WaitForChange(2u));  // 2x modify

  EXPECT_OK(path::RemoveDirRec(watcher_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(1u));

  EXPECT_TRUE(watcher_.GetModifiedFiles().empty());
  EXPECT_NOT_OK(watcher_.GetStatus());
  // The error status should not be overwritten.
  EXPECT_NOT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RecreateWatchedDir) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); },
                                   [this]() { OnDirRecreated(); }, kFWTimeout));

  EXPECT_OK(path::RemoveDirRec(watcher_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(1u));

  EXPECT_OK(path::CreateDirRec(watcher_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(2u));

  EXPECT_TRUE(watcher_.GetModifiedFiles().empty());
  EXPECT_OK(watcher_.GetStatus());

  // Wait until the watcher is running again, or else we might miss the file.
  EXPECT_TRUE(WaitForRunning());

  // Creation of a new file should be detected.
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);

  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RecreateUpperDir) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); },
                                   [this]() { OnDirRecreated(); }, kFWTimeout));

  // Initially, there should be no dir_recreated_events.
  EXPECT_EQ(watcher_.GetDirRecreateEventCountForTesting(), 0u);
  EXPECT_OK(path::RemoveDirRec(test_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(1u));

  // Only 1 event should exist (for directory removal).
  EXPECT_OK(path::CreateDirRec(test_dir_path_));
  EXPECT_OK(path::CreateDirRec(watcher_dir_path_));

  // As the upper level directory is created separately, no new event should be
  // available.
  EXPECT_TRUE(WaitForDirRecreated(2u));

  // Only 1 additional event should be registered for creation of the directory.
  EXPECT_EQ(watcher_.GetDirRecreateEventCountForTesting(), 2u);
  EXPECT_TRUE(watcher_.GetModifiedFiles().empty());
  EXPECT_OK(watcher_.GetStatus());

  // Wait until the watcher is running again, or else we might miss the file.
  EXPECT_TRUE(WaitForRunning());

  // Creation of a new file should be detected.
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  FileMap modified_files = GetChangedFiles(1u);
  EXPECT_EQ(modified_files.size(), 1u);
  ExpectFile(modified_files, kFirstFile);

  // No new events should be registered for the watched directory.
  EXPECT_EQ(watcher_.GetDirRecreateEventCountForTesting(), 2u);

  EXPECT_OK(watcher_.StopWatching());
}

TEST_P(FileWatcherParameterizedTest, RecreateWatchedDirNoOldChanges) {
  EXPECT_OK(watcher_.StartWatching([this]() { OnFilesChanged(); },
                                   [this]() { OnDirRecreated(); }, kFWTimeout));
  EXPECT_OK(path::WriteFile(first_file_path_, kFirstData, kFirstDataSize));

  EXPECT_OK(path::RemoveDirRec(watcher_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(1u));

  EXPECT_OK(path::CreateDirRec(watcher_dir_path_));
  EXPECT_TRUE(WaitForDirRecreated(2u));

  EXPECT_TRUE(watcher_.GetModifiedFiles().empty());
  EXPECT_OK(watcher_.GetStatus());

  EXPECT_OK(watcher_.StopWatching());
}

struct TestName {
  std::string operator()(const testing::TestParamInfo<bool>& legacy) const {
    return legacy.param ? "ReadDirectoryChangesW" : "ReadDirectoryChangesExW";
  }
};

// Run without and with legacy ReadDirectoryChangesW function.
INSTANTIATE_TEST_CASE_P(FileWatcherTest, FileWatcherParameterizedTest,
                        ::testing::Values(false, true), TestName());

}  // namespace
}  // namespace cdc_ft
