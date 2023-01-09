/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef COMMON_FILE_WATCHER_WIN_H_
#define COMMON_FILE_WATCHER_WIN_H_

#define WIN32_LEAN_AND_MEAN

#include <string>
#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace cdc_ft {
class AsyncFileWatcher;

// FileWatcherWin observes changes done in a specific directory on Windows.
class FileWatcherWin {
 public:
  // Default timeout in milliseconds.
  static constexpr unsigned int kFileWatcherTimeoutMs = 1000;

  using FilesChangedCb = std::function<void()>;
  using DirRecreatedCb = std::function<void()>;

  enum class FileAction { kAdded, kModified, kDeleted };

  struct FileInfo {
    FileAction action;
    bool is_dir;
    uint64_t size;
    int64_t mtime;

    FileInfo(FileAction action, bool is_dir, uint64_t size, int64_t mtime)
        : action(action), is_dir(is_dir), size(size), mtime(mtime) {}
  };

  using FileMap = std::unordered_map<std::string, FileInfo>;

  explicit FileWatcherWin(std::string directory);
  FileWatcherWin(const FileWatcherWin& other) = delete;
  FileWatcherWin& operator=(const FileWatcherWin& other) = delete;

  ~FileWatcherWin();

  // Returns a map that maps relative paths of modified files to their file
  // attributes.
  FileMap GetModifiedFiles() ABSL_LOCKS_EXCLUDED(modified_files_mutex_);

  // Starts watching directory changes.
  // |files_changed_cb| is called on a background thread whenever files changed.
  // |dir_recreated_cb| is called on a background thread whenever the watched
  // directory was removed/created/re-created. It does not guarantee that the
  // watched directory exists. The caller should check on its own and handle
  // a case if the directory still does not exist. The callback shows that all
  // outstanding changes are not valid anymore.
  // |timeout_ms| is a timeout in ms for a change in the watched directory.
  absl::Status StartWatching(FilesChangedCb files_changed_cb = FilesChangedCb(),
                             DirRecreatedCb dir_recreated_cb = DirRecreatedCb(),
                             unsigned int timeout_ms = kFileWatcherTimeoutMs);

  // Stops watching directory changes.
  absl::Status StopWatching() ABSL_LOCKS_EXCLUDED(modified_files_mutex_);

  // Indicates whether StartWatching() was called, but StopWatching() was not
  // called yet.
  bool IsStarted() const;

  // Indicates whether a directory is actively watched for changes. In contrast
  // to IsStarted(), returns false while the directory does not exist.
  bool IsWatching() const;

  // Returns the watching status.
  absl::Status GetStatus() const;

  // Returns the total file changed events received so far.
  // Returns 0 if the watcher is currently in stopped state.
  uint32_t GetEventCountForTesting() const;

  // Returns the total number of events for the directory changes received so
  // far. Returns 0 if the watcher is currently in stopped state.
  uint32_t GetDirRecreateEventCountForTesting() const;

  // Enforces the use of ReadDirectoryChanges instead of ReadDirectoryChangesEx.
  // Must be called before StartWatching.
  void EnforceLegacyReadDirectoryChangesForTesting();

 private:
  std::string dir_path_;

  std::unique_ptr<AsyncFileWatcher> async_watcher_;

  absl::Mutex modified_files_mutex_;
  FileMap modified_files_ ABSL_GUARDED_BY(modified_files_mutex_);

  bool enforceLegacyReadDirectoryChangesForTesting_ = false;
};

}  // namespace cdc_ft

#endif  // COMMON_FILE_WATCHER_WIN_H_
