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

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <windows.h>

#include <atomic>
#include <thread>

#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/log.h"
#include "common/path.h"
#include "common/scoped_handle_win.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/stopwatch.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

static constexpr DWORD kFileNotificationFlags =
    FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME |
    FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE;

static constexpr size_t kDefaultBufferSize = 1U << 18;  // 256 KiB.

void CancelDirIo(ScopedHandle& dir_handle, const std::wstring& dir_path) {
  if (dir_handle.IsValid() && !CancelIo(dir_handle.Get())) {
    LOG_ERROR("CancelIo() failed for directory '%s': '%s'",
              Util::WideToUtf8Str(dir_path), Util::GetLastWin32Error());
  }
  dir_handle.Close();
}

int64_t ToUnixTime(LARGE_INTEGER windows_time) {
  // Jan 1, 1970 (begin of Unix epoch) in 100 ns ticks since Jan 1, 1601 (begin
  // of Windows epoch).
  constexpr int64_t kUnixEpochOffset = 0x019DB1DED53E8000;

  // A Windows tick is 100 ns.
  constexpr int64_t kWinTicksPerSecond = 10000000;

  return (windows_time.QuadPart - kUnixEpochOffset) / kWinTicksPerSecond;
}

}  // namespace

// Background thread to read directory changes.
class AsyncFileWatcher {
 public:
  enum class FileWatcherState {
    kDefault,      // Not started.
    kFailed,       // Some error during watching, e.g. directory got deleted.
                   // Will attempt to recover automatically.
    kWatching,     // Actively watching directory.
    kShuttingDown  // Shutdown() was called, winding watcher down.
  };

  using FileAction = FileWatcherWin::FileAction;
  using FileInfo = FileWatcherWin::FileInfo;
  using FileMap = FileWatcherWin::FileMap;
  using FilesChangedCb = FileWatcherWin::FilesChangedCb;
  using DirRecreatedCb = FileWatcherWin::DirRecreatedCb;

  AsyncFileWatcher(std::string dir_path, FilesChangedCb files_changed_cb,
                   DirRecreatedCb dir_recreated_cb, unsigned int timeout_ms,
                   bool enforceLegacyReadDirectoryChangesForTesting)
      : dir_path_(dir_path),
        files_changed_cb_(std::move(files_changed_cb)),
        dir_recreated_cb_(std::move(dir_recreated_cb)),
        timeout_ms_(timeout_ms) {
    // Check whether ReadDirectoryChangesExW is available. It requires Windows
    // 10, version 1709, released October 17, 2017, or Corresponding Windows
    // Server versions.
    if (!enforceLegacyReadDirectoryChangesForTesting) {
      read_directory_changes_ex_ =
          reinterpret_cast<decltype(ReadDirectoryChangesExW)*>(::GetProcAddress(
              ::GetModuleHandle(L"Kernel32.dll"), "ReadDirectoryChangesExW"));
    }

    shutdown_event_ = ScopedHandle(CreateEvent(nullptr, TRUE, FALSE, nullptr));
    if (!shutdown_event_.IsValid()) {
      SetStatus(absl::InternalError(absl::StrFormat(
          "Failed to create shutdown event: '%s'", Util::GetLastWin32Error())));
      return;
    }
    dir_reader_ = std::thread([this]() { WatchDirChanges(); });
  }

  ~AsyncFileWatcher() { Shutdown(); }

  absl::Status GetStatus() const ABSL_LOCKS_EXCLUDED(status_mutex_) {
    absl::MutexLock mutex(&status_mutex_);
    return status_;
  }

  FileMap GetModifiedFiles() ABSL_LOCKS_EXCLUDED(modified_files_mutex_) {
    FileMap files;
    absl::MutexLock mutex(&modified_files_mutex_);
    std::swap(modified_files_, files);

    // Retrieve file stats if ReadDirectoryChangesEx is not available.
    if (!read_directory_changes_ex_ && !files.empty()) {
      Stopwatch sw;
      for (auto& [path, info] : files) {
        if (info.action == FileAction::kDeleted) continue;

        std::string full_path = path::Join(dir_path_, path);
        path::Stats stats;
        absl::Status status = path::GetStats(full_path, &stats);
        if (!status.ok()) {
          LOG_WARNING("Failed to get stats for path '%s'", full_path);
          continue;
        }
        // Don't use the Windows localized timestamp from GetStats.
        time_t mtime;
        status = path::GetFileTime(full_path, &mtime);
        if (!status.ok()) {
          LOG_WARNING("Failed to get modification time for path '%s'",
                      full_path);
          continue;
        }

        info.is_dir = (stats.mode & path::MODE_IFDIR) != 0;
        info.size = stats.size;
        info.mtime = mtime;
      }
      LOG_DEBUG("Time to fix file stats: %0.3f sec.", sw.ElapsedSeconds());
    }

    return files;
  }

  void ClearModifiedFiles() ABSL_LOCKS_EXCLUDED(modified_files_mutex_) {
    absl::MutexLock mutex(&modified_files_mutex_);
    modified_files_.clear();
  }

  uint32_t GetEventCount() const ABSL_LOCKS_EXCLUDED(modified_files_mutex_) {
    absl::MutexLock mutex(&modified_files_mutex_);
    return event_count_;
  }

  uint32_t GetDirRecreateEventCount() const
      ABSL_LOCKS_EXCLUDED(modified_files_mutex_) {
    absl::MutexLock mutex(&modified_files_mutex_);
    return dir_recreate_count_;
  }

  bool IsStarted() const ABSL_LOCKS_EXCLUDED(state_mutex_) {
    absl::MutexLock mutex(&state_mutex_);
    return state_ != FileWatcherState::kDefault &&
           state_ != FileWatcherState::kShuttingDown;
  }

  bool IsWatching() const ABSL_LOCKS_EXCLUDED(state_mutex_) {
    absl::MutexLock mutex(&state_mutex_);
    return state_ == FileWatcherState::kWatching;
  }

  bool IsShuttingDown() const ABSL_LOCKS_EXCLUDED(state_mutex_) {
    absl::MutexLock mutex(&state_mutex_);
    return state_ == FileWatcherState::kShuttingDown;
  }

  void Shutdown() {
    if (shutdown_event_.IsValid() && !SetEvent(shutdown_event_.Get())) {
      LOG_ERROR("SetEvent() for shutdown failed: '%s'",
                Util::GetLastWin32Error());
      exit(1);
    }

    if (dir_reader_.joinable()) {
      dir_reader_.join();
    }
  }

 private:
  // The most important method, which acquires directory handle and reads
  // directory changes. It detects removal, creation, and re-creation of the
  // watched directory. It is robust to the directory change on any level.
  void WatchDirChanges() {
    // TODO: Adjust also if there was no directory at the beginning. Currently,
    // the directory exists; otherwise, ManifestUpdater would fail.
    bool first_run = true, prev_run_was_success = false;
    while (true) {
      ScopedHandle read_event(CreateEvent(nullptr, /* no security attributes */
                                          TRUE,    /* manual-reset event */
                                          FALSE,   /* unsignaled */
                                          nullptr)); /* unnamed event object */
      if (!read_event.IsValid()) {
        SetStatus(absl::InternalError(absl::StrFormat(
            "Failed to create read event: '%s'", Util::GetLastWin32Error())));
        return;
      }

      FILE_BASIC_INFO dir_info;
      absl::StatusOr<ScopedHandle> status = GetValidDirHandle(&dir_info);
      SetStatus(status.status());
      if (status.ok()) {
        // The watched directory exists and its handle is valid.
        if (!first_run) {
          ++dir_recreate_count_;
          if (dir_recreated_cb_) dir_recreated_cb_();
        }
        first_run = false;
        prev_run_was_success = true;
        // Keep reading directory changes. This function only returns once it
        // gets the shutdown signal, the watched directory is removed, or an
        // error occurs while reading file changes.
        ReadDirChanges(*status, dir_info, read_event);
        if (IsShuttingDown()) {
          LOG_DEBUG("Shutting down watching '%s'.", dir_path_);
          return;
        }
        LOG_WARNING("Watched directory '%s' was possibly removed.", dir_path_);
        ClearModifiedFiles();
      } else if (prev_run_was_success) {
        prev_run_was_success = false;
        ++dir_recreate_count_;
        if (dir_recreated_cb_) dir_recreated_cb_();
      }
      // The shutdown event should be caught on both levels: when the
      // watched directory was not removed and when it was recreated. Here
      // the shutdown event is considered when the watched directory itself was
      // removed or created. If a shutdown was triggered, stop watching. The
      // current file-watcher status should not be changed.
      if (IsShuttingDown() ||
          WaitForSingleObject(shutdown_event_.Get(), timeout_ms_) ==
              WAIT_OBJECT_0) {
        LOG_DEBUG("Shutting down watching '%s'.", dir_path_);
        ResetEvent(shutdown_event_.Get());
        return;
      }
    }
  }

  // Creates a file handle for the watched directory and requests its
  // properties. Returns a directory handle or an error if the directory does
  // not exist or its properties could not be retrieved. |dir_info| is an output
  // parameter, this is the basic information about the returned directory
  // handle for the watched directory. It descibes for example creation and
  // modification timestamps.
  absl::StatusOr<ScopedHandle> GetValidDirHandle(
      FILE_BASIC_INFO* dir_info) const {
    assert(dir_info);

    // Create handle for the watched directory.
    ScopedHandle dir_handle(CreateFileW(
        Util::Utf8ToWideStr(dir_path_).c_str(), /* watched directory */
        FILE_LIST_DIRECTORY, /* rights to list the directory */
        FILE_SHARE_READ | FILE_SHARE_WRITE |
            FILE_SHARE_DELETE, /* sharing mode */
        nullptr,               /* security attributes */
        OPEN_EXISTING,         /* opens a file iff exists */
        FILE_FLAG_BACKUP_SEMANTICS |
            FILE_FLAG_OVERLAPPED, /* asynchronous I/O */
        nullptr));                /* no template file */

    // CreateFileW() failed and returned an invalid handle.
    if (!dir_handle.IsValid()) {
      return absl::FailedPreconditionError(
          absl::StrFormat("Could not start watching '%s': '%s'", dir_path_,
                          Util::GetLastWin32Error()));
    }
    // Failed to retrieve basic information about the watched directory.
    if (!GetFileInformationByHandleEx(dir_handle.Get(), FileBasicInfo, dir_info,
                                      sizeof(*dir_info))) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "Could not get information about directory '%s': '%s'", dir_path_,
          Util::GetLastWin32Error()));
    }
    return dir_handle;
  }

  // Reads changes in the watched directory itself.
  // In case of shutdown, the status is ok.
  // In case of any error, an error status is set.
  // It waits for changes in the watched directory, collects those events in
  // |modified_files_|, and notifies the caller about those changes.
  void ReadDirChanges(ScopedHandle& dir_handle, FILE_BASIC_INFO& dir_info,
                      ScopedHandle& read_event) {
    OVERLAPPED overlapped;
    ZeroMemory(&overlapped, sizeof(overlapped));

    // Use FILE_NOTIFY_EXTENDED_INFORMATION if |read_directory_changes_ex_| is
    // available and FILE_NOTIFY_INFORMATION otherwise.
    constexpr size_t alignment =
        std::max<size_t>(alignof(FILE_NOTIFY_EXTENDED_INFORMATION),
                         alignof(FILE_NOTIFY_INFORMATION));
    std::aligned_storage_t<kDefaultBufferSize, alignment> buffer;

    overlapped.hEvent = read_event.Get();
    BOOL res;
    if (read_directory_changes_ex_) {
      res = read_directory_changes_ex_(
          dir_handle.Get(), &buffer, sizeof(buffer),
          true /* check subfolders */, kFileNotificationFlags,
          nullptr /* not needed as async read */, &overlapped,
          nullptr /* no completion routine */,
          ReadDirectoryNotifyExtendedInformation);
    } else {
      res = ReadDirectoryChangesW(
          dir_handle.Get(), &buffer, sizeof(buffer),
          true /* check subfolders */, kFileNotificationFlags,
          nullptr /* not needed as async read */, &overlapped,
          nullptr /* no completion routine */);
    }

    if (res == FALSE && GetLastError() != ERROR_IO_PENDING) {
      SetStatus(absl::InternalError(absl::StrFormat(
          "Could not read changes in the watched directory '%s': '%s'",
          dir_path_, Util::GetLastWin32Error())));
      MaybeSetState(FileWatcherState::kFailed);
      CancelDirIo(dir_handle, Util::Utf8ToWideStr(dir_path_));
      return;
    }

    MaybeSetState(FileWatcherState::kWatching);
    // Initialize handles to watch: changes in |dir_path_| and shutdown
    // events.
    HANDLE watch_handles[] = {overlapped.hEvent, shutdown_event_.Get()};
    size_t read_index = 0;
    size_t shutdown_index = 1;
    while (true) {
      std::aligned_storage_t<kDefaultBufferSize, alignment> moved_buffer;
      const uint32_t wait_index =
          WaitForMultipleObjects(static_cast<DWORD>(std::size(watch_handles)),
                                 watch_handles, false, timeout_ms_);
      if (wait_index == WAIT_TIMEOUT) {
        ScopedHandle dir_handle2(CreateFileW(
            Util::Utf8ToWideStr(dir_path_).c_str(), /* watched directory */
            FILE_LIST_DIRECTORY, /* rights to list the directory */
            FILE_SHARE_READ | FILE_SHARE_WRITE |
                FILE_SHARE_DELETE, /* sharing mode */
            nullptr,               /* security attributes */
            OPEN_EXISTING,         /* opens a file iff exists */
            FILE_FLAG_BACKUP_SEMANTICS |
                FILE_FLAG_OVERLAPPED, /* asynchronous I/O */
            nullptr));                /* no template file */

        FILE_BASIC_INFO dir_info2;
        // The directory was removed. The new one was not created.
        if (!dir_handle.IsValid() || !dir_handle2.IsValid() ||
            !GetFileInformationByHandleEx(dir_handle2.Get(), FileBasicInfo,
                                          &dir_info2, sizeof(dir_info2))) {
          SetStatus(absl::FailedPreconditionError(
              absl::StrFormat("The watched directory was removed '%s': '%s'",
                              dir_path_, Util::GetLastWin32Error())));
          MaybeSetState(FileWatcherState::kFailed);
          break;
        }
        if (dir_info.CreationTime.QuadPart != dir_info2.CreationTime.QuadPart) {
          SetStatus(absl::FailedPreconditionError(absl::StrFormat(
              "The watched directory was recreated '%s'", dir_path_)));
          MaybeSetState(FileWatcherState::kFailed);
          break;
        }
        // Wait for a new event.
        continue;
      }
      const uint32_t handle_index = wait_index - WAIT_OBJECT_0;
      if (handle_index >= std::size(watch_handles)) {
        SetStatus(absl::InvalidArgumentError(absl::StrFormat(
            "WaitForMultipleObjects failed with invalid handle index %u",
            handle_index)));
        MaybeSetState(FileWatcherState::kFailed);
        break;
      }
      if (handle_index == shutdown_index) {
        ResetEvent(shutdown_event_.Get());
        SetStatus(absl::OkStatus());
        MaybeSetState(FileWatcherState::kShuttingDown);
        break;
      }
      if (handle_index == read_index) {
        DWORD read_bytes = 0;
        if (!GetOverlappedResult(dir_handle.Get(), &overlapped, &read_bytes,
                                 TRUE)) {
          ResetEvent(overlapped.hEvent);
          SetStatus(absl::UnavailableError(
              absl::StrFormat("GetOverlappedResult() failed: '%s'",
                              Util::GetLastWin32Error())));
          MaybeSetState(FileWatcherState::kFailed);
          break;
        }
        if (read_bytes == 0) {
          SetStatus(absl::DataLossError(absl::StrFormat(
              "Buffer overflow: no events were read for the directory '%s'",
              dir_path_)));
          MaybeSetState(FileWatcherState::kFailed);
          break;
        }
        std::swap(moved_buffer, buffer);
        ResetEvent(overlapped.hEvent);
        if (read_directory_changes_ex_) {
          res = read_directory_changes_ex_(
              dir_handle.Get(), &buffer, sizeof(buffer),
              true /* check subfolders */, kFileNotificationFlags,
              nullptr /* not needed as async read */, &overlapped,
              nullptr /* no completion routine */,
              ReadDirectoryNotifyExtendedInformation);

          ProcessDirChanges<FILE_NOTIFY_EXTENDED_INFORMATION>(&moved_buffer,
                                                              read_bytes);
        } else {
          res = ReadDirectoryChangesW(
              dir_handle.Get(), &buffer, sizeof(buffer),
              true /* check subfolders */, kFileNotificationFlags,
              nullptr /* not needed as async read */, &overlapped,
              nullptr /* no completion routine */);

          ProcessDirChanges<FILE_NOTIFY_INFORMATION>(&moved_buffer, read_bytes);
        }
        if (res == FALSE && GetLastError() != ERROR_IO_PENDING) {
          SetStatus(absl::FailedPreconditionError(absl::StrFormat(
              "Could not read changes in the watched directory '%s': '%s'",
              dir_path_, Util::GetLastWin32Error())));
          MaybeSetState(FileWatcherState::kFailed);
          break;
        }
      }
    }
    CancelDirIo(dir_handle, Util::Utf8ToWideStr(dir_path_));
    return;
  }

  // Classifies the changes in the watched directory collected till now,
  // deduplicates the changes if needed, and executes the caller's callback.
  // BufferType can be FILE_NOTIFY_EXTENDED_INFORMATION or
  // FILE_NOTIFY_INFORMATION, depending on whether ReadDirectoryChangesExW is
  // available or not.
  template <typename BufferType>
  void ProcessDirChanges(void* buffer, size_t read_bytes)
      ABSL_LOCKS_EXCLUDED(modified_files_mutex_) {
    {
      absl::MutexLock mutex(&modified_files_mutex_);

      assert(buffer);
      size_t offset = 0;
      FileMap modified_files;
      while (offset < read_bytes) {
        auto* info = reinterpret_cast<BufferType*>(
            reinterpret_cast<char*>(buffer) + offset);
        std::string file_name = Util::WideToUtf8Str(std::wstring(
            info->FileName,
            info->FileName + (info->FileNameLength / sizeof(info->FileName))));

        bool was_added = false;
        auto iter = modified_files_.find(file_name);
        if (iter != modified_files_.end())
          was_added = iter->second.action == FileAction::kAdded;

        // Merge the current change into |modified_files_| in a way so that
        // sequences that use temp files (e.g. ADDED - MODIFIED - REMOVED)
        // result in no entry in |modified_files_| at all.
        bool remove = false;
        FileAction new_action = FileAction::kAdded;
        switch (info->Action) {
          case FILE_ACTION_REMOVED:
          case FILE_ACTION_RENAMED_OLD_NAME:
            // If the entry was originally added, remove it again.
            if (was_added)
              remove = true;
            else
              new_action = FileAction::kDeleted;
            break;

          case FILE_ACTION_ADDED:
          case FILE_ACTION_RENAMED_NEW_NAME:
            new_action = FileAction::kAdded;
            break;

          case FILE_ACTION_MODIFIED:
            // Keep the "added" state if the file was originally added.
            new_action = was_added ? FileAction::kAdded : FileAction::kModified;
            break;
        }

        // The file stats are only present if ReadDirectoryChangesEx is present.
        // Otherwise, we'll get them later.
        bool is_dir = false;
        uint64_t size = 0;
        int64_t mtime = 0;
        if constexpr (std::is_same_v<BufferType,
                                     FILE_NOTIFY_EXTENDED_INFORMATION>) {
          is_dir = (info->FileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
          size = static_cast<uint64_t>(info->FileSize.QuadPart);
          mtime = ToUnixTime(info->LastModificationTime);
        }

        if (remove) {
          modified_files_.erase(iter);
        } else {
          // Note: insert_or_assign updates earlier entries.
          modified_files_.insert_or_assign(
              file_name, FileInfo(new_action, is_dir, size, mtime));
        }
        ++event_count_;

        // No further entry exists.
        if (info->NextEntryOffset == 0) break;
        offset += info->NextEntryOffset;
      }
    }

    // Invoke files changed callback if present.
    if (files_changed_cb_) files_changed_cb_();
  }

  void SetStatus(const absl::Status& status)
      ABSL_LOCKS_EXCLUDED(status_mutex_) {
    LOG_DEBUG("Setting status '%s' of the file watcher process",
              status.ToString().c_str());
    absl::MutexLock mutex(&status_mutex_);
    status_ = status;
  }

  // Modifies the file watcher state iff it is not shutting down.
  void MaybeSetState(FileWatcherState state) ABSL_LOCKS_EXCLUDED(state_mutex_) {
    LOG_DEBUG("Setting state %u of the file watcher process", state);
    absl::MutexLock mutex(&state_mutex_);
    if (state_ != FileWatcherState::kShuttingDown) state_ = state;
  }

  std::string dir_path_;  // the path to the watched directory in UTF8.

  ScopedHandle shutdown_event_;  // event to shutdown the watcher.
  std::thread dir_reader_;       // watching thread.

  mutable absl::Mutex status_mutex_;
  absl::Status status_ ABSL_GUARDED_BY(status_mutex_);

  mutable absl::Mutex modified_files_mutex_;
  FileMap modified_files_ ABSL_GUARDED_BY(modified_files_mutex_);
  uint32_t event_count_ ABSL_GUARDED_BY(modified_files_mutex_) = 0;
  uint32_t dir_recreate_count_ ABSL_GUARDED_BY(modified_files_mutex_) = 0;

  mutable absl::Mutex state_mutex_;
  FileWatcherState state_ ABSL_GUARDED_BY(state_mutex_) =
      FileWatcherState::kDefault;  // the current watcher state.

  // Pointer to ReadDirectoryChangesExW function if available.
  decltype(ReadDirectoryChangesExW)* read_directory_changes_ex_ = nullptr;

  FilesChangedCb files_changed_cb_;  // callback to react on modifications
                                     // inside the watched directory.
  DirRecreatedCb dir_recreated_cb_;  // callback to react on the
                                     // creation/removal/re-creation of the
                                     // watched directory. It is not guarantee
                                     // that the watched directory exists.
  unsigned int timeout_ms_;          // timeout in ms to wait for a change.
};

FileWatcherWin::FileWatcherWin(std::string directory) : dir_path_(directory) {}

FileWatcherWin::~FileWatcherWin() {
  absl::Status status = StopWatching();
  if (!status.ok()) {
    LOG_WARNING("Failed to stop watching files: %s", status.ToString());
  }
}

FileWatcherWin::FileMap FileWatcherWin::GetModifiedFiles() {
  absl::MutexLock mutex(&modified_files_mutex_);
  FileMap files;
  modified_files_.swap(files);
  if (async_watcher_) {
    for (const auto& [path, info] : async_watcher_->GetModifiedFiles())
      files.insert_or_assign(path, info);
  }
  return files;
}

absl::Status FileWatcherWin::StartWatching(FilesChangedCb files_changed_cb,
                                           DirRecreatedCb dir_recreated_cb,
                                           unsigned int timeout_ms) {
  LOG_INFO("Starting the file watcher");
  async_watcher_ = std::make_unique<AsyncFileWatcher>(
      dir_path_, std::move(files_changed_cb), std::move(dir_recreated_cb),
      timeout_ms, enforceLegacyReadDirectoryChangesForTesting_);
  while (GetStatus().ok() && !IsStarted()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return GetStatus();
}

absl::Status FileWatcherWin::StopWatching() {
  LOG_INFO("Stopping the file watcher");
  if (!async_watcher_) {
    return absl::OkStatus();
  }
  async_watcher_->Shutdown();
  absl::MutexLock mutex(&modified_files_mutex_);
  FileMap files = async_watcher_->GetModifiedFiles();
  if (modified_files_.empty()) {
    modified_files_.swap(files);
  } else {
    for (const auto& [path, info] : files)
      modified_files_.insert_or_assign(path, info);
  }
  absl::Status async_status = async_watcher_->GetStatus();
  async_watcher_.reset();
  return async_status;
}

bool FileWatcherWin::IsStarted() const {
  return async_watcher_ ? async_watcher_->IsStarted() : false;
}

bool FileWatcherWin::IsWatching() const {
  return async_watcher_ ? async_watcher_->IsWatching() : false;
}

absl::Status FileWatcherWin::GetStatus() const {
  return async_watcher_ ? async_watcher_->GetStatus() : absl::OkStatus();
}

uint32_t FileWatcherWin::GetEventCountForTesting() const {
  return async_watcher_ ? async_watcher_->GetEventCount() : 0;
}

uint32_t FileWatcherWin::GetDirRecreateEventCountForTesting() const {
  return async_watcher_ ? async_watcher_->GetDirRecreateEventCount() : 0;
}

void FileWatcherWin::EnforceLegacyReadDirectoryChangesForTesting() {
  assert(!IsStarted());
  enforceLegacyReadDirectoryChangesForTesting_ = true;
}

}  // namespace cdc_ft
