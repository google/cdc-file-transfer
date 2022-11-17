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

#include "data_store/disk_data_store.h"

#include <filesystem>
#include <memory>

#include "common/log.h"
#include "common/path.h"
#include "common/status.h"
#include "common/status_macros.h"

namespace cdc_ft {
namespace {

static constexpr char kDirNames[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                       '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

// Generates directory names of |length| symbols from kDirNames.
// If length = 2, the names are 00, 01, 02, etc.
std::vector<std::string> GenerateDirNames(size_t length) {
  size_t names_size = 1ull << (length * 4);
  std::vector<std::string> names(names_size, std::string(length, '0'));
  for (size_t idx = 0; idx < names_size; ++idx) {
    size_t symbol = idx;
    for (size_t jdx = 0; jdx < length; ++jdx) {
      names[idx][jdx] = kDirNames[symbol & 0xfu];
      symbol >>= 4;
    }
  }
  return names;
}

// Adds |count| path separators to |input| after each |distance| symbols
// starting from the beginning. At least one symbol is left at the end
// for the file name.
// AddSeparators("abc", 1, 3) -> a\b\c
// AddSeparators("abc", 1, 0) -> abc
// AddSeparators("abc", 2, 100) -> ab\c
static std::string AddPathSeparators(const std::string& input, size_t distance,
                                     size_t count) {
  if (input.empty() || distance == 0 || count == 0) {
    return input;
  }
  count = std::min((input.size() - 1) / distance, count);
  std::string path;
  path.reserve(input.size() + count);
  std::string::const_iterator it_pos = input.begin();
  while (count > 0 && it_pos < input.end()) {
    path.append(it_pos, it_pos + distance);
    path.push_back(path::PathSeparator());
    it_pos += distance;
    --count;
  }
  if (it_pos < input.end()) {
    path.append(it_pos, input.end());
  }
  return path;
}
}  // namespace

DiskDataStore::DiskDataStore(unsigned int depth, std::string cache_root_dir,
                             bool create_dirs, SystemClock* clock)
    : depth_(depth),
      root_dir_(std::move(cache_root_dir)),
      create_dirs_(create_dirs),
      clock_(clock) {
  assert(!root_dir_.empty());
  path::EnsureEndsWithPathSeparator(&root_dir_);
}

absl::StatusOr<std::unique_ptr<DiskDataStore>> DiskDataStore::Create(
    unsigned int depth, std::string cache_root_dir, bool create_dirs,
    SystemClock* clock) {
  // Resolve e.g. ~.
  RETURN_IF_ERROR(path::ExpandPathVariables(&cache_root_dir),
                  "Failed to expand cache dir '%s'", cache_root_dir);
  std::unique_ptr<DiskDataStore> store = absl::WrapUnique(
      new DiskDataStore(depth, std::move(cache_root_dir), create_dirs, clock));
  if (create_dirs) {
    RETURN_IF_ERROR(store->CreateDirHierarchy());
  }
  return store;
}

DiskDataStore::~DiskDataStore() {}

absl::Status DiskDataStore::Put(const ContentIdProto& content_id,
                                const void* data, size_t size) {
  std::string path = GetCacheFilePath(content_id);
  if (!create_dirs_) {
    RETURN_IF_ERROR(path::CreateDirRec(path::DirName(path)));
  }
  RETURN_IF_ERROR(path::WriteFile(path, data, size));
  UpdateModificationTime(path);
  size_.fetch_add(size, std::memory_order_relaxed);
  return absl::OkStatus();
}

absl::StatusOr<size_t> DiskDataStore::Get(const ContentIdProto& content_id,
                                          void* data, size_t offset,
                                          size_t size) {
  if (!size) return 0;
  assert(data);
  std::string path = GetCacheFilePath(content_id);
  size_t read_size;
  ASSIGN_OR_RETURN(read_size, path::ReadFile(path, data, offset, size),
                   "Failed to read chunk %s of size %d at offset %d",
                   ContentId::ToHexString(content_id), size, offset);
  UpdateModificationTime(path);
  return read_size;
}

absl::Status DiskDataStore::Get(const ContentIdProto& content_id,
                                Buffer* data) {
  assert(data);
  std::string path = GetCacheFilePath(content_id);
  size_t read_size = 0;
  size_t file_size = 0;

  RETURN_IF_ERROR(path::FileSize(path, &file_size),
                  "Failed to stat file size for '%s'", path);
  data->resize(file_size);
  ASSIGN_OR_RETURN(read_size, path::ReadFile(path, data->data(), 0, file_size),
                   "Failed to read %s of size %d",
                   ContentId::ToHexString(content_id), file_size);
  if (read_size != file_size) {
    return absl::DataLossError(
        absl::StrFormat("Only %u bytes out of %u are read for %s", read_size,
                        file_size, ContentId::ToHexString(content_id)));
  }
  UpdateModificationTime(path);
  return absl::OkStatus();
}

int64_t DiskDataStore::Capacity() const { return capacity_; }

double DiskDataStore::FillFactor() const { return fill_factor_; }

unsigned int DiskDataStore::Depth() const { return depth_; }

size_t DiskDataStore::Size() const { return size_; }

const std::string& DiskDataStore::RootDir() const { return root_dir_; }

void DiskDataStore::SetCapacity(int64_t capacity) { capacity_ = capacity; }

absl::Status DiskDataStore::SetFillFactor(double fill_factor) {
  if (fill_factor <= 0 || fill_factor > 1) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Failed to set cache fill factor to %f.", fill_factor));
  }
  fill_factor_ = fill_factor;
  return Cleanup();
}

absl::Status DiskDataStore::Wipe() {
  RETURN_IF_ERROR(path::RemoveDirRec(root_dir_),
                  "RemoveDirRec() for '%s' failed", root_dir_);
  size_ = 0;
  if (create_dirs_) {
    RETURN_IF_ERROR(CreateDirHierarchy());
  }
  return absl::OkStatus();
}

absl::Status DiskDataStore::Prune(
    std::unordered_set<ContentIdProto> ids_to_keep) {
  CacheFilesWithSize files_with_size;
  ASSIGN_OR_RETURN(files_with_size, CollectCacheFiles(),
                   "Failed to collect cache files");

  // Delete the set of chunks not in |ids_to_keep|.
  std::vector<ContentIdProto> to_delete;
  for (const CacheFile& file : files_with_size.files) {
    // Don't touch files that don't match the chunk naming scheme
    // (e.g. user-added files).
    ContentIdProto id;
    if (!ParseCacheFilePath(std::move(file.path), &id)) continue;

    if (ids_to_keep.find(id) == ids_to_keep.end()) {
      RETURN_IF_ERROR(Remove(id));
      size_.fetch_sub(file.size, std::memory_order_relaxed);
    } else {
      ids_to_keep.erase(id);
    }
  }

  // Verify that all chunks in |ids_to_keep| are present in the cache.
  if (!ids_to_keep.empty()) {
    return absl::NotFoundError(absl::StrFormat(
        "%u chunks, e.g. '%s', not found in the store", ids_to_keep.size(),
        ContentId::ToHexString(*ids_to_keep.begin())));
  }
  return absl::OkStatus();
}

absl::Status DiskDataStore::Remove(const ContentIdProto& content_id) {
  std::string path = GetCacheFilePath(content_id);
  return path::RemoveFile(path);
}

bool DiskDataStore::Contains(const ContentIdProto& content_id) {
  return path::Exists(GetCacheFilePath(content_id));
}

absl::Status DiskDataStore::Cleanup() {
  if (capacity_ < 0) {
    return absl::OkStatus();
  }

  size_t size_threshold = static_cast<size_t>(capacity_) * fill_factor_;
  if (size_initialized_.load() && size_ <= size_threshold) {
    return absl::OkStatus();
  }
  CacheFilesWithSize files_with_size;
  ASSIGN_OR_RETURN(files_with_size, CollectCacheFiles());
  LOG_DEBUG("Cache size before the cleanup: %u bytes", size_.load());
  std::vector<CacheFile>& files = files_with_size.files;
  // Sort in the LRU order: the old files stored first.
  std::sort(files.begin(), files.end(),
            [](const CacheFile& file1, const CacheFile& file2) {
              // Also sort by path for deterministic results in tests.
              if (file1.mtime == file2.mtime) return file1.path < file2.path;
              return file1.mtime < file2.mtime;
            });
  size_t file_index = 0;
  const size_t num_of_files = files.size();
  while (size_ > size_threshold && file_index < num_of_files) {
    std::string path = path::Join(root_dir_, files[file_index].path);
    RETURN_IF_ERROR(path::RemoveFile(path));
    size_.fetch_sub(files[file_index].size, std::memory_order_relaxed);
    ++file_index;
    if (interrupt_ && *interrupt_) {
      return absl::CancelledError("Cache cleanup has been cancelled");
    }
  }
  LOG_DEBUG("Cache size after the cleanup: %u bytes", size_.load());
  return absl::OkStatus();
}

absl::StatusOr<std::vector<ContentIdProto>> DiskDataStore::List() {
  CacheFilesWithSize files_with_size;
  ASSIGN_OR_RETURN(files_with_size, CollectCacheFiles(true),
                   "Failed to collect cache files");

  std::vector<ContentIdProto> ids;
  ids.reserve(files_with_size.files.size());
  for (const CacheFile& file : files_with_size.files) {
    ContentIdProto id;
    if (ParseCacheFilePath(std::move(file.path), &id))
      ids.push_back(std::move(id));
  }

  return ids;
}

absl::StatusOr<DiskDataStore::Statistics> DiskDataStore::CalculateStatistics()
    const {
  Statistics statistics;
  auto handler = [&](const std::string& dir, const std::string& filename,
                     int64_t /*modified_time*/, uint64_t size,
                     bool is_directory) -> absl::Status {
    if (!is_directory) {
      statistics.size += size;
      ++statistics.number_of_chunks;
    }
    return absl::OkStatus();
  };
  RETURN_IF_ERROR(path::SearchFiles(root_dir_, true, handler));
  return statistics;
}

absl::StatusOr<DiskDataStore::CacheFilesWithSize>
DiskDataStore::CollectCacheFiles(bool continue_on_interrupt) {
  CacheFilesWithSize cache_files;

  if (!path::DirExists({root_dir_})) return cache_files;

  auto handler = [&](const std::string& dir, const std::string& filename,
                     int64_t modified_time, uint64_t size,
                     bool is_directory) -> absl::Status {
    if (!is_directory) {
      cache_files.files.emplace_back();
      cache_files.files.back().path =
          path::Join(dir.substr(root_dir_.size()), filename);
      cache_files.files.back().mtime = modified_time;
      cache_files.files.back().size = size;
      cache_files.size += size;
    }
    if (!continue_on_interrupt && interrupt_ && *interrupt_) {
      return absl::CancelledError("Cache cleanup has been cancelled");
    }
    return absl::OkStatus();
  };

  RETURN_IF_ERROR(path::SearchFiles(root_dir_, true, handler));
  size_ = cache_files.size;
  size_initialized_ = true;
  return cache_files;
}

std::string DiskDataStore::GetCacheFilePath(
    const ContentIdProto& content_id) const {
  std::string file_name = AddPathSeparators(ContentId::ToHexString(content_id),
                                            kDirNameLength, depth_);
  return path::Join(root_dir_, file_name);
}

bool DiskDataStore::ParseCacheFilePath(std::string path,
                                       ContentIdProto* content_id) const {
  // Remove path separators.
  if (depth_ > 0) {
    path.erase(std::remove_if(path.begin(), path.end(),
                              [](char c) {
                                return c == path::PathSeparator() ||
                                       c == path::OtherPathSeparator();
                              }),
               path.end());
  }
  return ContentId::FromHexString(path, content_id);
}

void DiskDataStore::UpdateModificationTime(const std::string& path) {
  // Don't fail if the time cannot be modified.
  // The time might be updated in parallel, so it is not critical.
  path::SetFileTime(path, std::chrono::system_clock::to_time_t(clock_->Now()))
      .IgnoreError();
}

absl::Status DiskDataStore::CreateDirHierarchy() {
  if (dirs_.empty() && depth_ > 0) {
    dirs_ = GenerateDirNames(kDirNameLength);
  }
  RETURN_IF_ERROR(path::CreateDirRec(root_dir_));
  return CreateDirLevelRec(root_dir_, depth_);
}

absl::Status DiskDataStore::CreateDirLevelRec(const std::string& parent,
                                              unsigned int depth) {
  if (depth == 0) {
    return absl::OkStatus();
  }
  for (const std::string& dir : dirs_) {
    std::string name = path::Join(parent, dir);
    RETURN_IF_ERROR(path::CreateDir(name));
    RETURN_IF_ERROR(CreateDirLevelRec(name, depth - 1),
                    "CreateDirLevelRec() for %s failed at level %d:", name,
                    depth - 1);
  }
  return absl::OkStatus();
}
};  // namespace cdc_ft
