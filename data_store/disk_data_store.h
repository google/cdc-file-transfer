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

#ifndef DATA_STORE_DISK_DATA_STORE_H_
#define DATA_STORE_DISK_DATA_STORE_H_

#include <atomic>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/buffer.h"
#include "common/clock.h"
#include "common/platform.h"
#include "data_store/data_store_writer.h"
#include "manifest/content_id.h"

namespace cdc_ft {

// File-based LRU cache to store data chunks on disk. The LRU strategy is based
// on each file's mtime, which gets updated on each access.
// Not thread-safe.
class DiskDataStore : public DataStoreWriter {
 public:
  struct Statistics {
    size_t size = 0;
    size_t number_of_chunks = 0;
  };

  static constexpr uint64_t kDefaultCapacity{150ull << 30};  // 150 GiB

  // Creates and returns a DiskDataStore that generates the cache directory
  // hierarchy in |cache_root_dir| of |depth| at startup if |create_dirs| is
  // set.
  // Returns an error status if the cache directories cannot be created.
  // Uses |clock| as an internal clock for the file modification times.
  static absl::StatusOr<std::unique_ptr<DiskDataStore>> Create(
      unsigned int depth, std::string cache_root_dir, bool create_dirs,
      SystemClock* clock = DefaultSystemClock::GetInstance());

  DiskDataStore(const DiskDataStore& other) = delete;
  DiskDataStore& operator=(const DiskDataStore& other) = delete;

  ~DiskDataStore();

  // DataStoreReader:
  absl::StatusOr<size_t> Get(const ContentIdProto& content_id, void* data,
                             size_t offset, size_t size) override;
  absl::Status Get(const ContentIdProto& content_id, Buffer* data) override;

  // DataStoreWriter:
  absl::Status Put(const ContentIdProto& content_id, const void* data,
                   size_t size) override;
  absl::Status Remove(const ContentIdProto& content_id) override;
  absl::Status Wipe() override;
  absl::Status Prune(std::unordered_set<ContentIdProto> ids_to_keep) override;
  bool Contains(const ContentIdProto& content_id) override;
  // Removes chunks in the LRU order if the cache size exceeds its capacity.
  // Cleans the cache up until its size drops below the cache capacity
  // limited by the fill factor (capacity * fill factor).
  absl::Status Cleanup() override;

  // Returns a list of all contained content ids independent of |interrupt_|.
  absl::StatusOr<std::vector<ContentIdProto>> List();

  // Returns the defined cache capacity in bytes.
  // If 0, the cache is disabled.
  // If < 0, the disk space is not limited and the whole disk can be used for
  // storing data in the cache.
  int64_t Capacity() const;

  // Returns the fill factor that defines the maximum portion of the capacity,
  // which can be occupied by the cache after cleanup.
  double FillFactor() const;

  // Returns the depth of the hierarchy of the cache directories.
  unsigned int Depth() const;

  // Returns the current total |size_| of the stored data.
  size_t Size() const;

  // Returns the path to the root cache directory.
  const std::string& RootDir() const;

  // Sets the cache capacity in bytes.
  // No cleanup is performed.
  void SetCapacity(int64_t capacity);

  // Sets the cache fill factor.
  // |factor| should be a positive number (0,1].
  absl::Status SetFillFactor(double factor);

  // Calculates cache statistics including the total amount of disk space used
  // for storing chunks measured in bytes and the number of chunks.
  // Returns an error, if the size could not be calculated.
  // This is an expensive operation.
  absl::StatusOr<Statistics> CalculateStatistics() const;

  // The number of symbols in the cache's directory names.
  static constexpr int kDirNameLength = 2;

 private:
  friend class DataProviderTest;

  struct CacheFile {
    std::string path;
    int64_t mtime = 0;
    size_t size = 0;

    void swap(CacheFile& other) {
      std::swap(path, other.path);
      std::swap(mtime, other.mtime);
      std::swap(size, other.size);
    }
  };

  struct CacheFilesWithSize {
    size_t size = 0;
    std::vector<CacheFile> files;
  };

  DiskDataStore(unsigned int depth, std::string cache_root_dir,
                bool create_dirs, SystemClock* clock);

  // Returns a vector of CacheFile with their total size in bytes.
  // In addition, initializes the size if the method succeeds.
  // Returns an error status, if an error occured.
  // |continue_on_interrupt| shows whether the method should be cancelled on new
  // read/write requests.
  absl::StatusOr<CacheFilesWithSize> CollectCacheFiles(
      bool continue_on_interrupt = false);

  // Returns the path to the file, which stores the data chunk for |content_id|.
  std::string GetCacheFilePath(const ContentIdProto& content_id) const;

  // Parses the chunk file |path| into its content id if possible.
  // |path| is expected to look similar to "aa/bb/ccddeeff...".
  // Returns false if parsing fails.
  bool ParseCacheFilePath(std::string path, ContentIdProto* content_id) const;

  // Updates modification time of |path|.
  void UpdateModificationTime(const std::string& path);

  // Creates the cache directory hierarchy.
  absl::Status CreateDirHierarchy();

  // Creates cache directories in the |parent| on the level |depth| recursively.
  absl::Status CreateDirLevelRec(const std::string& parent, unsigned int depth);

  // When the cache is cleaned up, it is advantageous to make some more space
  // available for new chunks and not only to clean the redundant chunks up,
  // which make the cache exceed its capacity.
  static constexpr double kDefaultFillFactor = 0.8;

  const unsigned int depth_;
  std::string root_dir_;
  const bool create_dirs_;
  const SystemClock* clock_;

  std::atomic<int64_t> capacity_{kDefaultCapacity};
  std::atomic<double> fill_factor_{kDefaultFillFactor};

  // The total data size is updated at Put(), Prune(), Wipe(), and Cleanup().
  // It is not guaranteed to be correct between cleanups:
  // - Put() does not consider the size of the file metadata.
  // - before the first Cleanup if the cache had already some data stored on the
  // disk from previous AS runs.
  std::atomic<size_t> size_{0};

  // Shows if the |size_| was already initialized correctly in the Cleanup().
  // Without it, Cleanup() can be skipped if some new data has been written
  // before the first Cleanup() took place.
  std::atomic<bool> size_initialized_{false};

  std::vector<std::string> dirs_;
};  // class DiskDataStore

};      // namespace cdc_ft
#endif  // DATA_STORE_DISK_DATA_STORE_H_
