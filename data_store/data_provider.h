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

#ifndef DATA_STORE_DATA_PROVIDER_H_
#define DATA_STORE_DATA_PROVIDER_H_

#include <atomic>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "common/clock.h"
#include "data_store/data_store_reader.h"
#include "data_store/data_store_writer.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// DataProvider is a composite of several data-store readers used for the file
// transfer. Thread-safe.
class DataProvider : public DataStoreReader {
 public:
  // Default cleanup interval in seconds.
  static constexpr unsigned int kCleanupTimeoutSec = 300;
  // Default access-idling time in seconds.
  static constexpr int64_t kAccessIdleSec = 5;

  DataProvider(std::unique_ptr<DataStoreWriter> writer,
               std::vector<std::unique_ptr<DataStoreReader>> readers,
               size_t prefetch_size,
               uint32_t cleanup_timeout_sec = kCleanupTimeoutSec,
               uint32_t access_idle_timeout_sec = kAccessIdleSec);
  DataProvider() = delete;
  DataProvider(const DataProvider&) = delete;
  DataProvider& operator=(const DataProvider&) = delete;
  virtual ~DataProvider() ABSL_LOCKS_EXCLUDED(shutdown_mutex_);

  // Shuts down the background cleanup thread.
  void Shutdown();

  // DataStoreReader:
  size_t PrefetchSize(size_t read_size) const override;
  absl::StatusOr<size_t> Get(const ContentIdProto& content_id, void* data,
                             size_t offset, size_t size)
      ABSL_LOCKS_EXCLUDED(*content_mutexes_) override;
  absl::Status Get(ChunkTransferList* chunks)
      ABSL_LOCKS_EXCLUDED(*content_mutexes_) override;
  absl::Status Get(const ContentIdProto& content_id, Buffer* data)
      ABSL_LOCKS_EXCLUDED(*content_mutexes_) override;

 private:
  friend class DataProviderTest;

  // Returns whether the writer was cleaned up and resets |is_cleaned_|.
  bool WaitForCleanupAndResetForTesting(absl::Duration timeout)
      ABSL_LOCKS_EXCLUDED(cleaned_mutex_);

  // Vector of WriterMutexLock pointers to lock multiple mutexes together.
  using WriterMutexLockList =
      std::vector<std::unique_ptr<absl::WriterMutexLock>>;

  // Logs a warning if unexpectedly could not get data from the writer.
  void LogWriterWarning(const absl::Status& status,
                        const ContentIdProto& content_id);

  // Returns the mutex for |content_id| from |content_mutexes_|.
  absl::Mutex* GetContentMutex(const ContentIdProto& content_id);

  // Acquires write locks on the corresponding mutexes for all content IDs in
  // |chunk_ids|. The locks are placed in the |locks| list. Detects if two chunk
  // IDs are guarded by the same mutex and locks it only once.
  //
  // The list of mutexes is sorted in a deterministic way before they are
  // locked. This prevents cycles when calling this function from multiple
  // threads and thus avoids deadlocks.
  void WriteLockAll(std::vector<const ContentIdProto*> chunk_ids,
                    WriterMutexLockList* locks);

  // Tries to fulfill as many of the chunk transfer tasks in |chunks| as
  // possible. Tasks that are completed are marked as `done`. If |lock_required|
  // is true, a read lock is acquired for each chunk as its read. Otherwise the
  // caller is responsible for acquiring all required locks beforehand.
  absl::Status GetFromWriter(ChunkTransferList* chunks, bool lock_required);

  // Collects locks for all mutexes.
  void LockAllMutexes(WriterMutexLockList* locks)
      ABSL_LOCKS_EXCLUDED(*content_mutexes_);

  // Periodically cleans up data in |writer_|.
  void CleanupThreadMain() ABSL_LOCKS_EXCLUDED(shutdown_mutex_, cleaned_mutex_);

  // Returns the current time of |steady_clock_| in seconds.
  int64_t GetSteadyNowSec();

  static constexpr unsigned int kNumberOfMutexes = 256;

  // How much additional data to prefetch when a max. FUSE read is encountered.
  size_t prefetch_size_;

  std::unique_ptr<DataStoreWriter> writer_;
  std::vector<std::unique_ptr<DataStoreReader>> readers_;

  // Array of mutexes to protect read/write operations.
  absl::Mutex content_mutexes_[kNumberOfMutexes];

  // Runs periodical cleanup of the data writer.
  std::unique_ptr<std::thread> async_cleaner_;

  absl::Mutex shutdown_mutex_;

  // Indicates whether the shutdown was triggered.
  bool shutdown_ ABSL_GUARDED_BY(shutdown_mutex_) = false;

  // The last access time in seconds since construction. Note that for some
  // compilers we can't use std::chrono::time_point with atomics, so keep the
  // time in seconds.
  std::atomic<int64_t> last_access_sec_;

  // Identifies if new data was added to the cache since the last cleanup.
  std::atomic<bool> chunks_updated_;

  // Clock to track the last access time.
  SteadyClock* steady_clock_ = DefaultSteadyClock::GetInstance();
  const std::chrono::time_point<std::chrono::steady_clock> first_now_ =
      steady_clock_->Now();

  // Cleanup interval.
  uint32_t cleanup_timeout_sec_ = kCleanupTimeoutSec;

  // The number of seconds needs to pass since the last write or read operation
  // to mark the data provider as access-idling.
  uint32_t access_idle_timeout_sec_ = kAccessIdleSec;

  absl::Mutex cleaned_mutex_;

  // Whether the writer was cleaned up since the last time
  // WaitForCleanupAndResetForTesting() was executed or since beginning.
  bool is_cleaned_ ABSL_GUARDED_BY(cleaned_mutex_) = false;

  // Shows whether any read/write request arrived during Cleanup().
  // data_writer_ only reads it and cancels Cleanup() if it is true.
  // It is set in GetContentMutex() and reset at the end of Get().
  std::atomic<bool> interrupt_;
};  // class DataProvider
};  // namespace cdc_ft

#endif  // DATA_STORE_DATA_PROVIDER_H_
