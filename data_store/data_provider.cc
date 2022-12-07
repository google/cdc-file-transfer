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

#include "data_store/data_provider.h"

#include <algorithm>
#include <thread>

#include "absl/strings/str_format.h"
#include "common/log.h"
#include "common/status.h"
#include "common/stopwatch.h"
#include "manifest/content_id.h"

namespace cdc_ft {
namespace {

// FUSE limits the maximum read request size to 128k. Larger requests will be
// split up into smaller requests up to at most this size. This constant can
// be used to identify max. size requests.
constexpr uint64_t kMaxFuseRequestSize = 1 << 17;

}  // namespace

DataProvider::DataProvider(
    std::unique_ptr<DataStoreWriter> writer,
    std::vector<std::unique_ptr<DataStoreReader>> readers, size_t prefetch_size,
    uint32_t cleanup_timeout_sec, uint32_t access_idle_timeout_sec)
    : prefetch_size_(prefetch_size),
      writer_(std::move(writer)),
      readers_(std::move(readers)),
      chunks_updated_{true},
      cleanup_timeout_sec_(cleanup_timeout_sec),
      access_idle_timeout_sec_(access_idle_timeout_sec) {
  if (writer_) {
    assert(!async_cleaner_);
    async_cleaner_ =
        std::make_unique<std::thread>([this]() { CleanupThreadMain(); });
  }
}

DataProvider::~DataProvider() { Shutdown(); }

void DataProvider::Shutdown() {
  {
    absl::MutexLock lock(&shutdown_mutex_);
    shutdown_ = true;
  }
  if (async_cleaner_) {
    if (async_cleaner_->joinable()) async_cleaner_->join();
    async_cleaner_.reset();
  }
}

size_t DataProvider::PrefetchSize(size_t read_size) const {
  // If the read size matches the maximum FUSE request size, it is very likely
  // that the next chunk is needed as well, so we enlarge the read size by the
  // prefetch size.
  if (read_size == kMaxFuseRequestSize) read_size += prefetch_size_;
  return read_size;
}

absl::StatusOr<size_t> DataProvider::Get(const ContentIdProto& content_id,
                                         void* data, size_t offset,
                                         size_t size) {
  last_access_sec_ = GetSteadyNowSec();
  absl::Mutex* content_mutex = GetContentMutex(content_id);
  absl::StatusOr<size_t> read_bytes;
  if (writer_) {
    {
      absl::ReaderMutexLock read_lock(content_mutex);
      read_bytes = writer_->Get(content_id, data, offset, size);
    }
    if (read_bytes.ok()) {
      return read_bytes;
    }
    LogWriterWarning(read_bytes.status(), content_id);
  }
  // To prevent reading the same chunk from multiple threads, make read/write
  // atomic.
  absl::WriterMutexLock write_lock(content_mutex);
  // Read from the writer_ again, in case the cache has been populated by
  // another thread.
  if (writer_ && absl::IsNotFound(read_bytes.status())) {
    read_bytes = writer_->Get(content_id, data, offset, size);
    if (read_bytes.ok()) {
      return read_bytes;
    }
    LogWriterWarning(read_bytes.status(), content_id);
  }
  for (auto& reader : readers_) {
    Buffer buffer;
    absl::Status status = reader->Get(content_id, &buffer);
    if (!status.ok()) {
      // Try next reader if this one doesn't contain the chunk.
      if (absl::IsNotFound(status)) continue;
      // TODO: Add reader identification for debugging.
      return WrapStatus(status, "Failed to get '%s'.",
                        ContentId::ToHexString(content_id));
    }
    if (writer_) {
      status = writer_->Put(content_id, buffer.data(), buffer.size());
      chunks_updated_ = true;
      if (!status.ok()) {
        LOG_ERROR("Failed to write chunk '%s': %s.",
                  ContentId::ToHexString(content_id), status.ToString());
      }
    }
    if (buffer.size() <= offset) return 0;
    size_t return_bytes = std::min(buffer.size() - offset, size);
    memcpy(data, buffer.data() + offset, return_bytes);
    return return_bytes;
  }
  return absl::NotFoundError(absl::StrFormat(
      "Failed to find %s.", ContentId::ToHexString(content_id)));
}

absl::Status DataProvider::Get(ChunkTransferList* chunks) {
  last_access_sec_ = GetSteadyNowSec();
  // Try to fetch chunks from the cache first.
  RETURN_IF_ERROR(GetFromWriter(chunks, /*lock_required=*/true));
  if (chunks->ReadDone()) return absl::OkStatus();

  // Get list of all missing chunk IDs.
  std::vector<const ContentIdProto*> chunk_ids;
  for (const ChunkTransferTask& chunk : *chunks) {
    if (!chunk.done) chunk_ids.push_back(&chunk.id);
  }

  // Acquire writer locks for all missing chunks.
  WriterMutexLockList locks;
  WriteLockAll(std::move(chunk_ids), &locks);

  // Read from the |writer_| again, in case the cache has been populated by
  // another thread. We hold all chunk locks already.
  RETURN_IF_ERROR(GetFromWriter(chunks, /*lock_required=*/false));
  if (chunks->ReadDone()) return absl::OkStatus();

  // Try to read from all readers.
  for (auto& reader : readers_) {
    absl::Status status = reader->Get(chunks);
    if (!status.ok()) {
      // TODO: Add reader identification for debugging.
      return WrapStatus(status, "Failed to get chunks [%s] from list [%s]",
                        chunks->UndoneToHexString(), chunks->ToHexString());
    }
    if (chunks->PrefetchDone()) break;
  }

  // Cache complete chunks in the writer.
  if (writer_) {
    for (ChunkTransferTask& chunk : *chunks) {
      if (!chunk.done || chunk.chunk_data.empty()) continue;
      absl::Status status = writer_->Put(chunk.id, chunk.chunk_data.data(),
                                         chunk.chunk_data.size());
      chunks_updated_ = true;
      if (!status.ok()) {
        LOG_WARNING("Failed to put '%s' to writer: %s.",
                    ContentId::ToHexString(chunk.id), status.message());
      }
    }
  }
  return absl::OkStatus();
}

absl::Status DataProvider::Get(const ContentIdProto& content_id, Buffer* data) {
  last_access_sec_ = GetSteadyNowSec();
  absl::Mutex* content_mutex = GetContentMutex(content_id);
  absl::Status status = absl::OkStatus();
  if (writer_) {
    {
      absl::ReaderMutexLock read_lock(content_mutex);
      status = writer_->Get(content_id, data);
    }
    if (status.ok()) {
      return absl::OkStatus();
    }
    LogWriterWarning(status, content_id);
  }

  // To prevent reading the same chunk from multiple threads, make read/write
  // atomic.
  absl::WriterMutexLock write_lock(content_mutex);
  // Read from the writer_ again, in case the cache has been populated by
  // another thread.
  if (writer_ && absl::IsNotFound(status)) {
    status = writer_->Get(content_id, data);
    if (status.ok()) {
      return absl::OkStatus();
    }
    LogWriterWarning(status, content_id);
  }
  for (auto& reader : readers_) {
    status = reader->Get(content_id, data);
    if (!status.ok()) {
      // Try next reader if this one doesn't contain the chunk.
      if (absl::IsNotFound(status)) continue;
      // TODO: Add reader identification for debugging.
      return WrapStatus(status, "Failed to get '%s'.",
                        ContentId::ToHexString(content_id));
    }
    if (writer_) {
      writer_->Put(content_id, data->data(), data->size()).IgnoreError();
      chunks_updated_ = true;
    }
    return absl::OkStatus();
  }
  return absl::NotFoundError(absl::StrFormat(
      "Failed to find '%s'.", ContentId::ToHexString(content_id)));
}

void DataProvider::LogWriterWarning(const absl::Status& status,
                                    const ContentIdProto& content_id) {
  if (!absl::IsNotFound(status)) {
    LOG_WARNING("Failed to get '%s' from writer: %s.",
                ContentId::ToHexString(content_id), status.message());
  }
}

absl::Mutex* DataProvider::GetContentMutex(const ContentIdProto& content_id) {
  interrupt_ = true;
  uint8_t id = ContentId::GetByte(content_id, 0);
  return &content_mutexes_[id];
}

void DataProvider::WriteLockAll(std::vector<const ContentIdProto*> chunk_ids,
                                WriterMutexLockList* locks) {
  // Sorting the list avoids cycles when locking from multiple threads
  // concurrently, thus avoiding deadlocks when holding some mutexes while
  // trying to lock others.
  std::sort(
      chunk_ids.begin(), chunk_ids.end(),
      [](const ContentIdProto* a, const ContentIdProto* b) { return *a < *b; });

  std::unordered_set<absl::Mutex*> locked;
  for (const ContentIdProto* id : chunk_ids) {
    absl::Mutex* mu = GetContentMutex(*id);
    auto [_, inserted] = locked.insert(mu);
    if (!inserted) continue;
    locks->push_back(std::make_unique<absl::WriterMutexLock>(mu));
  }
}

absl::Status DataProvider::GetFromWriter(ChunkTransferList* chunks,
                                         bool lock_required) {
  if (!writer_ || chunks->ReadDone()) return absl::OkStatus();

  // Try to read all remaining chunks from the cache.
  absl::StatusOr<size_t> read_bytes;
  for (ChunkTransferTask& chunk : *chunks) {
    if (chunk.done) continue;

    {
      std::unique_ptr<absl::ReaderMutexLock> lock;
      if (lock_required) {
        lock =
            std::make_unique<absl::ReaderMutexLock>(GetContentMutex(chunk.id));
      }
      if (!chunk.size) {
        // Check if the prefetch chunk is already present, no further processing
        // needed.
        chunk.done = writer_->Contains(chunk.id);
        continue;
      }

      // Read the requested data.
      read_bytes = writer_->Get(chunk.id, chunk.data, chunk.offset, chunk.size);
    }

    if (!read_bytes.ok()) {
      LogWriterWarning(read_bytes.status(), chunk.id);
    } else if (*read_bytes == chunk.size) {
      chunk.done = true;
      if (chunks->ReadDone()) return absl::OkStatus();
    } else {
      LogWriterWarning(
          MakeStatus("Expected %u bytes, got %u", chunk.size, *read_bytes),
          chunk.id);
      // Remove the corrupted chunk from the cache, but only if the chunk was
      // write-locked by the caller.
      if (!lock_required) {
        absl::Status status = writer_->Remove(chunk.id);
        if (!status.ok()) {
          LOG_WARNING("Failed to remove chunk '%s' from the cache: %s",
                      ContentId::ToHexString(chunk.id), status.ToString());
        }
      }
    }
  }
  return absl::OkStatus();
}

void DataProvider::LockAllMutexes(WriterMutexLockList* locks) {
  for (absl::Mutex& mu : content_mutexes_) {
    locks->push_back(std::make_unique<absl::WriterMutexLock>(&mu));
  }
}

void DataProvider::CleanupThreadMain() {
  assert(writer_);
  writer_->RegisterInterrupt(&interrupt_);
  absl::MutexLock lock(&shutdown_mutex_);
  SteadyClock::Timestamp next_cleanup_time =
      steady_clock_->Now() + std::chrono::seconds(cleanup_timeout_sec_);
  while (!shutdown_) {
    auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(shutdown_mutex_) {
      return shutdown_;
    };
    shutdown_mutex_.AwaitWithTimeout(
        absl::Condition(&cond),
        std::max(absl::Seconds(access_idle_timeout_sec_),
                 absl::Seconds(std::chrono::duration_cast<std::chrono::seconds>(
                                   next_cleanup_time - steady_clock_->Now())
                                   .count())));
    int64_t time_sec_since_last_access =
        GetSteadyNowSec() - last_access_sec_.load();
    if (chunks_updated_ &&
        time_sec_since_last_access > access_idle_timeout_sec_) {
      WriterMutexLockList locks;
      LockAllMutexes(&locks);
      chunks_updated_ = false;
      LOG_INFO("Starting cache cleanup");
      Stopwatch sw;
      absl::Status status = writer_->Cleanup();
      LOG_INFO("Finished cache cleanup in %0.3f seconds", sw.ElapsedSeconds());
      next_cleanup_time =
          steady_clock_->Now() + std::chrono::seconds(cleanup_timeout_sec_);
      absl::MutexLock cleaned_lock(&cleaned_mutex_);
      if (!status.ok()) {
        LOG_WARNING("Failed to cleanup the cache: %s", status.message());
        chunks_updated_ = true;
        is_cleaned_ = false;
      } else {
        is_cleaned_ = true;
      }
    }
    interrupt_ = false;
  }
}

bool DataProvider::WaitForCleanupAndResetForTesting(absl::Duration timeout) {
  absl::MutexLock lock(&cleaned_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(cleaned_mutex_) {
    return is_cleaned_;
  };
  cleaned_mutex_.AwaitWithTimeout(absl::Condition(&cond), timeout);
  bool is_cleaned = is_cleaned_;
  is_cleaned_ = false;
  return is_cleaned;
}

int64_t DataProvider::GetSteadyNowSec() {
  return std::chrono::duration_cast<std::chrono::seconds>(steady_clock_->Now() -
                                                          first_now_)
      .count();
}

}  // namespace cdc_ft
