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

#include "cdc_indexer/indexer.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <queue>
#include <thread>

#include "absl/functional/bind_front.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "blake3.h"
#include "common/dir_iter.h"
#include "common/errno_mapping.h"
#include "common/path.h"
#include "common/status_macros.h"

namespace cdc_ft {

struct IndexerJob {
  std::string filepath;
};

class Indexer::Impl {
 public:
  Impl(const IndexerConfig& cfg, const std::vector<std::string>& inputs);
  const IndexerConfig& Config() const;

  // Calls the given `progress` function periodically until `SetDone(true)` is
  // called.
  void TriggerProgress(ProgressFn fn);
  bool GetNextJob(IndexerJob* job);

  bool HasError() const;
  absl::Status Error() const;
  void SetError(absl::Status err);

  void SetDone(bool done);

  inline const IndexerConfig& Cfg() const { return cfg_; }
  inline Indexer::OpStats Stats() const;
  inline Indexer::ChunkSizeMap ChunkSizes() const;
  void AddChunk(const uint8_t* data, size_t len);
  void AddFile();

 private:
  friend class Indexer;
  // Calculates a hash value for the given data.
  inline hash_t Hash(const uint8_t* data, size_t len);
  inline hash_t HashBlake3(const uint8_t* data, size_t len);
  inline hash_t HashXxhash(const uint8_t* data, size_t len);
  // Finds the smallest power of 2 such that the result is <= size. If size is >
  // 2^31, then UINT64_MAX is returned.
  inline size_t SizeBucket(size_t size) const;

  IndexerConfig cfg_;
  bool done_;
  // The following members are all guarded by jobs_mutex_.
  std::queue<std::string> inputs_;
  DirectoryIterator dir_iter_;
  std::mutex jobs_mutex_;
  // Guarded by chunks_mutex_
  Indexer::ChunkMap chunks_;
  std::mutex chunks_mutex_;
  // Guarded by stats_mutex_.
  Indexer::OpStats stats_;
  mutable std::mutex stats_mutex_;
  // Guarded by chunk_sizes_mutex_;
  Indexer::ChunkSizeMap chunk_sizes_;
  mutable std::mutex chunk_sizes_mutex_;
  // Guarded by result_mutex_
  absl::Status result_;
  mutable std::mutex result_mutex_;
};

class Indexer::Worker {
 public:
  Worker(Impl* impl);
  void Run();

 private:
  absl::Status IndexFile(const std::string& filepath);

  Impl* impl_;
  absl::Cord buf_;
  const fastcdc::Config cdc_cfg_;
};

// This class holds a `Worker` object and the associated `std::thread` object
// that executes it.
class Indexer::WorkerThread {
 public:
  WorkerThread() : worker(nullptr), thrd(nullptr) {}
  ~WorkerThread() {
    if (thrd) {
      if (thrd->joinable()) thrd->join();
      delete thrd;
    }
    if (worker) {
      delete worker;
    }
  }
  Worker* worker;
  std::thread* thrd;
};

Indexer::Impl::Impl(const IndexerConfig& cfg,
                    const std::vector<std::string>& inputs)
    : cfg_(cfg), done_(false) {
  // Perform some sanity checks on the config.
  if (cfg_.num_threads == 0)
    cfg_.num_threads = std::thread::hardware_concurrency();
  if (cfg_.read_block_size == 0) cfg_.read_block_size = 4 << 10;
  if (cfg_.avg_chunk_size == 0) cfg_.avg_chunk_size = 256 << 10;
  if (cfg_.min_chunk_size == 0 || cfg_.min_chunk_size > cfg_.avg_chunk_size)
    cfg_.min_chunk_size = cfg_.avg_chunk_size >> 1;
  if (cfg_.max_chunk_size == 0 || cfg_.max_chunk_size < cfg_.avg_chunk_size)
    cfg_.max_chunk_size = cfg_.avg_chunk_size << 1;
  if (cfg_.max_chunk_size_step == 0)
    cfg_.max_chunk_size_step =
        cfg_.min_chunk_size > 0 ? cfg_.min_chunk_size : 128u;
  // Populate the CDC bitmasks which the Chunker creates. Only done here for
  // being able to write it to the output, setting them in the IndexerConfig has
  // no effect.
  fastcdc::Config ccfg(cfg_.min_chunk_size, cfg_.avg_chunk_size,
                       cfg_.max_chunk_size);
  Indexer::Chunker chunker(ccfg, nullptr);
  cfg_.threshold = chunker.Threshold();
  // Collect inputs.
  for (auto it = inputs.begin(); it != inputs.end(); ++it) {
    inputs_.push(*it);
  }
}

const IndexerConfig& Indexer::Impl::Config() const { return cfg_; }

// Executes the `progress` function in a loop, approximately every 200ms. Call
// `SetDone(true)` to stop this function.
void Indexer::Impl::TriggerProgress(Indexer::ProgressFn fn) {
  if (!fn) return;
  const int64_t interval = 200;
  absl::Time started = absl::Now();
  // Keeping going until we're done or an error occured.
  while (!done_ && !HasError()) {
    absl::Time loop_started = absl::Now();
    stats_mutex_.lock();
    stats_.elapsed = loop_started - started;
    stats_mutex_.unlock();

    fn(Stats());
    // Aim for one update every interval.
    auto loop_elapsed = absl::ToInt64Milliseconds(loop_started - absl::Now());
    if (loop_elapsed < interval)
      std::this_thread::sleep_for(
          std::chrono::milliseconds(interval - loop_elapsed));
  }
}

bool Indexer::Impl::GetNextJob(IndexerJob* job) {
  // Stop if an error occured.
  if (HasError()) return false;
  const std::lock_guard<std::mutex> lock(jobs_mutex_);

  DirectoryEntry dent;
  while (!dent.Valid()) {
    // Open the next directory, if needed.
    if (!dir_iter_.Valid()) {
      if (inputs_.empty()) {
        // We are done.
        return false;
      } else {
        std::string input = inputs_.front();
        std::string uinput = path::ToUnix(input);
        inputs_.pop();
        // Return files as jobs.
        if (path::FileExists(uinput)) {
          job->filepath = uinput;
          return true;
        }
        // Otherwise read the directory.
        if (!dir_iter_.Open(input, DirectorySearchFlags::kFiles)) {
          // Ignore permission errors.
          if (absl::IsPermissionDenied(dir_iter_.Status())) {
            continue;
          }
          if (!dir_iter_.Status().ok()) {
            SetError(dir_iter_.Status());
          }
          return false;
        }
      }
    }
    if (dir_iter_.NextEntry(&dent)) {
      break;
    } else if (!dir_iter_.Status().ok()) {
      SetError(dir_iter_.Status());
      return false;
    }
  }

  path::Join(&job->filepath, dir_iter_.Path(), dent.RelPathName());
  return true;
}

void Indexer::Impl::SetDone(bool done) { done_ = done; }

inline size_t Indexer::Impl::SizeBucket(size_t size) const {
  size_t bucket = 1024;
  // Go in steps of powers of two until min. chunk size is reached.
  while (bucket < size && bucket < cfg_.min_chunk_size && bucket < (1llu << 63))
    bucket <<= 1;
  // Go in steps of the configurable step size afterwards.
  while (bucket < size && bucket < (1llu << 63))
    bucket += cfg_.max_chunk_size_step;
  return bucket >= size ? bucket : UINT64_MAX;
}

inline Indexer::OpStats Indexer::Impl::Stats() const {
  const std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

inline Indexer::ChunkSizeMap Indexer::Impl::ChunkSizes() const {
  const std::lock_guard<std::mutex> lock(chunk_sizes_mutex_);
  return chunk_sizes_;
}

Indexer::hash_t Indexer::Impl::HashBlake3(const uint8_t* data, size_t len) {
  blake3_hasher state;
  uint8_t out[BLAKE3_OUT_LEN];
  blake3_hasher_init(&state);
  blake3_hasher_update(&state, data, len);
  blake3_hasher_finalize(&state, out, BLAKE3_OUT_LEN);
  return Indexer::hash_t(reinterpret_cast<const char*>(out), BLAKE3_OUT_LEN);
}

Indexer::hash_t Indexer::Impl::Hash(const uint8_t* data, size_t len) {
  switch (cfg_.hash_type) {
    case IndexerConfig::HashType::kNull:
      return hash_t();
    case IndexerConfig::HashType::kBlake3:
      return HashBlake3(data, len);
    case IndexerConfig::HashType::kUndefined:
      break;
  }
  std::cerr << "Unknown hash type" << std::endl;
  return std::string();
}

void Indexer::Impl::AddChunk(const uint8_t* data, size_t len) {
  std::string hash = Hash(data, len);
  // See if the chunk already exists, insert it if not.
  chunks_mutex_.lock();
  bool new_chunk = chunks_.find(hash) == chunks_.end();
  if (new_chunk) {
    chunks_.emplace(hash, Chunk{hash, len});
  }
  chunks_mutex_.unlock();

  // Update the stats.
  stats_mutex_.lock();
  stats_.total_bytes += len;
  ++stats_.total_chunks;
  if (new_chunk) {
    stats_.unique_bytes += len;
    ++stats_.unique_chunks;
  }
  stats_mutex_.unlock();

  // Update chunk sizes distribution.
  if (new_chunk) {
    size_t bucket = SizeBucket(len);
    chunk_sizes_mutex_.lock();
    chunk_sizes_[bucket]++;
    chunk_sizes_mutex_.unlock();
  }
}

void Indexer::Impl::AddFile() {
  const std::lock_guard<std::mutex> lock(stats_mutex_);
  ++stats_.total_files;
}

bool Indexer::Impl::HasError() const {
  const std::lock_guard<std::mutex> lock(result_mutex_);
  return !result_.ok();
}

absl::Status Indexer::Impl::Error() const {
  const std::lock_guard<std::mutex> lock(result_mutex_);
  return result_;
}

void Indexer::Impl::SetError(absl::Status err) {
  // Ignore attempts to set a non-error.
  if (err.ok()) return;
  const std::lock_guard<std::mutex> lock(result_mutex_);
  // Don't overwrite any previous error.
  if (result_.ok()) result_ = err;
}

Indexer::Worker::Worker(Indexer::Impl* impl)
    : impl_(impl),
      cdc_cfg_(impl_->Cfg().min_chunk_size, impl_->Cfg().avg_chunk_size,
               impl_->Cfg().max_chunk_size) {}

void Indexer::Worker::Run() {
  IndexerJob job;
  while (impl_->GetNextJob(&job)) {
    absl::Status err = IndexFile(job.filepath);
    if (!err.ok()) {
      impl_->SetError(err);
      return;
    }
  }
}

absl::Status Indexer::Worker::IndexFile(const std::string& filepath) {
  std::FILE* fin = std::fopen(filepath.c_str(), "rb");
  if (!fin) {
    return ErrnoToCanonicalStatus(errno, "failed to open file '%s'", filepath);
  }
  path::FileCloser closer(fin);
  std::fseek(fin, 0, SEEK_SET);

  auto hdlr = absl::bind_front(&Indexer::Impl::AddChunk, impl_);
  Indexer::Chunker chunker(cdc_cfg_, hdlr);

  std::vector<uint8_t> buf(impl_->Cfg().read_block_size, 0);
  int err = 0;
  while (!std::feof(fin)) {
    size_t cnt = std::fread(buf.data(), sizeof(uint8_t), buf.size(), fin);
    err = std::ferror(fin);
    if (err) {
      return ErrnoToCanonicalStatus(err, "failed to read from file '%s'",
                                    filepath);
    }
    if (cnt) {
      chunker.Process(buf.data(), cnt);
    }
  }
  chunker.Finalize();
  impl_->AddFile();

  return absl::OkStatus();
}

IndexerConfig::IndexerConfig()
    : read_block_size(32 << 10),
      min_chunk_size(0),
      avg_chunk_size(0),
      max_chunk_size(0),
      max_chunk_size_step(0),
      num_threads(0),
      threshold(0) {}

Indexer::Indexer() : impl_(nullptr) {}

Indexer::~Indexer() {
  if (impl_) delete impl_;
}

absl::Status Indexer::Run(const IndexerConfig& cfg,
                          const std::vector<std::string>& inputs,
                          Indexer::ProgressFn fn) {
  if (impl_) delete impl_;
  impl_ = new Impl(cfg, inputs);

  // Start the file creation workers.
  std::vector<WorkerThread> workers(impl_->Config().num_threads);
  for (auto it = workers.begin(); it != workers.end(); ++it) {
    auto worker = new Worker(impl_);
    it->worker = worker;
    it->thrd = new std::thread(&Worker::Run, worker);
  }
  // Start the progress function worker.
  std::thread prog(&Impl::TriggerProgress, impl_, fn);

  // Wait for the workers to finish.
  for (auto it = workers.begin(); it != workers.end(); ++it) {
    it->thrd->join();
  }
  // Wait for the progress worker to finish.
  impl_->SetDone(true);
  prog.join();

  return Error();
}

absl::Status Indexer::Error() const {
  return impl_ ? impl_->Error() : absl::Status();
}

IndexerConfig Indexer::Config() const {
  if (impl_) return impl_->Cfg();
  return IndexerConfig();
}

Indexer::OpStats Indexer::Stats() const {
  if (impl_) return impl_->Stats();
  return Stats();
}

Indexer::ChunkSizeMap Indexer::ChunkSizes() const {
  if (impl_) return impl_->ChunkSizes();
  return Indexer::ChunkSizeMap();
}

inline Indexer::OpStats::OpStats()
    : total_files(0),
      total_chunks(0),
      unique_chunks(0),
      total_bytes(0),
      unique_bytes(0) {}

};  // namespace cdc_ft
