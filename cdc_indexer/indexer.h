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

#ifndef CDC_INDEXER_INDEXER_H_
#define CDC_INDEXER_INDEXER_H_

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/status/status.h"
#include "absl/time/time.h"
#include "fastcdc/fastcdc.h"

// Compile-time parameters for the FastCDC algorithm.
#define CDC_GEAR_32BIT 32
#define CDC_GEAR_64BIT 64
#ifndef CDC_GEAR_BITS
#define CDC_GEAR_BITS CDC_GEAR_64BIT
#endif

namespace cdc_ft {

struct IndexerConfig {
  // The hash function to use.
  enum class HashType {
    kUndefined = 0,
    // No hashing performed, always return an empty string.
    kNull,
    // Use BLAKE3 (cryptographic)
    kBlake3,
  };
  IndexerConfig();
  // Read file contents in the given block size from disk, defaults to 4K.
  size_t read_block_size;
  // The minimum allowed chunk size, defaults to avg_chunk_size/2.
  size_t min_chunk_size;
  // The target average chunk size.
  size_t avg_chunk_size;
  // The maximum allowed chunk size, defaults to 2*avg_chunk_size.
  size_t max_chunk_size;
  // Max. step size for bucketing the chunk size distribution.
  size_t max_chunk_size_step;
  // How many operations to run in parallel. If this value is zero, then
  // `std::thread::hardware_concurrency()` is used.
  uint32_t num_threads;
  // Which hash function to use.
  HashType hash_type;
  // The threshold will be populated by the indexer, setting it here has no
  // effect. It is in this struct so that it can be conveniently accessed
  // when printing the operation summary (and since it is derived from the
  // configuration, it is technically part of it).
  uint64_t threshold;
};

class Indexer {
 public:
  using hash_t = std::string;
#if CDC_GEAR_BITS == CDC_GEAR_32BIT
  typedef fastcdc::Chunker32<> Chunker;
#elif CDC_GEAR_BITS == CDC_GEAR_64BIT
  typedef fastcdc::Chunker64<> Chunker;
#else
#error "Unknown gear table"
#endif

  // Represents a chunk.
  struct Chunk {
    hash_t hash;
    size_t size;
  };

  // Chunk storage, keyed by hash. The hash value must be mapped to a uint64_t
  // value here, which is only acceptable for an experimental program like this.
  typedef std::unordered_map<hash_t, Chunk> ChunkMap;
  // Used for counting number of chunks in size buckets.
  typedef std::unordered_map<size_t, uint64_t> ChunkSizeMap;

  // Statistics about the current operation.
  struct OpStats {
    OpStats();
    size_t total_files;
    size_t total_chunks;
    size_t unique_chunks;
    size_t total_bytes;
    size_t unique_bytes;
    absl::Duration elapsed;
  };

  // Defines a callback function that can be used to display progress updates
  // while the Indexer is busy.
  typedef void(ProgressFn)(const OpStats& stats);

  Indexer();
  ~Indexer();

  // Starts the indexing operation for the given configuration `cfg` and
  // `inputs`. The optional callback function `fn` is called periodically with
  // statistics about the ongoing operation.
  absl::Status Run(const IndexerConfig& cfg,
                   const std::vector<std::string>& inputs, ProgressFn fn);
  // Returns the status of the ongoing or completed operation.
  absl::Status Error() const;
  // Returns the configuration that was passed to Run().
  IndexerConfig Config() const;
  // Returns the statistics about the ongoing or completed operation.
  OpStats Stats() const;
  // Returns a map of chunk sizes to the number of occurrences. The sizes are
  // combined to buckets according to the given `IndexerConfig` of the Run()
  // operation.
  ChunkSizeMap ChunkSizes() const;

 private:
  class Impl;
  class Worker;
  class WorkerThread;
  Impl* impl_;
};

};  // namespace cdc_ft

#endif  // CDC_INDEXER_INDEXER_H_
