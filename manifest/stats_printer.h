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

#ifndef MANIFEST_STATS_PRINTER_H_
#define MANIFEST_STATS_PRINTER_H_

#include <deque>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "common/stopwatch.h"

namespace cdc_ft {

// Collects and prints statistics about chunks streamed and cached per file.
// Also prints general bandwidth and total bytes statistics.
// Sample output (X's are colored by FUSE thread id, not shown):
//   gamedata.pak CCCXXXXXXXXXX------
//   lib.so       XXX------
//   Legend: (-) not loaded, (C) cached, (X) streamed (color=FUSE thread)
//   Bandwidth    0.00 MB/sec (curr)    2.39 MB/sec (max)
//   Total data      3 MB (streamed)       1 MB (cached)
// Each X/C/- represents a chunk unless the file is large and the chunks don't
// fit into a single line. In that case, the X/C/- represents the most recently
// accessed chunk in a group of several chunks.
class StatsPrinter {
 public:
  StatsPrinter();
  ~StatsPrinter();

  // Registers a file for the given relative Unix |path| that has |num_chunks|
  // chunks.
  void InitFile(const std::string& path, size_t num_chunks);

  // Clears all data expect max bandwidth.
  void Clear();

  // Resets measurement of current bandwidth.
  void ResetBandwidthStats();

  // Records a chunk that was streamed from the workstation.
  // |path| is the relative Unix path of a file that contains the chunk.
  // |index| is the index of the chunk.
  // |size| is the size of the chunk in bytes.
  // |thread_id| is the id of the thread that requested the chunk on the
  // gamelet, usually the hash of the std::thread::id.
  // Asserts that the file was registered with InitFile() and that |index| is
  // smaller than |num_chunks| passed to InitFile().
  void RecordStreamedChunk(const std::string& path, size_t index, uint32_t size,
                           size_t thread_id);

  // Records a chunk that is cached on the gamelet.
  // |path| is the relative Unix path of a file that contains the chunk.
  // |index| is the index of the chunk.
  // |size| is the size of the chunk in bytes.
  // Asserts that the file was registered with InitFile() and that |index| is
  // smaller than |num_chunks| passed to InitFile().
  void RecordCachedChunk(const std::string& path, size_t index, uint32_t size);

  // Prints all statistics.
  void Print();

 private:
  // Adds |path| to |recent_files_| if it's not already there and removes the
  // first entry if the list gets too large.
  void AddToRecentFiles(const std::string& path);

  // Updates the current and total bandwidth stats.
  void UpdateBandwidthStats();

  enum class ChunkState : uint8_t {
    kNotLoaded = 0,  // Chunk is neither cached nor streamed.
    kStreamed = 1,   // Chunk was streamed from the workstation.
    kCached = 2,     // Chunk was cached on the gamelet.
  };

  struct FileChunk {
    // Thread on gamelet that requested a streamed chunk.
    // Unused for cached chunks and chunks that are not loaded.
    size_t thread_id = 0;

    // Time when this data was modified.
    absl::Time modified_time;

    // Whether the chunk is cached, was streamed or is not loaded.
    ChunkState state = ChunkState::kNotLoaded;

    FileChunk() {}
    explicit FileChunk(ChunkState state, size_t thread_id)
        : thread_id(thread_id), modified_time(absl::Now()), state(state) {}
  };

  struct File {
    // All chunks in the file.
    std::vector<FileChunk> chunks;
  };

  // LRU access list.
  std::deque<std::string> recent_files_;

  // Map from relative Unix file path to all chunks in that file.
  using PathToFileMap = absl::flat_hash_map<std::string, File>;
  PathToFileMap path_to_file_;

  // Assigns each thread a fixed color.
  std::unordered_map<size_t, int> thread_id_to_color_;
  int num_threads_ = 0;

  Stopwatch bandwidth_timer_;
  double curr_bandwidth_ = 0;
  uint64_t curr_streamed_bytes_ = 0;

  double max_bandwidth_ = 0;
  uint64_t total_streamed_bytes_ = 0;
  uint64_t total_cached_bytes_ = 0;
};

}  // namespace cdc_ft

#endif  // MANIFEST_STATS_PRINTER_H_
