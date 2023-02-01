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

#ifndef FASTCDC_FASTCDC_H_
#define FASTCDC_FASTCDC_H_

#include <string.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <functional>
#include <iostream>
#include <vector>

namespace cdc_ft {
namespace fastcdc {

// These are recommended and widely used multipliers for LCGs from
// http://citeseer.ist.psu.edu/132363.html. They replace the gear lookup
// tables in the rolling hash.
static constexpr uint32_t default_kmult32 = 2891336453;
static constexpr uint64_t default_kmult64 = 2862933555777941757;

// Configures the chunk sizes that the ChunkerTmpl class produces. All sizes are
// given in bytes.
struct Config {
  Config(size_t min_size, size_t avg_size, size_t max_size)
      : min_size(min_size), avg_size(avg_size), max_size(max_size) {}
  // The minimum chunk size defines the lower bound for generated chunks. Note
  // that this size can still be undercut for the last chunk after processing
  // the input data.
  size_t min_size;
  // The average chunk size is the target size for chunks. Sizes will show an
  // offset exponential distribution decaying after min_size with the desired
  // average size.
  size_t avg_size;
  // The maximum size is the upper bound for generating chunks. This limit is
  // never exceeded. If a chunk boundary was not detected based on the content
  // when this limit is hit, a new boundary is forced.
  size_t max_size;
};

// Callback function for handling the cut-point of a new chunk.
using ChunkFoundHandler = std::function<void(const uint8_t* data, size_t len)>;

// Implements a very fast content-defined chunking algorithm.
//
// FastCDC [1] identifies chunk boundaries based on a simple yet efficient
// "gear" rolling hash, a "normalized chunking" algorithm using a stepped
// chunk probability with a pair spread-out bitmasks for the '!(hash&mask)'
// "hash criteria".
//
// This library implements a modified version based on rollsum-chunking [2]
// tests and analysis that showed simple "exponential chunking" gives better
// deduplication, and a 'hash<=threshold' "hash criteria" works better for
// the gear rollsum and can support arbitrary non-power-of-two sizes. It also
// uses a modified version of the gear rollsum from rollsum-tests [3] that
// showed using a multiply instead of a lookup table works just as well.
//
// The Config struct passed in during construction defines the minimum, average,
// and maximum allowed chunk sizes. Those are runtime parameters.
//
// The template allows additional compile-time configuration:
//
// - T : The type used for the hash. Should be an unsigned integer type,
// ideally uint32_t or uint64_t. The number of bits of this type determines
// the "sliding window" size of the gear hash. A smaller type is likely to be
// faster at the expense of reduced deduplication.
//
// - kmult: The multiplier to use for the modified gear rollsum. This
// library comes with two different recommended multipliers, one of type
// uint32_t and one of uint64_t. Both should work well, but the 64-bit
// version probably gives slightly better deduplication and is slightly
// slower.
//
// [1] https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf.
// [2] https://github.com/dbaarda/rollsum-chunking/blob/master/RESULTS.rst
// [3] https://github.com/dbaarda/rollsum-tests/blob/master/RESULTS.rst
//
// TODO: Remove template parameters.
template <typename T, const T kmult>
class ChunkerTmpl {
 public:

  // Constructor.
  ChunkerTmpl(const Config& cfg, ChunkFoundHandler handler)
      : cfg_(cfg), handler_(handler) {
    assert(cfg_.avg_size >= 1);
    assert(cfg_.min_size <= cfg_.avg_size);
    assert(cfg_.avg_size <= cfg_.max_size);

    // Calculate the threshold the hash must be <= to for a 1/(avg-min+1)
    // chance of a chunk boundary.
    kthreshold_ = (T)(-1) / (cfg_.avg_size - cfg_.min_size + 1);
    data_.reserve(cfg_.max_size << 1);
  }

  // Slices the given data block into chunks and calls the specified handler
  // function for each chunk cut-point. The remaining data is buffered and used
  // in the next call to Process() or Finalize().
  void Process(const uint8_t* data, size_t size) {
    bool eof = data == nullptr || size == 0;
    if (!eof) {
      // Append new data.
      assert(size > 0);
      data_.insert(data_.end(), data, data + size);
    }

    if (data_.empty()) {
      return;
    }

    assert(handler_ != nullptr);

    const uint8_t* data_ptr = &data_[0];
    size_t bytes_left = data_.size();
    while (bytes_left >= cfg_.max_size || (bytes_left > 0 && eof)) {
      const size_t chunk_size = FindChunkBoundary(data_ptr, bytes_left);

      handler_(data_ptr, chunk_size);

      assert(bytes_left >= chunk_size);
      bytes_left -= chunk_size;
      data_ptr += chunk_size;
    }

    // Copy remaining data to the beginning of the array. Using memmove/resize
    // is not slow when shrinking a vector.
    memmove(&data_[0], data_ptr, bytes_left);
    data_.resize(bytes_left);
  }

  // Slices all remaining buffered data into chunks, where the last chunk might
  // be smaller than the specified minimum chunk size.
  void Finalize() { Process(nullptr, 0); }

  // Returns the threshold for the hash <= threshold chunk boundary.
  T Threshold() { return kthreshold_; }

 private:

  size_t FindChunkBoundary(const uint8_t* data, size_t len) {
    if (len <= cfg_.min_size) {
      return len;
    }
    if (len > cfg_.max_size) {
      len = cfg_.max_size;
    }

    // Init hash to all 1's to avoid zero-length chunks with min_size=0.
    T hash = (T)-1;
    // Skip the first min_size bytes, but "warm up" the rolling hash for enough
    // rounds to make sure the hash has gathered full "content history".
    size_t i = cfg_.min_size > khashbits_ ? cfg_.min_size - khashbits_ : 0;
    for (/*empty*/; i < cfg_.min_size; ++i) {
      hash = ((hash << 1) + data[i]) * kmult;
    }
    for (/*empty*/; i < len; ++i) {
      if (hash <= kthreshold_) {
        return i;
      }
      hash = ((hash << 1) + data[i]) * kmult;
    }
    return i;
  }

  static constexpr int khashbits_ = sizeof(T) * 8;
  const Config cfg_;
  const ChunkFoundHandler handler_;
  T kthreshold_;
  std::vector<uint8_t> data_;
};

// Chunker template with a 32-bit gear table.
template <uint32_t kmult = default_kmult32>
using Chunker32 =
    ChunkerTmpl<uint32_t, kmult>;

// Chunker template with a 64-bit gear table.
template <uint64_t kmult = default_kmult64>
using Chunker64 =
    ChunkerTmpl<uint64_t, kmult>;

// Default chunker class using params that are known to work well.
using Chunker = Chunker64<>;

}  // namespace fastcdc
}  // namespace cdc_ft

#endif  // FASTCDC_FASTCDC_H_
