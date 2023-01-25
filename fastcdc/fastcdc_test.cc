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

#include "fastcdc/fastcdc.h"

#include "gtest/gtest.h"

namespace cdc_ft {
namespace fastcdc {

// This gear function has the following properties:
// - data like {0, 0, 0, ...} results in a continuously zero rolling hash, thus
// is always identified as a chunk boundary.
// - data like {1, 1, 1, ...} results in a continuously all-ones rolling hash,
// thus is never identified as a chunk boundary.
static constexpr uint64_t test_kmult = 1;

template <uint64_t kmult = test_kmult>
using TestChunker = Chunker64<kmult>;

class ChunkerTest : public ::testing::Test {
 public:
  ChunkerTest() {}
};

// Tests that the threshold for hash comparison is set correctly.
TEST_F(ChunkerTest, ValidateThreshold) {
  // Sizes: 128/256/512 bytes
  Config cfg(128, 256, 512);
  TestChunker<> chunker(cfg, nullptr);
  EXPECT_EQ(0x1fc07f01fc07f01, chunker.Threshold());
}

// Tests that the minimum chunk size is not undercut.
TEST_F(ChunkerTest, MinChunkSize) {
  Config cfg(64, 96, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  // All-zero data matches a chunk boundary everywhere.
  std::vector<uint8_t> data(cfg.max_size, 0);
  chunker.Process(data.data(), data.size());
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 2);
  for (size_t size : chunk_sizes) {
    EXPECT_EQ(size, cfg.min_size);
  }
}

// Tests that maximum chunk size is not exceeded.
TEST_F(ChunkerTest, MaxChunkSize) {
  Config cfg(0, 64, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  // All-ones data never matches a chunk boundary.
  std::vector<uint8_t> data(4 * cfg.max_size, 1);
  chunker.Process(data.data(), data.size());
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 4);
  for (size_t size : chunk_sizes) {
    EXPECT_EQ(size, cfg.max_size);
  }
}

// Tests that Finalize() returns the remaining data as a chunk.
TEST_F(ChunkerTest, FinalizeChunk) {
  Config cfg(32, 64, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  std::vector<uint8_t> data(1, 0);
  chunker.Process(data.data(), data.size());
  EXPECT_EQ(chunk_sizes.size(), 0);
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 1);
  EXPECT_EQ(chunk_sizes[0], 1);
}

// Tests that Finalize() works when no data is left.
TEST_F(ChunkerTest, FinalizeEmptyChunk) {
  Config cfg(32, 64, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  std::vector<uint8_t> data(1, 0);
  chunker.Process(data.data(), 0);
  EXPECT_EQ(chunk_sizes.size(), 0);
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 0);
}

// Tests that Finalize() works when Process() was not called.
TEST_F(ChunkerTest, FinalizeWithoutProcess) {
  Config cfg(32, 64, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 0);
}

}  // namespace fastcdc
}  // namespace cdc_ft
