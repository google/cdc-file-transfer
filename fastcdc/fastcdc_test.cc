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
static const uint64_t testgear64[256]{0, 1};  // 0, 1, 0, 0, 0, ...
static constexpr uint32_t test_mask_stages = 5;
static constexpr uint32_t test_mask_lshift = 1;

template <uint32_t mask_stages = test_mask_stages,
          uint32_t mask_lshift = test_mask_lshift>
using TestChunker = ChunkerTmpl<uint64_t, testgear64, mask_stages, mask_lshift>;

// Returns the number of bits set to 1 in the given mask.
uint32_t BitCount(uint64_t mask) {
  uint32_t count = 0;
  for (; mask; mask >>= 1) {
    count += mask & 1u;
  }
  return count;
}

class ChunkerTest : public ::testing::Test {
 public:
  ChunkerTest() {}

 protected:
  template <uint32_t mask_stages>
  static void ValidateStagesTmpl(const Config& cfg);

  template <uint32_t mask_lshift>
  static void ValidateLshiftTmpl(const Config& cfg);
};

template <uint32_t mask_stages>
void ChunkerTest::ValidateStagesTmpl(const Config& cfg) {
  TestChunker<mask_stages> chunker(cfg, nullptr);
  EXPECT_EQ(chunker.StagesCount(), mask_stages);

  for (uint32_t i = 1; i < chunker.StagesCount(); i++) {
    auto prev_stg = chunker.Stage(i - 1);
    auto stg = chunker.Stage(i);
    EXPECT_LT(prev_stg.barrier, stg.barrier)
        << "Stage " << i + 1 << " of " << mask_stages
        << ": barriers should be at increasing positions";
    if (prev_stg.mask > 1) {
      EXPECT_EQ(BitCount(prev_stg.mask), BitCount(stg.mask) + 1)
          << "Stage " << i + 1 << " of " << mask_stages
          << ": number of bits in adjacent stages should differ by 1";
    } else {
      EXPECT_EQ(1, BitCount(stg.mask))
          << "Stage " << i + 1 << " of " << mask_stages
          << ": number of bits in last bitmasks should be 1";
    }
  }

  EXPECT_EQ(chunker.Stage(mask_stages - 1).barrier, cfg.max_size)
      << "final stage barrier must match the maximum chunk size";
}

// Tests that the stages to apply different bitmasks are initialized properly
TEST_F(ChunkerTest, ValidateStages) {
  // Sizes: 128/256/512 bytes
  Config cfg(128, 256, 512);
  ValidateStagesTmpl<1>(cfg);
  ValidateStagesTmpl<2>(cfg);
  ValidateStagesTmpl<3>(cfg);
  ValidateStagesTmpl<4>(cfg);
  ValidateStagesTmpl<5>(cfg);
  ValidateStagesTmpl<6>(cfg);
  ValidateStagesTmpl<7>(cfg);
  ValidateStagesTmpl<8>(cfg);

  // Sizes: 128/256/512 KiB
  cfg = Config(128 << 10, 256 << 10, 512 << 10);
  ValidateStagesTmpl<1>(cfg);
  ValidateStagesTmpl<2>(cfg);
  ValidateStagesTmpl<3>(cfg);
  ValidateStagesTmpl<4>(cfg);
  ValidateStagesTmpl<5>(cfg);
  ValidateStagesTmpl<6>(cfg);
  ValidateStagesTmpl<7>(cfg);
  ValidateStagesTmpl<8>(cfg);
  ValidateStagesTmpl<16>(cfg);
  ValidateStagesTmpl<32>(cfg);
  ValidateStagesTmpl<64>(cfg);

  // Sizes: 128/256/512 MiB
  cfg = Config(128 << 20, 256 << 20, 512 << 20);
  ValidateStagesTmpl<1>(cfg);
  ValidateStagesTmpl<2>(cfg);
  ValidateStagesTmpl<3>(cfg);
  ValidateStagesTmpl<4>(cfg);
  ValidateStagesTmpl<5>(cfg);
  ValidateStagesTmpl<6>(cfg);
  ValidateStagesTmpl<7>(cfg);
  ValidateStagesTmpl<8>(cfg);
  ValidateStagesTmpl<16>(cfg);
  ValidateStagesTmpl<32>(cfg);
  ValidateStagesTmpl<64>(cfg);

  // Sizes: 0/512/1024 KiB
  cfg = Config(0, 512 << 10, 1024 << 10);
  ValidateStagesTmpl<1>(cfg);
  ValidateStagesTmpl<2>(cfg);
  ValidateStagesTmpl<3>(cfg);
  ValidateStagesTmpl<4>(cfg);
  ValidateStagesTmpl<5>(cfg);
  ValidateStagesTmpl<6>(cfg);
  ValidateStagesTmpl<7>(cfg);
  ValidateStagesTmpl<8>(cfg);
  ValidateStagesTmpl<16>(cfg);
  ValidateStagesTmpl<32>(cfg);
  ValidateStagesTmpl<64>(cfg);

  // Sizes: 0/512/1024 MiB
  cfg = Config(0, 512 << 20, 1024 << 20);
  ValidateStagesTmpl<1>(cfg);
  ValidateStagesTmpl<2>(cfg);
  ValidateStagesTmpl<3>(cfg);
  ValidateStagesTmpl<4>(cfg);
  ValidateStagesTmpl<5>(cfg);
  ValidateStagesTmpl<6>(cfg);
  ValidateStagesTmpl<7>(cfg);
  ValidateStagesTmpl<8>(cfg);
  ValidateStagesTmpl<16>(cfg);
  ValidateStagesTmpl<32>(cfg);
  ValidateStagesTmpl<64>(cfg);
}

template <uint32_t mask_lshift>
void ChunkerTest::ValidateLshiftTmpl(const Config& cfg) {
  TestChunker<1, mask_lshift> chunker(cfg, nullptr);
  uint64_t mask = chunker.Stage(0).mask;
  uint64_t expected = BitCount(mask);
  EXPECT_GE(expected, 1) << "no bits were set in the bit mask for lshift "
                         << mask_lshift;
  // Compare no. of all 1-bits to no. of 1-bits with the given shift amount.
  uint32_t actual = 0;
  for (; mask; mask >>= mask_lshift) {
    actual += mask & 1u;
  }
  EXPECT_EQ(expected, actual)
      << "number of bits set is different with lshift " << mask_lshift;
}

// Tests that the bitmasks for each stage honor the mask_lshift template
// parameter correctly.
TEST_F(ChunkerTest, ValidateLshift) {
  Config cfg(32, 64, 128);
  ValidateLshiftTmpl<1>(cfg);
  ValidateLshiftTmpl<2>(cfg);
  ValidateLshiftTmpl<3>(cfg);
  ValidateLshiftTmpl<4>(cfg);
  ValidateLshiftTmpl<5>(cfg);
}

// Tests that the minimum chunk size is not undercut.
TEST_F(ChunkerTest, MinChunkSize) {
  Config cfg(32, 64, 128);
  std::vector<size_t> chunk_sizes;
  TestChunker<> chunker(cfg, [&](const uint8_t* /* data */, size_t len) {
    chunk_sizes.push_back(len);
  });
  // All-zero data matches a chunk boundary everywhere.
  std::vector<uint8_t> data(cfg.max_size, 0);
  chunker.Process(data.data(), data.size());
  chunker.Finalize();
  EXPECT_EQ(chunk_sizes.size(), 4);
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
