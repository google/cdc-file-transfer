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

#include "manifest/file_chunk_map.h"

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr char kFile1[] = "file1";
constexpr char kFile2[] = "file2";

class FileChunkMapTest : public ::testing::Test {
 protected:
  // Creates a ChunkRef proto list from chunk data.
  RepeatedChunkRefProto MakeChunks(
      std::initializer_list<std::string> chunk_data) {
    uint64_t offset = 0;
    RepeatedChunkRefProto chunks;
    for (const std::string& data : chunk_data) {
      ChunkRefProto* chunk = chunks.Add();
      chunk->set_offset(offset);
      *chunk->mutable_chunk_id() = ContentId::FromDataString(data);
      offset += data.size();
    }
    return chunks;
  }

  // Creates a ContentId proto from string |data|.
  ContentIdProto Id(const std::string& data) {
    return ContentId::FromDataString(data);
  }

  FileChunkMap file_chunks_{/*enable_stats=*/false};
  std::string path_;
  uint64_t offset_ = 0;
  uint32_t size_ = 0;
};

TEST_F(FileChunkMapTest, LookupOneChunk) {
  file_chunks_.Init(kFile1, 10);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0123456789"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123456789"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 0);
  EXPECT_EQ(size_, 10);
}

TEST_F(FileChunkMapTest, LookupWithoutFlush) {
  file_chunks_.Init(kFile1, 10);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0123456789"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123456789"), &path_, &offset_, &size_));

  file_chunks_.Clear();
  // No FlushUpdates() call.

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123456789"), &path_, &offset_, &size_));
}

TEST_F(FileChunkMapTest, LookupTwoChunks) {
  file_chunks_.Init(kFile1, 10);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0123", "456789"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 0);
  EXPECT_EQ(size_, 4);

  EXPECT_TRUE(file_chunks_.Lookup(Id("456789"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 4);
  EXPECT_EQ(size_, 6);
}

TEST_F(FileChunkMapTest, LookupTwoFiles) {
  file_chunks_.Init(kFile1, 4);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0123"}), 0);

  file_chunks_.Init(kFile2, 6);
  file_chunks_.AppendCopy(kFile2, MakeChunks({"012345"}), 0);

  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 0);
  EXPECT_EQ(size_, 4);

  EXPECT_TRUE(file_chunks_.Lookup(Id("012345"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile2);
  EXPECT_EQ(offset_, 0);
  EXPECT_EQ(size_, 6);
}

TEST_F(FileChunkMapTest, InitWithChunks) {
  std::vector<FileChunk> chunks;
  chunks.emplace_back(Id("0123"), 0);
  chunks.emplace_back(Id("456789"), 4);

  file_chunks_.Init(kFile1, 10, &chunks);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123"), &path_, &offset_, &size_));
  EXPECT_TRUE(file_chunks_.Lookup(Id("456789"), &path_, &offset_, &size_));
  EXPECT_TRUE(chunks.empty());
}

TEST_F(FileChunkMapTest, InitWithChunksAndAppend) {
  std::vector<FileChunk> chunks;
  chunks.emplace_back(Id("0123"), 0);

  file_chunks_.Init(kFile1, 10, &chunks);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"456789"}), 4);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123"), &path_, &offset_, &size_));
  EXPECT_TRUE(file_chunks_.Lookup(Id("456789"), &path_, &offset_, &size_));
  EXPECT_TRUE(chunks.empty());
}

TEST_F(FileChunkMapTest, InitClearsExistingEntry) {
  file_chunks_.Init(kFile1, 6);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"012345"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("012345"), &path_, &offset_, &size_));
  EXPECT_EQ(size_, 6);

  file_chunks_.Init(kFile1, 4);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0123"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0123"), &path_, &offset_, &size_));
  EXPECT_EQ(size_, 4);
}

TEST_F(FileChunkMapTest, AppendAddsOffset) {
  file_chunks_.Init(kFile1, 10);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"01", "23", "45"}), 0);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"67", "89"}), 6);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("45"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 4);
  EXPECT_EQ(size_, 2);

  EXPECT_TRUE(file_chunks_.Lookup(Id("67"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 6);
  EXPECT_EQ(size_, 2);

  EXPECT_TRUE(file_chunks_.Lookup(Id("89"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
  EXPECT_EQ(offset_, 8);
  EXPECT_EQ(size_, 2);
}

TEST_F(FileChunkMapTest, Remove_DifferentChunks) {
  file_chunks_.Init(kFile1, 1);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0"}), 0);

  file_chunks_.Init(kFile2, 1);
  file_chunks_.AppendCopy(kFile2, MakeChunks({"1"}), 0);

  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));
  EXPECT_TRUE(file_chunks_.Lookup(Id("1"), &path_, &offset_, &size_));

  file_chunks_.Remove(kFile2);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));
  EXPECT_FALSE(file_chunks_.Lookup(Id("1"), &path_, &offset_, &size_));
}

TEST_F(FileChunkMapTest, Remove_SameChunks) {
  file_chunks_.Init(kFile1, 1);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0"}), 0);

  file_chunks_.Init(kFile2, 1);
  file_chunks_.AppendCopy(kFile2, MakeChunks({"0"}), 0);

  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));
  // |path_| is not deterministic as an absl::flat_hash_map is used internally.
  EXPECT_TRUE(path_ == kFile1 || path_ == kFile2) << path_;

  file_chunks_.Remove(kFile2);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);
}

TEST_F(FileChunkMapTest, Clear) {
  file_chunks_.Init(kFile1, 1);
  file_chunks_.AppendCopy(kFile1, MakeChunks({"0"}), 0);
  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));

  file_chunks_.Clear();
  file_chunks_.FlushUpdates();

  EXPECT_FALSE(file_chunks_.Lookup(Id("0"), &path_, &offset_, &size_));
}

TEST_F(FileChunkMapTest, AppendCopyMove) {
  RepeatedChunkRefProto chunks1 = MakeChunks({"01"});
  RepeatedChunkRefProto chunks2 = MakeChunks({"23"});

  file_chunks_.Init(kFile1, 2);
  file_chunks_.Init(kFile2, 2);

  file_chunks_.AppendCopy(kFile1, chunks1, 0);
  file_chunks_.AppendMove(kFile2, &chunks2, 0);

  file_chunks_.FlushUpdates();

  EXPECT_TRUE(file_chunks_.Lookup(Id("01"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile1);

  EXPECT_TRUE(file_chunks_.Lookup(Id("23"), &path_, &offset_, &size_));
  EXPECT_EQ(path_, kFile2);

  // AppendMove() should have moved the second chunk off the list.
  EXPECT_EQ(chunks1[0].chunk_id(), Id("01"));
  EXPECT_EQ(chunks2[0].chunk_id(), ContentIdProto());
}

}  // namespace
}  // namespace cdc_ft
