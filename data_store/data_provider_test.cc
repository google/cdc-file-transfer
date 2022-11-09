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

#include <numeric>
#include <thread>

#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/testing_clock.h"
#include "data_store/disk_data_store.h"
#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"
#include "manifest/content_id.h"

namespace cdc_ft {

namespace {
constexpr uint8_t kFirstData[] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
constexpr size_t kFirstDataSize = sizeof(kFirstData);
constexpr char kTestCacheDirName[] = ".cdc_ft_cache";
}  // namespace

class DataProviderTest : public ::testing::Test {
 public:
  void SetUp() override {
    cache_dir_path_ = path::Join(path::GetTempDir(), kTestCacheDirName);
    EXPECT_OK(path::RemoveDirRec(cache_dir_path_));
  }
  void TearDown() override { EXPECT_OK(path::RemoveDirRec(cache_dir_path_)); }
  DataProviderTest() {
    first_content_id_ = ContentId::FromArray(kFirstData, kFirstDataSize);
  }

  ContentIdProto Id(const std::string& data) {
    return ContentId::FromDataString(data);
  }

  std::unique_ptr<DiskDataStore> CreateDiskCache(
      const std::vector<std::string>& chunks) {
    absl::StatusOr<std::unique_ptr<DiskDataStore>> cache =
        DiskDataStore::Create(0, cache_dir_path_, false, &clock_);
    EXPECT_OK(cache);
    for (const std::string& s : chunks) {
      EXPECT_OK((*cache)->Put(Id(s), s.data(), s.size()));
    }
    return std::move(*cache);
  }

  std::vector<std::unique_ptr<DataStoreReader>> CreateMemCache(
      const std::vector<std::string>& chunks) {
    auto cache = std::make_unique<MemDataStore>();
    for (const std::string& chunk : chunks) {
      cache->AddData({chunk.begin(), chunk.end()});
    }
    std::vector<std::unique_ptr<DataStoreReader>> readers;
    readers.emplace_back(std::move(cache));
    return readers;
  }

  std::unique_ptr<DiskDataStore> CreateCacheWithFirstData() {
    absl::StatusOr<std::unique_ptr<DiskDataStore>> cache =
        DiskDataStore::Create(0, cache_dir_path_, false, &clock_);
    EXPECT_OK(cache);
    EXPECT_OK((*cache)->Put(first_content_id_, &kFirstData[0], kFirstDataSize));
    return std::move(*cache);
  }

  std::unique_ptr<MemDataStore> CreateMemCacheWithFirstData() {
    auto cache = std::make_unique<MemDataStore>();
    cache->AddData({kFirstData, kFirstData + kFirstDataSize});
    return cache;
  }

  std::string GetDiskCacheFilePath(DiskDataStore* dds,
                                   const ContentIdProto& content_id) const {
    return dds->GetCacheFilePath(content_id);
  }

  bool WaitForProviderCleanupAndResetForTesting(DataProvider* dp,
                                                absl::Duration timeout) {
    return dp->WaitForCleanupAndResetForTesting(timeout);
  }

  void TestGetExistingChunkInBounds(DataProvider& data_provider) {
    uint8_t ret_data[kFirstDataSize];
    size_t offset = 3;
    absl::StatusOr<uint64_t> bytes_read =
        data_provider.Get(first_content_id_, &ret_data, offset, kFirstDataSize);
    ASSERT_OK(bytes_read);
    ASSERT_EQ(kFirstDataSize - offset, *bytes_read);
    EXPECT_TRUE(std::equal(std::begin(kFirstData) + offset,
                           std::end(kFirstData), std::begin(ret_data)));
  }

  void TestGetExistingChunkOutOfBounds(DataProvider& data_provider) {
    uint8_t ret_data[kFirstDataSize];
    size_t offset = 15;
    absl::StatusOr<uint64_t> bytes_read =
        data_provider.Get(first_content_id_, &ret_data, offset, kFirstDataSize);
    ASSERT_OK(bytes_read);
    EXPECT_EQ(0u, *bytes_read);
  }

  void TestGetExistingChunkComplete(DataProvider& data_provider) {
    Buffer buffer;
    EXPECT_OK(data_provider.Get(first_content_id_, &buffer));
    Buffer exp_buffer({10, 20, 30, 40, 50, 60, 70, 80, 90});
    EXPECT_EQ(exp_buffer, buffer);
  }

  void TestGetExistingChunk(DataProvider& data_provider) {
    TestGetExistingChunkInBounds(data_provider);
    TestGetExistingChunkOutOfBounds(data_provider);
    TestGetExistingChunkComplete(data_provider);
  }

 protected:
  ContentIdProto first_content_id_;
  TestingSystemClock clock_;
  std::string cache_dir_path_;
};

namespace {

// TODO: Add test with several readers and a writer, which has no data at the
// beginning. Request the chunk several times (the first time it should be
// received from reader, the second time - from the writer).
TEST_F(DataProviderTest, DataProvider) {
  DataProvider data_provider(nullptr, {}, 0);
  uint8_t ret_data[kFirstDataSize];
  absl::StatusOr<uint64_t> bytes =
      data_provider.Get(first_content_id_, &ret_data, 0, kFirstDataSize);
  EXPECT_TRUE(absl::IsNotFound(bytes.status()));

  Buffer buffer;
  EXPECT_TRUE(absl::IsNotFound(data_provider.Get(first_content_id_, &buffer)));
}

TEST_F(DataProviderTest, CacheAsReader) {
  std::vector<std::unique_ptr<DataStoreReader>> readers;
  readers.emplace_back(CreateCacheWithFirstData());
  DataProvider data_provider(nullptr, std::move(readers), 0);

  TestGetExistingChunk(data_provider);
}

TEST_F(DataProviderTest, CacheAsWriter) {
  DataProvider data_provider(CreateCacheWithFirstData(), {}, 0);
  TestGetExistingChunk(data_provider);
}

TEST_F(DataProviderTest, MemCacheAsReader) {
  std::vector<std::unique_ptr<DataStoreReader>> readers;
  readers.emplace_back(CreateMemCacheWithFirstData());
  DataProvider data_provider(nullptr, std::move(readers), 0);
  TestGetExistingChunk(data_provider);
}

TEST_F(DataProviderTest, MemCacheAsWriter) {
  DataProvider data_provider(CreateMemCacheWithFirstData(), {}, 0);
  TestGetExistingChunk(data_provider);
}

TEST_F(DataProviderTest, CacheAsWriterMemCacheAsReader) {
  absl::StatusOr<std::unique_ptr<DiskDataStore>> cache =
      DiskDataStore::Create(0, cache_dir_path_, false, &clock_);
  ASSERT_OK(cache);
  std::vector<std::unique_ptr<DataStoreReader>> readers;
  readers.emplace_back(CreateMemCacheWithFirstData());
  DataProvider data_provider(std::move(*cache), std::move(readers), 0);
  TestGetExistingChunk(data_provider);
}

TEST_F(DataProviderTest, GetMultiChunksFromWriterSuccess) {
  DataProvider data_provider(CreateDiskCache({"aaa", "bbb", "ccc"}), {}, 0);
  char buf[10];
  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  chunks.emplace_back(Id("bbb"), 0, buf + 3, 3);
  chunks.emplace_back(Id("ccc"), 0, buf + 6, 3);
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_TRUE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_TRUE(chunks[1].done);
  EXPECT_TRUE(chunks[2].done);
  EXPECT_EQ(absl::string_view(buf, 9), "aaabbbccc");
}

TEST_F(DataProviderTest, GetMultiChunksFromWriterPartialFail) {
  DataProvider data_provider(CreateDiskCache({"aaa", "bbb", "ccc"}), {}, 0);
  char buf[10];
  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  chunks.emplace_back(Id("does not exist"), 0, buf + 3, sizeof(buf));
  chunks.emplace_back(Id("ccc"), 0, buf + 6, 3);
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_FALSE(chunks.ReadDone());
  EXPECT_FALSE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_FALSE(chunks[1].done);
  EXPECT_TRUE(chunks[2].done);
  EXPECT_EQ(absl::string_view(buf, 3), "aaa");
  EXPECT_EQ(absl::string_view(buf + 6, 3), "ccc");
}

TEST_F(DataProviderTest, GetMultiChunksFromWriterAllFail) {
  DataProvider data_provider(CreateDiskCache({"aaa", "bbb", "ccc"}), {}, 0);
  char buf[10];
  ChunkTransferList chunks;
  chunks.emplace_back(Id("does not exist"), 0, buf, sizeof(buf));
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_FALSE(chunks.ReadDone());
  EXPECT_FALSE(chunks.PrefetchDone());
  EXPECT_FALSE(chunks[0].done);
}

TEST_F(DataProviderTest, GetMultiChunksFromReaderCachedInWriter) {
  auto readers = CreateMemCache({"aaa", "bbb", "ccc"});
  auto disk_cache = CreateDiskCache({});
  DiskDataStore* disk_cache_ptr = disk_cache.get();
  DataProvider data_provider(std::move(disk_cache), std::move(readers), 0);
  char buf[10];
  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_TRUE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_EQ(absl::string_view(buf, 3), "aaa");
  // Verify data has been cached in the writer.
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("aaa")));
  EXPECT_EQ(disk_cache_ptr->List()->size(), 1);

  chunks.clear();
  chunks.emplace_back(Id("bbb"), 0, buf + 3, 3);
  chunks.emplace_back(Id("ccc"), 0, buf + 6, 3);
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_TRUE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_TRUE(chunks[1].done);
  EXPECT_EQ(absl::string_view(buf, 9), "aaabbbccc");
  // Verify data has been cached in the writer.
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("aaa")));
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("bbb")));
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("ccc")));
  EXPECT_EQ(disk_cache_ptr->List()->size(), 3);
}

TEST_F(DataProviderTest, GetMultiChunksFromReaderAndWriterSkipPrefetch) {
  auto readers = CreateMemCache({"bbb", "ccc"});
  auto disk_cache = CreateDiskCache({"aaa"});
  DiskDataStore* disk_cache_ptr = disk_cache.get();
  DataProvider data_provider(std::move(disk_cache), std::move(readers), 0);
  char buf[10];

  // This request can be fulfilled with cached data, so "bbb" and "ccc" are not
  // fetched from the reader.
  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  chunks.emplace_back(Id("bbb"), 0, nullptr, 0);  // prefetch
  chunks.emplace_back(Id("ccc"), 0, nullptr, 0);  // prefetch
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_FALSE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_FALSE(chunks[1].done);
  EXPECT_FALSE(chunks[2].done);
  EXPECT_EQ(absl::string_view(buf, 3), "aaa");
  // No additional chunks should have been cached in the writer.
  EXPECT_EQ(disk_cache_ptr->List()->size(), 1);
}

TEST_F(DataProviderTest, GetMultiChunksFromReaderAndWriterWithPrefetch) {
  auto readers = CreateMemCache({"bbb", "ccc"});
  auto disk_cache = CreateDiskCache({"aaa"});
  DiskDataStore* disk_cache_ptr = disk_cache.get();
  DataProvider data_provider(std::move(disk_cache), std::move(readers), 0);
  char buf[10];

  // This request includes one chunk that has to be fetched, so the third
  // chunk should be prefetched as well.
  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  chunks.emplace_back(Id("bbb"), 0, buf + 3, 3);
  chunks.emplace_back(Id("ccc"), 0, nullptr, 0);  // prefetch
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_TRUE(chunks.PrefetchDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_TRUE(chunks[1].done);
  EXPECT_TRUE(chunks[2].done);
  EXPECT_EQ(absl::string_view(buf, 6), "aaabbb");
  // Verify data has been cached in the writer
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("aaa")));
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("bbb")));
  EXPECT_TRUE(disk_cache_ptr->Contains(Id("ccc")));
  EXPECT_EQ(disk_cache_ptr->List()->size(), 3);
}

TEST_F(DataProviderTest, RecoverFromTruncatedChunkInCache) {
  auto readers = CreateMemCache({"aaa"});
  auto disk_cache = CreateDiskCache({"aaa"});
  DiskDataStore* disk_cache_ptr = disk_cache.get();
  DataProvider data_provider(std::move(disk_cache), std::move(readers), 0);
  char buf[3];

  // Truncate the chunk stored in the disk cache.
  std::string path = GetDiskCacheFilePath(disk_cache_ptr, Id("aaa"));
  size_t size;
  EXPECT_OK(path::WriteFile(path, "a", 1));
  EXPECT_OK(path::FileSize(path, &size));
  EXPECT_EQ(size, 1);

  ChunkTransferList chunks;
  chunks.emplace_back(Id("aaa"), 0, buf, 3);
  EXPECT_OK(data_provider.Get(&chunks));
  EXPECT_TRUE(chunks.ReadDone());
  EXPECT_TRUE(chunks[0].done);
  EXPECT_EQ(absl::string_view(buf, 3), "aaa");
  // Verify that the chunk has been recovered.
  EXPECT_OK(path::FileSize(path, &size));
  EXPECT_EQ(size, 3);
}

TEST_F(DataProviderTest, CleanupNotAllChunksRead) {
  auto cache = CreateDiskCache({"aaa", "bbb", "ccc"});
  cache->SetCapacity(5);

  // Check that chunks are available in the cache first.
  char buf[10];
  EXPECT_EQ(cache->Get(Id("aaa"), buf, 0, 3).value(), 3u);
  EXPECT_EQ(cache->Get(Id("bbb"), buf + 3, 0, 3).value(), 3u);
  EXPECT_EQ(cache->Get(Id("ccc"), buf + 6, 0, 3).value(), 3u);
  EXPECT_EQ(absl::string_view(buf, 9), "aaabbbccc");

  DataProvider data_provider(std::move(cache), {}, 0, 0 /*cleanup timeout*/,
                             0 /*idling timeout*/);
  memset(buf, 0, 10);

  EXPECT_EQ(data_provider.Get(Id("ccc"), buf, 0, 3).value(), 3u);

  // The data provider should contain only 1 chunk as the cleanup was already
  // executed.
  EXPECT_TRUE(WaitForProviderCleanupAndResetForTesting(
      &data_provider, absl::Seconds(5) /*timeout*/));
  memset(buf, 0, 10);
  EXPECT_NOT_OK(data_provider.Get(Id("aaa"), buf, 0, 3));
  EXPECT_NOT_OK(data_provider.Get(Id("bbb"), buf, 0, 3));
  EXPECT_EQ(data_provider.Get(Id("ccc"), buf, 0, 3).value(), 3u);
}

}  // namespace
}  // namespace cdc_ft
