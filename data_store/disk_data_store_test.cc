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

#include "data_store/disk_data_store.h"

#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/testing_clock.h"
#include "gtest/gtest.h"
#include "manifest/content_id.h"

namespace cdc_ft {

namespace {
constexpr uint8_t kFirstData[] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
constexpr uint8_t kSecondData[] = {100, 101, 102, 103, 104, 105, 106};
constexpr size_t kFirstDataSize = sizeof(kFirstData);
constexpr size_t kSecondDataSize = sizeof(kSecondData);
constexpr char kTestCacheDirName[] = ".cdc_ft_cache";

class DiskDataStoreTest : public ::testing::Test {
 public:
  DiskDataStoreTest() {
    first_content_id_ = ContentId::FromArray(kFirstData, kFirstDataSize);
    second_content_id_ = ContentId::FromArray(kSecondData, kSecondDataSize);
  }
  void SetUp() override {
    cache_dir_path_ = path::Join(path::GetTempDir(), kTestCacheDirName);
    EXPECT_OK(path::RemoveDirRec(cache_dir_path_));
  }
  void TearDown() override { EXPECT_OK(path::RemoveDirRec(cache_dir_path_)); }

  std::unique_ptr<DiskDataStore> CreateCache(unsigned int depth,
                                             bool create_dirs = false) {
    absl::StatusOr<std::unique_ptr<DiskDataStore>> cache =
        DiskDataStore::Create(depth, cache_dir_path_, create_dirs, &clock_);
    EXPECT_OK(cache);
    return std::move(*cache);
  }

 protected:
  ContentIdProto first_content_id_;
  ContentIdProto second_content_id_;
  TestingSystemClock clock_;
  std::string cache_dir_path_;
};

TEST_F(DiskDataStoreTest, DiskDataStore) {
  auto cache = CreateCache(0);
  EXPECT_EQ(0u, cache->Size());

  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(0u, statistics->size);
  EXPECT_EQ(0u, statistics->number_of_chunks);

  EXPECT_GT(cache->Capacity(), 0);
  EXPECT_GT(cache->FillFactor(), 0);
  EXPECT_LT(cache->FillFactor(), 1);
}

TEST_F(DiskDataStoreTest, PutGet) {
  auto cache = CreateCache(2);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_EQ(kFirstDataSize, cache->Size());
  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kFirstDataSize, statistics->size);
  EXPECT_EQ(1u, statistics->number_of_chunks);
  EXPECT_TRUE(cache->Contains(first_content_id_));

  uint8_t ret_data[kFirstDataSize];
  absl::StatusOr<uint64_t> bytes_read =
      cache->Get(first_content_id_, &ret_data, 0, kFirstDataSize);
  EXPECT_EQ(kFirstDataSize, cache->Size());
  ASSERT_OK(bytes_read);
  ASSERT_EQ(kFirstDataSize, *bytes_read);
  EXPECT_TRUE(std::equal(std::begin(kFirstData), std::end(kFirstData),
                         std::begin(ret_data)));
  statistics = cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kFirstDataSize, statistics->size);
  EXPECT_EQ(1u, statistics->number_of_chunks);
}

TEST_F(DiskDataStoreTest, GetBuffer) {
  auto cache = CreateCache(1);
  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));

  Buffer buffer;
  EXPECT_OK(cache->Get(first_content_id_, &buffer));
  Buffer exp_buffer({10, 20, 30, 40, 50, 60, 70, 80, 90});
  EXPECT_EQ(exp_buffer, buffer);
}

TEST_F(DiskDataStoreTest, Wipe) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_EQ(kFirstDataSize, cache->Size());
  EXPECT_OK(cache->Wipe());
  EXPECT_EQ(0u, cache->Size());
  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(0u, statistics->size);
  EXPECT_EQ(0u, statistics->number_of_chunks);
  EXPECT_FALSE(cache->Contains(first_content_id_));
}

TEST_F(DiskDataStoreTest, PruneSucceeds) {
  auto cache = CreateCache(2);

  ContentIdProto content_ids[4];
  for (size_t n = 0; n < std::size(content_ids); ++n) {
    content_ids[n] = ContentId::FromArray(&n, sizeof(n));
    EXPECT_OK(cache->Put(content_ids[n], &n, sizeof(n)));
  }

  std::unordered_set<ContentIdProto> ids_to_keep = {content_ids[0],
                                                    content_ids[2]};
  EXPECT_OK(cache->Prune(std::move(ids_to_keep)));
  EXPECT_TRUE(cache->Contains(content_ids[0]));
  EXPECT_TRUE(cache->Contains(content_ids[2]));
  EXPECT_EQ(2 * sizeof(size_t), cache->Size());

  EXPECT_FALSE(cache->Contains(content_ids[1]));
  EXPECT_FALSE(cache->Contains(content_ids[3]));
}

TEST_F(DiskDataStoreTest, PruneFailsNotFound) {
  auto cache = CreateCache(2);

  ContentIdProto content_ids[2];
  for (size_t n = 0; n < std::size(content_ids); ++n)
    content_ids[n] = ContentId::FromArray(&n, sizeof(n));
  EXPECT_OK(cache->Put(content_ids[0], nullptr, 0));

  std::unordered_set<ContentIdProto> ids_to_keep = {content_ids[1]};
  EXPECT_TRUE(absl::IsNotFound(cache->Prune(std::move(ids_to_keep))));

  EXPECT_FALSE(cache->Contains(content_ids[0]));
}

TEST_F(DiskDataStoreTest, SetCapacity) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  cache->SetCapacity(0);
  EXPECT_OK(cache->Cleanup());
  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(0u, statistics->size);
  EXPECT_EQ(0u, statistics->number_of_chunks);
  EXPECT_FALSE(cache->Contains(first_content_id_));
}

TEST_F(DiskDataStoreTest, SetFillFactor) {
  auto cache = CreateCache(2);

  EXPECT_OK(cache->SetFillFactor(0.1));
  EXPECT_NOT_OK(cache->SetFillFactor(0));
  EXPECT_NOT_OK(cache->SetFillFactor(100));
  EXPECT_OK(cache->SetFillFactor(1));
  EXPECT_EQ(1, cache->FillFactor());
}

TEST_F(DiskDataStoreTest, GetNonExisting) {
  auto cache = CreateCache(0);

  EXPECT_FALSE(cache->Contains(first_content_id_));

  uint8_t ret_data[kFirstDataSize];
  absl::StatusOr<size_t> read_bytes =
      cache->Get(first_content_id_, &ret_data, 0, kFirstDataSize);
  EXPECT_TRUE(absl::IsNotFound(read_bytes.status()));

  Buffer buffer;
  EXPECT_TRUE(absl::IsNotFound(cache->Get(first_content_id_, &buffer)));
}

TEST_F(DiskDataStoreTest, PutTwoRemoveOne) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_EQ(kFirstDataSize, cache->Size());
  clock_.Advance(1000);
  EXPECT_OK(cache->Put(second_content_id_, kSecondData, kSecondDataSize));

  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kFirstDataSize + kSecondDataSize, statistics->size);
  EXPECT_EQ(statistics->size, cache->Size());
  EXPECT_EQ(2u, statistics->number_of_chunks);

  cache->SetCapacity(kFirstDataSize + 4);
  EXPECT_OK(cache->Cleanup());

  statistics = cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kSecondDataSize, statistics->size);
  EXPECT_EQ(statistics->size, cache->Size());
  EXPECT_EQ(1u, statistics->number_of_chunks);

  EXPECT_FALSE(cache->Contains(first_content_id_));
  EXPECT_TRUE(cache->Contains(second_content_id_));
}

TEST_F(DiskDataStoreTest, PutTwoReadOldRemoveOne) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  clock_.Advance(1000);
  EXPECT_OK(cache->Put(second_content_id_, kSecondData, kSecondDataSize));
  clock_.Advance(1000);
  uint8_t ret_data[kFirstDataSize];
  EXPECT_OK(
      cache->Get(first_content_id_, &ret_data, 0, kFirstDataSize).status());

  // second_key should be removed after the cleanup.
  cache->SetCapacity(kFirstDataSize + 4);
  EXPECT_OK(cache->Cleanup());

  EXPECT_TRUE(cache->Contains(first_content_id_));
  EXPECT_FALSE(cache->Contains(second_content_id_));

  uint8_t ret_data2[kFirstDataSize];
  absl::StatusOr<uint64_t> bytes_read =
      cache->Get(first_content_id_, &ret_data2, 0, kFirstDataSize);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(kFirstDataSize, *bytes_read);
  EXPECT_TRUE(std::equal(std::begin(kFirstData), std::end(kFirstData),
                         std::begin(ret_data)));
}

TEST_F(DiskDataStoreTest, GetWithZeroLength) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));

  uint8_t ret_data[1];
  absl::StatusOr<size_t> read_bytes =
      cache->Get(first_content_id_, &ret_data, 0, 0);
  ASSERT_OK(read_bytes);
  ASSERT_EQ(0u, *read_bytes);
}

TEST_F(DiskDataStoreTest, GetWithOffset) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));

  size_t const offset = 3;
  size_t const len = kFirstDataSize - offset;
  uint8_t ret_data[len];
  absl::StatusOr<size_t> read_bytes =
      cache->Get(first_content_id_, &ret_data, offset, len);
  ASSERT_OK(read_bytes);
  ASSERT_EQ(len, *read_bytes);
  EXPECT_TRUE(std::equal(std::begin(kFirstData) + offset, std::end(kFirstData),
                         std::begin(ret_data)));
}

TEST_F(DiskDataStoreTest, GetWithWrongOffset) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));

  uint8_t ret_data[kFirstDataSize];
  absl::StatusOr<size_t> read_bytes =
      cache->Get(first_content_id_, &ret_data, 1000, kFirstDataSize);
  ASSERT_OK(read_bytes);
  ASSERT_EQ(0, *read_bytes);
}

TEST_F(DiskDataStoreTest, GetWithTooBigLength) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));

  uint8_t ret_data[kFirstDataSize + 10];
  absl::StatusOr<size_t> read_bytes =
      cache->Get(first_content_id_, &ret_data, 0, kFirstDataSize + 10);
  ASSERT_OK(read_bytes);
  ASSERT_EQ(kFirstDataSize, *read_bytes);
}

TEST_F(DiskDataStoreTest, Remove) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_OK(cache->Remove(first_content_id_));
  EXPECT_FALSE(cache->Contains(first_content_id_));
  EXPECT_OK(cache->Remove(first_content_id_));
}

TEST_F(DiskDataStoreTest, CreateCacheIfRootDirExists) {
  auto cache1 = CreateCache(1);
  EXPECT_OK(cache1->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_OK(cache1->Put(second_content_id_, kSecondData, kSecondDataSize));

  auto cache2 = CreateCache(1);
  absl::StatusOr<DiskDataStore::Statistics> statistics1 =
      cache1->CalculateStatistics();
  ASSERT_OK(statistics1);
  absl::StatusOr<DiskDataStore::Statistics> statistics2 =
      cache2->CalculateStatistics();
  ASSERT_OK(statistics2);

  EXPECT_EQ(statistics1->size, statistics2->size);
  EXPECT_EQ(statistics1->number_of_chunks, statistics2->number_of_chunks);
  EXPECT_EQ(cache2->Capacity(), cache1->Capacity());
  EXPECT_EQ(cache2->FillFactor(), cache1->FillFactor());
  EXPECT_EQ(cache2->Depth(), cache1->Depth());
  EXPECT_TRUE(cache2->Contains(first_content_id_));
  EXPECT_TRUE(cache2->Contains(second_content_id_));
}

TEST_F(DiskDataStoreTest, CacheWithDirectories) {
  unsigned int depth = 1;
  unsigned int dir_count = 0;
  auto cache = CreateCache(depth, true);

  EXPECT_EQ(depth, cache->Depth());

  auto handler = [&dir_count](const std::string& /*dir*/,
                              const std::string& /*filename*/,
                              int64_t /*modified_time*/, uint64_t /*size*/,
                              bool is_directory) -> absl::Status {
    if (is_directory) {
      ++dir_count;
    }
    return absl::OkStatus();
  };
  EXPECT_OK(path::SearchFiles(cache->RootDir(), true, handler));
  EXPECT_EQ(dir_count,
            std::pow(std::pow(16, DiskDataStore::kDirNameLength), depth));
}

TEST_F(DiskDataStoreTest, CacheWithDirectoriesOnDemand) {
  unsigned int depth = 4;
  unsigned int dir_count = 0;
  auto cache = CreateCache(depth, false);

  EXPECT_EQ(depth, cache->Depth());
  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  auto handler = [&dir_count](const std::string& /*dir*/,
                              const std::string& /*filename*/,
                              int64_t /*modified_time*/, uint64_t /*size*/,
                              bool is_directory) -> absl::Status {
    if (is_directory) {
      ++dir_count;
    }
    return absl::OkStatus();
  };
  EXPECT_OK(path::SearchFiles(cache->RootDir(), true, handler));
  EXPECT_EQ(dir_count, 4u);
}

TEST_F(DiskDataStoreTest, OverwriteExistingEntry) {
  auto cache = CreateCache(0, true);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  EXPECT_OK(cache->Put(first_content_id_, kSecondData, kSecondDataSize));
  uint8_t ret_data[kSecondDataSize];
  absl::StatusOr<uint64_t> bytes_read =
      cache->Get(first_content_id_, &ret_data, 0, kSecondDataSize);
  ASSERT_OK(bytes_read);
  ASSERT_EQ(kSecondDataSize, *bytes_read);
  EXPECT_TRUE(std::equal(std::begin(kSecondData), std::end(kSecondData),
                         std::begin(ret_data)));
}

TEST_F(DiskDataStoreTest, List) {
  auto cache = CreateCache(0, true);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, 1));
  EXPECT_OK(cache->Put(second_content_id_, kSecondData, 1));

  absl::StatusOr<std::vector<ContentIdProto>> ids = cache->List();
  ASSERT_OK(ids);
  ASSERT_EQ(ids->size(), 2);

  if (ids->at(0) == second_content_id_) std::swap(ids->at(0), ids->at(1));
  EXPECT_TRUE(ids->at(0) == first_content_id_);
  EXPECT_TRUE(ids->at(1) == second_content_id_);
}

TEST_F(DiskDataStoreTest, InterruptCleanup) {
  auto cache = CreateCache(0);

  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  cache->SetCapacity(0);
  std::atomic<bool> interrupt{true};
  cache->RegisterInterrupt(&interrupt);
  EXPECT_TRUE(absl::IsCancelled(cache->Cleanup()));

  absl::StatusOr<DiskDataStore::Statistics> statistics =
      cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kFirstDataSize, statistics->size);
  EXPECT_EQ(1u, statistics->number_of_chunks);
  EXPECT_TRUE(cache->Contains(first_content_id_));

  // Resetting interrupt should enable Cleanup().
  interrupt = false;
  EXPECT_OK(cache->Cleanup());

  statistics = cache->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(0u, statistics->size);
  EXPECT_EQ(0u, statistics->number_of_chunks);
  EXPECT_FALSE(cache->Contains(first_content_id_));
}

TEST_F(DiskDataStoreTest, CleanupForPrefilledCacheSuccess) {
  auto cache = CreateCache(0);
  EXPECT_OK(cache->Put(first_content_id_, kFirstData, kFirstDataSize));
  clock_.Advance(1000);

  absl::StatusOr<std::unique_ptr<DiskDataStore>> filled_cache =
      DiskDataStore::Create(0, cache_dir_path_, false, &clock_);
  EXPECT_OK(filled_cache);
  EXPECT_OK(
      (*filled_cache)->Put(second_content_id_, kSecondData, kSecondDataSize));
  (*filled_cache)->SetCapacity(kFirstDataSize + 4);
  EXPECT_OK((*filled_cache)->Cleanup());

  absl::StatusOr<DiskDataStore::Statistics> statistics =
      (*filled_cache)->CalculateStatistics();
  ASSERT_OK(statistics);
  EXPECT_EQ(kSecondDataSize, statistics->size);
  EXPECT_EQ(1u, statistics->number_of_chunks);
}

}  // namespace
}  // namespace cdc_ft
