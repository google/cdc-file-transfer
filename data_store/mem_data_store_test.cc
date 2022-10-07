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

#include "data_store/mem_data_store.h"

#include "common/status_test_macros.h"
#include "gtest/gtest.h"
#include "manifest/content_id.h"

namespace cdc_ft {
namespace {

TEST(MemDataStoreTest, GetWithMultipleIds) {
  std::vector<char> expected_data1 = {1, 3, 3, 7};
  std::vector<char> expected_data2 = {15, 0, 0, 13, 0};

  MemDataStore p;
  ContentIdProto id1 = p.AddData(expected_data1);
  ContentIdProto id2 = p.AddData(expected_data2);

  std::vector<char> data1;
  std::vector<char> data2;

  data1.resize(expected_data1.size());
  data2.resize(expected_data2.size());

  absl::StatusOr<uint64_t> bytes_read1 =
      p.Get(id1, data1.data(), 0, data1.size());
  absl::StatusOr<uint64_t> bytes_read2 =
      p.Get(id2, data2.data(), 0, data2.size());

  ASSERT_OK(bytes_read1);
  ASSERT_OK(bytes_read2);

  EXPECT_EQ(*bytes_read1, data1.size());
  EXPECT_EQ(*bytes_read2, data2.size());

  EXPECT_EQ(expected_data1, data1);
  EXPECT_EQ(expected_data2, data2);
}

TEST(MemDataStoreTest, GetWithRangeInsideOfData) {
  MemDataStore p;
  ContentIdProto id = p.AddData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  std::vector<char> data;
  data.resize(5);
  absl::StatusOr<uint64_t> bytes_read =
      p.Get(id, data.data(), /*offset=*/2, data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, data.size());
  EXPECT_EQ(data, std::vector<char>({2, 3, 4, 5, 6}));
}

TEST(MemDataStoreTest, GetWithRangePartlyOutsideOfData) {
  MemDataStore p;
  ContentIdProto id = p.AddData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  std::vector<char> data;
  data.resize(5);
  absl::StatusOr<uint64_t> bytes_read =
      p.Get(id, data.data(), /*offset=*/7, data.size());

  ASSERT_OK(bytes_read);
  ASSERT_EQ(*bytes_read, 3);
  data.resize(3);
  EXPECT_EQ(data, std::vector<char>({7, 8, 9}));
}

TEST(MemDataStoreTest, GetWithRangeOutsideOfData) {
  MemDataStore p;
  ContentIdProto id = p.AddData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  std::vector<char> data;
  data.resize(5);
  absl::StatusOr<uint64_t> bytes_read =
      p.Get(id, data.data(), /*offset=*/12, data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 0);
}

TEST(MemDataStoreTest, GetWholeChunk) {
  std::vector<char> expected_data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  Buffer expected_buffer;
  expected_buffer.append(expected_data.data(), expected_data.size());
  MemDataStore p;
  ContentIdProto id = p.AddData(std::move(expected_data));

  Buffer data;
  EXPECT_OK(p.Get(id, &data));
  EXPECT_EQ(data, expected_buffer);
}

TEST(MemDataStoreTest, GetProtoWithMultipleKeys) {
  AssetProto expected_proto1;
  AssetProto expected_proto2;

  expected_proto1.set_type(AssetProto::DIRECTORY);
  expected_proto2.set_type(AssetProto::FILE);

  expected_proto1.set_name("dir");
  expected_proto2.set_name("file");

  // Use a MemDataStore to get test data in.
  // Note that GetProto is implemented by DataStoreReader.
  MemDataStore p;
  ContentIdProto key1 = p.AddProto(expected_proto1);
  ContentIdProto key2 = p.AddProto(expected_proto2);

  AssetProto proto1;
  AssetProto proto2;

  EXPECT_OK(p.GetProto(key1, &proto1));
  EXPECT_OK(p.GetProto(key2, &proto2));

  EXPECT_EQ(expected_proto1.type(), proto1.type());
  EXPECT_EQ(expected_proto1.type(), proto1.type());

  EXPECT_EQ(expected_proto1.name(), proto1.name());
  EXPECT_EQ(expected_proto2.name(), proto2.name());
}

TEST(MemDataStoreTest, PutGet) {
  std::vector<char> expected_data = {1, 3, 3, 7};
  ContentIdProto content_id =
      ContentId::FromArray(expected_data.data(), expected_data.size());

  MemDataStore p;
  ASSERT_OK(p.Put(content_id, expected_data.data(), expected_data.size()));
  ASSERT_TRUE(p.Contains(content_id));

  std::vector<char> data;
  data.resize(expected_data.size());
  absl::StatusOr<uint64_t> bytes_read =
      p.Get(content_id, data.data(), 0, data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, data.size());
  EXPECT_EQ(expected_data, data);
}

TEST(MemDataStoreTest, PruneSucceeds) {
  MemDataStore p;
  ContentIdProto content_ids[4];
  for (size_t n = 0; n < std::size(content_ids); ++n) {
    content_ids[n] = ContentId::FromArray(&n, sizeof(n));
    EXPECT_OK(p.Put(content_ids[n], &n, sizeof(n)));
  }

  std::unordered_set<ContentIdProto> ids_to_keep = {content_ids[0],
                                                    content_ids[2]};
  EXPECT_OK(p.Prune(std::move(ids_to_keep)));

  EXPECT_TRUE(p.Contains(content_ids[0]));
  EXPECT_TRUE(p.Contains(content_ids[2]));

  EXPECT_FALSE(p.Contains(content_ids[1]));
  EXPECT_FALSE(p.Contains(content_ids[3]));
}

TEST(MemDataStoreTest, PruneFailsNotFound) {
  MemDataStore p;
  ContentIdProto content_ids[2];
  for (size_t n = 0; n < std::size(content_ids); ++n)
    content_ids[n] = ContentId::FromArray(&n, sizeof(n));
  EXPECT_OK(p.Put(content_ids[0], nullptr, 0));

  std::unordered_set<ContentIdProto> ids_to_keep = {content_ids[1]};
  EXPECT_TRUE(absl::IsNotFound(p.Prune(std::move(ids_to_keep))));

  EXPECT_FALSE(p.Contains(content_ids[0]));
}

}  // namespace
}  // namespace cdc_ft
