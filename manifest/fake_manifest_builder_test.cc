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

#include "manifest/fake_manifest_builder.h"

#include "common/status_test_macros.h"
#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

TEST(FakeManifestBuilderTest, RootDir) {
  MemDataStore store;
  FakeManifestBuilder builder(&store);

  const AssetProto& root = builder.Manifest()->root_dir();
  EXPECT_EQ(&root, builder.Root());
  EXPECT_EQ(root.type(), AssetProto::DIRECTORY);
  EXPECT_TRUE(root.name().empty());
  ASSERT_EQ(root.dir_assets().size(), 0);
}

TEST(FakeManifestBuilderTest, AddFile) {
  MemDataStore store;
  FakeManifestBuilder builder(&store);

  std::vector<char> expected_data = {1, 3, 3, 7};
  builder.AddFile(builder.Root(), "file", 12345, 0750, expected_data);
  const AssetProto& root = builder.Manifest()->root_dir();

  ASSERT_EQ(root.dir_assets().size(), 1);
  const AssetProto& file = root.dir_assets(0);
  EXPECT_EQ(file.name(), "file");
  EXPECT_EQ(file.type(), AssetProto::FILE);
  EXPECT_EQ(file.mtime_seconds(), 12345);
  EXPECT_EQ(file.permissions(), 0750);

  ASSERT_EQ(file.file_chunks_size(), 1);
  const ChunkRefProto& chunk = file.file_chunks(0);
  EXPECT_EQ(chunk.offset(), 0);

  // Try to read a byte more to see if it's properly clamped.
  std::vector<char> data;
  data.resize(expected_data.size() + 1);
  absl::StatusOr<uint64_t> bytes_read =
      store.Get(chunk.chunk_id(), data.data(), 0, data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, expected_data.size());
  data.resize(expected_data.size());
}

TEST(FakeManifestBuilderTest, AddDirectory) {
  MemDataStore store;
  FakeManifestBuilder builder(&store);

  AssetProto* dir = builder.AddDirectory(builder.Root(), "dir", 12345, 0750);
  builder.AddFile(dir, "file", 23456, 0321, {});
  const AssetProto& root = builder.Manifest()->root_dir();

  ASSERT_EQ(root.dir_assets().size(), 1);

  EXPECT_EQ(&root.dir_assets(0), dir);
  EXPECT_EQ(dir->name(), "dir");
  EXPECT_EQ(dir->type(), AssetProto::DIRECTORY);
  EXPECT_EQ(dir->mtime_seconds(), 12345);
  EXPECT_EQ(dir->permissions(), 0750);

  ASSERT_EQ(dir->dir_assets_size(), 1);
  const AssetProto& file = dir->dir_assets(0);
  EXPECT_EQ(file.name(), "file");
}

TEST(FakeManifestBuilderTest, ModifyFile) {
  MemDataStore store;
  FakeManifestBuilder builder(&store);

  std::vector<char> expected_data = {1, 3, 3, 7, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1};
  builder.AddFile(builder.Root(), "file", 12345, 0750, expected_data);
  expected_data = {2, 4, 4, 3};
  builder.ModifyFile(builder.Root(), "file", 14843, 0666, expected_data);

  const AssetProto& root = builder.Manifest()->root_dir();

  ASSERT_EQ(root.dir_assets().size(), 1);
  const AssetProto& file = root.dir_assets(0);
  EXPECT_EQ(file.name(), "file");
  EXPECT_EQ(file.type(), AssetProto::FILE);
  EXPECT_EQ(file.mtime_seconds(), 14843);
  EXPECT_EQ(file.permissions(), 0666);

  ASSERT_EQ(file.file_chunks_size(), 1);
  const ChunkRefProto& chunk = file.file_chunks(0);
  EXPECT_EQ(chunk.offset(), 0);

  // Try to read a byte more to see if it's properly clamped.
  std::vector<char> data;
  data.resize(expected_data.size() + 1);
  absl::StatusOr<uint64_t> bytes_read =
      store.Get(chunk.chunk_id(), data.data(), 0, data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, expected_data.size());
  data.resize(expected_data.size());
}

}  // namespace
}  // namespace cdc_ft
