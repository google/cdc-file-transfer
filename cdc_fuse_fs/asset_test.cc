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

#include "cdc_fuse_fs/asset.h"

#include "absl/strings/match.h"
#include "common/buffer.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class AssetTest : public ::testing::Test {
 public:
  AssetTest()
      : bad_id_(ContentId::FromDataString(std::string("does not exist"))) {
    for (size_t n = 0; n < kNumChildProtos; ++n) {
      child_protos_[n].set_name("file" + std::to_string(n));
    }
  }

 protected:
  static constexpr Asset::ino_t kParentIno = 1;

  // Adds chunks with the data given by |data_vec| to the store, and
  // adds references to the chunks to |list|. Updates |offset|.
  void AddChunks(std::vector<std::vector<char>> data_vec, uint64_t* offset,
                 RepeatedChunkRefProto* list) {
    for (auto& data : data_vec) {
      ChunkRefProto* chunk_ref = list->Add();
      chunk_ref->set_offset(*offset);
      *offset += data.size();
      *chunk_ref->mutable_chunk_id() = store_.AddData(std::move(data));
    }
  }

  // Adds chunks with the data given by |data_vec| to the store,
  // creates an indirect chunk list from those chunks and adds a reference to
  // that list to |list|.
  void AddIndirectChunks(std::vector<std::vector<char>> data_vec,
                         uint64_t* offset,
                         RepeatedIndirectChunkListProto* list) {
    uint64_t indirect_list_offset = *offset;
    *offset = 0;

    ChunkListProto chunk_list;
    AddChunks(data_vec, offset, chunk_list.mutable_chunks());

    IndirectChunkListProto* indirect_list = list->Add();
    indirect_list->set_offset(indirect_list_offset);
    *indirect_list->mutable_chunk_list_id() = store_.AddProto(chunk_list);
    *offset += indirect_list_offset;
  }

  // Checks if the given list |protos| contains an asset having |name|.
  static bool ContainsAsset(const std::vector<const AssetProto*>& protos,
                            const std::string& name) {
    return std::find_if(protos.begin(), protos.end(), [=](const AssetProto* p) {
             return p->name() == name;
           }) != protos.end();
  }

  MemDataStore store_;
  AssetProto proto_;
  Asset asset_;

  static constexpr size_t kNumChildProtos = 4;
  AssetProto child_protos_[kNumChildProtos];

  const ContentIdProto bad_id_;
  std::string asset_check_;
};

TEST_F(AssetTest, BasicGetters) {
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_EQ(asset_.parent_ino(), kParentIno);
  EXPECT_EQ(asset_.proto(), &proto_);
  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ("Undefined asset type", asset_check_.c_str());
}

TEST_F(AssetTest, GetAllChildProtosDirectSucceeds) {
  // Put all children into the direct asset list.
  for (size_t n = 0; n < kNumChildProtos; ++n)
    *proto_.add_dir_assets() = child_protos_[n];
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();

  ASSERT_OK(protos);
  ASSERT_EQ(protos->size(), kNumChildProtos);
  for (size_t n = 0; n < kNumChildProtos; ++n) {
    EXPECT_TRUE(ContainsAsset(protos.value(), child_protos_[n].name()))
        << "Could not find asset " << child_protos_[n].name();
  }
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, GetAllChildProtosIndirectSucceeds) {
  // Put child0 into the direct asset list and children 1-N into indirect lists.
  *proto_.add_dir_assets() = child_protos_[0];
  for (size_t n = 1; n < kNumChildProtos; ++n) {
    AssetListProto list;
    *list.add_assets() = child_protos_[n];
    *proto_.add_dir_indirect_assets() = store_.AddProto(list);
  }
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();

  EXPECT_EQ(asset_.GetNumFetchedDirAssetsListsForTesting(),
            kNumChildProtos - 1);
  ASSERT_OK(protos);
  ASSERT_EQ(protos->size(), kNumChildProtos);
  for (size_t n = 0; n < kNumChildProtos; ++n) {
    EXPECT_TRUE(ContainsAsset(protos.value(), child_protos_[n].name()))
        << "Could not find asset " << child_protos_[n].name();
  }
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, GetAllChildProtosWithBadListIdFails) {
  *proto_.add_dir_indirect_assets() = bad_id_;
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();

  ASSERT_NOT_OK(protos);
  EXPECT_TRUE(absl::StrContains(protos.status().message(),
                                "Failed to fetch directory assets"));
}

TEST_F(AssetTest, GetAllChildProtosWithWrongTypeFails) {
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();

  ASSERT_NOT_OK(protos);
  EXPECT_TRUE(absl::IsInvalidArgument(protos.status()));
}

TEST_F(AssetTest, GetLoadedChildProtosSucceedsForEmpty) {
  asset_.Initialize(kParentIno, &store_, &proto_);
  EXPECT_TRUE(asset_.GetLoadedChildProtos().empty());
  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ("Undefined asset type", asset_check_.c_str());
}

TEST_F(AssetTest, GetLoadedChildProtosSucceedsForNonEmpty) {
  // Put child0 into the direct asset list and children 1-N into indirect lists.
  *proto_.add_dir_assets() = child_protos_[0];
  for (size_t n = 1; n < kNumChildProtos; ++n) {
    AssetListProto list;
    *list.add_assets() = child_protos_[n];
    *proto_.add_dir_indirect_assets() = store_.AddProto(list);
  }
  proto_.set_type(AssetProto::DIRECTORY);

  // The direct list is always loaded.
  asset_.Initialize(kParentIno, &store_, &proto_);
  std::vector<const AssetProto*> protos = asset_.GetLoadedChildProtos();
  ASSERT_EQ(protos.size(), 1);
  EXPECT_EQ(protos[0]->name(), child_protos_[0].name());

  // A lookup for the first child triggers loading of the first indirect list.
  EXPECT_OK(asset_.Lookup(child_protos_[1].name().c_str()));
  protos = asset_.GetLoadedChildProtos();
  ASSERT_EQ(protos.size(), 2);
  EXPECT_TRUE(ContainsAsset(protos, child_protos_[0].name()));
  EXPECT_TRUE(ContainsAsset(protos, child_protos_[1].name()));

  // GetAllChildProtos() triggers loading of all indirect lists.
  EXPECT_OK(asset_.GetAllChildProtos());
  protos = asset_.GetLoadedChildProtos();
  ASSERT_EQ(protos.size(), 4u);
  for (size_t n = 0; n < protos.size(); ++n) {
    EXPECT_TRUE(ContainsAsset(protos, child_protos_[n].name()))
        << "Could not find asset " << child_protos_[n].name();
  }
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, LookupSucceeds) {
  // Put child0 into the direct asset list and children 1-N into indirect lists.
  *proto_.add_dir_assets() = child_protos_[0];
  for (size_t n = 1; n < kNumChildProtos; ++n) {
    AssetListProto list;
    *list.add_assets() = child_protos_[n];
    *proto_.add_dir_indirect_assets() = store_.AddProto(list);
  }
  proto_.set_type(AssetProto::DIRECTORY);

  // Indirect asset lists should be fetched in a lazy fashion.
  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<const AssetProto*> file0 = asset_.Lookup("file0");
  EXPECT_EQ(asset_.GetNumFetchedDirAssetsListsForTesting(), 0);
  absl::StatusOr<const AssetProto*> file1 = asset_.Lookup("file1");
  EXPECT_EQ(asset_.GetNumFetchedDirAssetsListsForTesting(), 1);
  absl::StatusOr<const AssetProto*> file3 = asset_.Lookup("file3");
  EXPECT_EQ(asset_.GetNumFetchedDirAssetsListsForTesting(), 3);

  ASSERT_OK(file0);
  ASSERT_OK(file1);
  ASSERT_OK(file3);

  ASSERT_NE(*file0, nullptr);
  ASSERT_NE(*file1, nullptr);
  ASSERT_NE(*file3, nullptr);

  EXPECT_EQ((*file0)->name(), child_protos_[0].name());
  EXPECT_EQ((*file1)->name(), child_protos_[1].name());
  EXPECT_EQ((*file3)->name(), child_protos_[3].name());

  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, LookupNotFoundSucceeds) {
  // Put child0 into the direct asset list and children 1-N into indirect lists.
  *proto_.add_dir_assets() = child_protos_[0];
  for (size_t n = 1; n < kNumChildProtos; ++n) {
    AssetListProto list;
    *list.add_assets() = child_protos_[n];
    *proto_.add_dir_indirect_assets() = store_.AddProto(list);
  }
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<const AssetProto*> proto = asset_.Lookup("non_existing");

  EXPECT_EQ(asset_.GetNumFetchedDirAssetsListsForTesting(),
            kNumChildProtos - 1);
  ASSERT_OK(proto);
  ASSERT_EQ(*proto, nullptr);

  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, LookupWithWrongTypeFails) {
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<const AssetProto*> proto = asset_.Lookup("foo");

  ASSERT_NOT_OK(proto);
  EXPECT_TRUE(absl::IsInvalidArgument(proto.status()));
}

TEST_F(AssetTest, LookupWithBadListIdFails) {
  *proto_.add_dir_assets() = child_protos_[0];
  *proto_.add_dir_indirect_assets() = bad_id_;
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);

  // This should succeed since 'file0' on the direct assets list.
  ASSERT_OK(asset_.Lookup("file0"));

  // This should fail since it should trigger loading the bad id.
  absl::StatusOr<const AssetProto*> proto = asset_.Lookup("file1");
  ASSERT_NOT_OK(proto);
  EXPECT_TRUE(absl::StrContains(proto.status().message(),
                                "Failed to fetch directory assets"));
}

TEST_F(AssetTest, ReadDirectSucceeds) {
  uint64_t offset = 0;
  AddChunks({{1, 2}, {3, 4}}, &offset, proto_.mutable_file_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(4);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 4);
  EXPECT_EQ(data, std::vector<char>({1, 2, 3, 4}));
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, ReadIndirectSucceeds) {
  uint64_t offset = 0;
  AddChunks({{1, 2}}, &offset, proto_.mutable_file_chunks());
  AddIndirectChunks({{3}, {4, 5, 6}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{7, 8, 9}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(9);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 9);
  EXPECT_EQ(data, std::vector<char>({1, 2, 3, 4, 5, 6, 7, 8, 9}));
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, ReadIndirectOnlySucceeds) {
  uint64_t offset = 0;
  AddIndirectChunks({{1, 2}}, &offset, proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(2);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 2);
  EXPECT_EQ(data, std::vector<char>({1, 2}));
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ("", asset_check_.c_str());
}

TEST_F(AssetTest, ReadWithWrongType) {
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(1);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(absl::IsInvalidArgument(bytes_read.status()));
}

TEST_F(AssetTest, ReadIndirectWithBadListIdFails) {
  IndirectChunkListProto* indirect_list = proto_.add_file_indirect_chunks();
  indirect_list->set_offset(0);
  *indirect_list->mutable_chunk_list_id() = bad_id_;
  proto_.set_file_size(1);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(1);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(absl::StrContains(bytes_read.status().message(),
                                "Failed to fetch indirect chunk list 0"));
}

TEST_F(AssetTest, ReadFetchesIndirectListsLazily) {
  uint64_t offset = 0;
  AddChunks({{0, 1, 2}}, &offset, proto_.mutable_file_chunks());
  AddIndirectChunks({{3}}, &offset, proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{4, 5, 6}, {7}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{8, 9}}, &offset, proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);
  EXPECT_EQ(asset_.GetNumFetchedFileChunkListsForTesting(), 0);

  // Read direct chunks. Should not trigger indirect reads.
  std::vector<char> data(10);
  absl::StatusOr<uint64_t> bytes_read = asset_.Read(0, data.data(), 3);
  EXPECT_EQ(asset_.GetNumFetchedFileChunkListsForTesting(), 0);

  // Read an indirect chunk near the end ({ {8, 9} }).
  bytes_read = asset_.Read(8, data.data(), 1);
  EXPECT_EQ(asset_.GetNumFetchedFileChunkListsForTesting(), 1);

  // Read an indirect chunk in the beginning ({ {3} }).
  bytes_read = asset_.Read(3, data.data(), 1);
  EXPECT_EQ(asset_.GetNumFetchedFileChunkListsForTesting(), 2);

  // Read an indirect chunk in the middle ({ {4, 5, 6}, {7} }).
  bytes_read = asset_.Read(4, data.data(), 4);
  EXPECT_EQ(asset_.GetNumFetchedFileChunkListsForTesting(), 3);
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, ReadEmptySucceeds) {
  asset_.Initialize(kParentIno, &store_, &proto_);
  proto_.set_type(AssetProto::FILE);

  std::vector<char> data(4);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 0);
}

TEST_F(AssetTest, ReadEmptyDirectChunkSucceeds) {
  uint64_t offset = 0;
  AddChunks({{}, {1}}, &offset, proto_.mutable_file_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(4);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 1);
  data.resize(1);
  EXPECT_EQ(data, std::vector<char>({1}));
}

TEST_F(AssetTest, ReadEmptyIndirectChunkListFails) {
  uint64_t offset = 0;
  AddChunks({{1}}, &offset, proto_.mutable_file_chunks());
  AddIndirectChunks({}, &offset, proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{}}, &offset, proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{2}}, &offset, proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(4);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 2);
  data.resize(2);
  EXPECT_EQ(data, std::vector<char>({1, 2}));
}

TEST_F(AssetTest, ReadWithBadFileSizeFails) {
  // Construct a case where the second chunk is empty, but file size indicates
  // that it should be 1 byte long. Reading that byte should fail.
  uint64_t offset = 0;
  AddChunks({{1}, {}}, &offset, proto_.mutable_file_chunks());
  proto_.set_file_size(offset + 1);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(1);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(1, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(
      absl::StrContains(bytes_read.status().message(),
                        "requested offset 0 is larger or equal than size 0"));
}

TEST_F(AssetTest, ReadWithBadChunkIdSizeFails) {
  uint64_t offset = 0;
  AddChunks({{1}}, &offset, proto_.mutable_file_chunks());
  *proto_.mutable_file_chunks(0)->mutable_chunk_id() = bad_id_;
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(1);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(absl::StrContains(bytes_read.status().message(),
                                "Failed to fetch chunk(s)"));
}

TEST_F(AssetTest, ReadWithBadOffsetFails) {
  uint64_t offset = 0;
  AddChunks({{1, 2, 3}, {4, 5, 6}}, &offset, proto_.mutable_file_chunks());
  proto_.mutable_file_chunks(1)->set_offset(4);  // Instead of 3.
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(6);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(absl::StrContains(bytes_read.status().message(),
                                "requested size 4 at offset 0"));
}

TEST_F(AssetTest, ReadEmptyWithBadFileSize) {
  uint64_t offset = 0;
  AddChunks({}, &offset, proto_.mutable_file_chunks());
  proto_.set_file_size(1);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(1);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_NOT_OK(bytes_read);
  EXPECT_TRUE(absl::StrContains(bytes_read.status().message(),
                                "Invalid chunk ref list"));
}

TEST_F(AssetTest, ReadWithOffsetAndSizeSucceeds) {
  uint64_t offset = 0;
  AddChunks({{0, 1}, {}, {2}}, &offset, proto_.mutable_file_chunks());
  AddIndirectChunks({{3}, {4, 5, 6}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({}, &offset, proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{7, 8, 9}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::FILE);

  asset_.Initialize(kParentIno, &store_, &proto_);

  // Test all kinds of different permutations of offsets and sizes.
  std::vector<char> expected_data;
  for (offset = 0; offset < 12; ++offset) {
    for (uint64_t size = 0; size < 12; ++size) {
      expected_data.clear();
      for (uint64_t n = offset; n < std::min<uint64_t>(offset + size, 10);
           ++n) {
        expected_data.push_back(static_cast<char>(n));
      }

      std::vector<char> data(size);
      absl::StatusOr<uint64_t> bytes_read =
          asset_.Read(offset, data.data(), data.size());

      ASSERT_OK(bytes_read);
      EXPECT_EQ(*bytes_read, expected_data.size());
      data.resize(expected_data.size());
      EXPECT_EQ(data, expected_data);
    }
  }
}

TEST_F(AssetTest, UpdateProtoWithEmptyAssetSucceeds) {
  proto_.set_type(AssetProto::DIRECTORY);
  // Put all children into the direct asset list.
  for (size_t n = 0; n < kNumChildProtos; ++n)
    *proto_.add_dir_assets() = child_protos_[n];
  asset_.Initialize(kParentIno, &store_, &proto_);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();
  ASSERT_OK(protos);
  ASSERT_EQ(protos->size(), kNumChildProtos);

  AssetProto proto_updated;
  proto_updated.set_type(AssetProto::DIRECTORY);
  asset_.UpdateProto(&proto_updated);
  protos = asset_.GetAllChildProtos();
  ASSERT_OK(protos);
  ASSERT_TRUE(protos->empty());
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, UpdateProtoFromEmptyAssetSucceeds) {
  AssetProto empty_proto;
  empty_proto.set_type(AssetProto::DIRECTORY);
  asset_.Initialize(kParentIno, &store_, &empty_proto);
  absl::StatusOr<std::vector<const AssetProto*>> protos =
      asset_.GetAllChildProtos();
  ASSERT_OK(protos);
  ASSERT_TRUE(protos->empty());

  proto_.set_type(AssetProto::DIRECTORY);
  // Put all children into the direct asset list.
  for (size_t n = 0; n < kNumChildProtos; ++n)
    *proto_.add_dir_assets() = child_protos_[n];
  asset_.UpdateProto(&proto_);

  protos = asset_.GetAllChildProtos();
  ASSERT_OK(protos);
  ASSERT_EQ(protos->size(), kNumChildProtos);
  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
}

TEST_F(AssetTest, AssetProtoComparison) {
  AssetProto a;
  AssetProto b;
  EXPECT_EQ(a, b);

  a.set_type(AssetProto::DIRECTORY);
  b.set_type(AssetProto::FILE);
  EXPECT_NE(a, b);

  b.set_type(AssetProto::DIRECTORY);
  EXPECT_EQ(a, b);

  for (size_t n = 0; n < kNumChildProtos; ++n)
    *a.add_dir_assets() = child_protos_[n];
  EXPECT_NE(a, b);

  for (size_t n = 0; n < kNumChildProtos; ++n)
    *b.add_dir_assets() = child_protos_[n];
  EXPECT_EQ(a, b);
}

TEST_F(AssetTest, IsConsistentFailsFileWithDirAssets) {
  // Put all children into the direct asset list.
  for (size_t n = 0; n < kNumChildProtos; ++n)
    *proto_.add_dir_assets() = child_protos_[n];
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "File asset contains sub-assets");
}

TEST_F(AssetTest, IsConsistentFailsFileWithSymlink) {
  proto_.set_symlink_target("symlink");
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "File asset contains a symlink");
}

TEST_F(AssetTest, IsConsistentFailsDirWithFileChunks) {
  uint64_t offset = 0;
  AddChunks({{1, 2}}, &offset, proto_.mutable_file_chunks());
  proto_.set_type(AssetProto::DIRECTORY);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Directory asset contains file chunks");
}

TEST_F(AssetTest, IsConsistentFailsDirWithIndirectFileChunks) {
  uint64_t offset = 0;
  AddIndirectChunks({{3}, {4, 5, 6}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);
  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Directory asset contains file chunks");
}

TEST_F(AssetTest, IsConsistentFailsDirWithSymlink) {
  proto_.set_symlink_target("symlink");
  proto_.set_type(AssetProto::DIRECTORY);

  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Directory asset contains a symlink");
}

TEST_F(AssetTest, IsConsistentFailsDirWithFileSize) {
  proto_.set_file_size(2);
  proto_.set_type(AssetProto::DIRECTORY);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(),
               "File size is defined for a directory asset");
}

TEST_F(AssetTest, IsConsistentFailsSymlinkWithDirAssets) {
  // Put all children into the direct asset list.
  for (size_t n = 0; n < kNumChildProtos; ++n)
    *proto_.add_dir_assets() = child_protos_[n];
  proto_.set_type(AssetProto::SYMLINK);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Symlink asset contains sub-assets");
}

TEST_F(AssetTest, IsConsistentFailsSymlinkWithIndirectFileChunks) {
  uint64_t offset = 0;
  AddIndirectChunks({{3}, {4, 5, 6}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(offset);
  proto_.set_type(AssetProto::SYMLINK);

  asset_.Initialize(kParentIno, &store_, &proto_);
  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Symlink asset contains file chunks");
}

TEST_F(AssetTest, IsConsistentFailsSymlinkWithFileSize) {
  proto_.set_file_size(2);
  proto_.set_type(AssetProto::SYMLINK);

  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(),
               "File size is defined for a symlink asset");
}

TEST_F(AssetTest, IsConsistentFailsUndefinedAssetType) {
  proto_.set_type(AssetProto::UNKNOWN);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(), "Undefined asset type");
}

TEST_F(AssetTest, IsConsistentFailsFileChunkWrongOffsets) {
  uint64_t offset = 10;
  AddChunks({{1}}, &offset, proto_.mutable_file_chunks());
  offset = 5;
  AddChunks({{2}}, &offset, proto_.mutable_file_chunks());
  proto_.set_file_size(2);
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(
      asset_check_.c_str(),
      "Disordered direct chunks: idx=1, total_offset=10, chunk_offset=5");
}

TEST_F(AssetTest, IsConsistentFailsWrongFileSize) {
  uint64_t offset = 0;
  AddChunks({{1}, {2}}, &offset, proto_.mutable_file_chunks());
  AddIndirectChunks({{3}, {4}, {5}, {6}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  AddIndirectChunks({{7}, {8}, {9}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(5);
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  std::vector<char> data(9);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(asset_check_.c_str(),
               "The last absolute file offset exceeds the file size: 8 >= 5");
}

TEST_F(AssetTest, IsConsistentFailsNonZeroFirstIndirectListOffset) {
  uint64_t offset = 10;
  AddIndirectChunks({{1}}, &offset, proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(1);
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(
      asset_check_.c_str(),
      "Disordered indirect chunk list: the list offset should be 0, as there "
      "are no direct file chunks: list_offset=10, previous list_offset=0");
}

TEST_F(AssetTest, IsConsistentFailsNonIncreasingIndirectListOffset) {
  uint64_t offset = 0;
  AddIndirectChunks({{1}, {2}, {3}}, &offset,
                    proto_.mutable_file_indirect_chunks());
  offset = 1;
  AddIndirectChunks({{3}}, &offset, proto_.mutable_file_indirect_chunks());
  proto_.set_file_size(3);
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);

  // Read the first indirect list to fill the internal structure.
  std::vector<char> data(3);
  absl::StatusOr<uint64_t> bytes_read =
      asset_.Read(0, data.data(), data.size());

  ASSERT_OK(bytes_read);
  EXPECT_EQ(*bytes_read, 3);

  EXPECT_FALSE(asset_.IsConsistent(&asset_check_));
  EXPECT_STREQ(
      asset_check_.c_str(),
      "Disordered indirect chunk list: the list offset should increase: "
      "list_offset=1, previous list_offset=0, total_offset=2");
}

TEST_F(AssetTest, IsConsistentEmptyFileSucceeds) {
  proto_.set_type(AssetProto::FILE);
  asset_.Initialize(kParentIno, &store_, &proto_);
  proto_.set_file_size(0);

  EXPECT_TRUE(asset_.IsConsistent(&asset_check_));
  EXPECT_TRUE(asset_check_.empty());
}

}  // namespace
}  // namespace cdc_ft
