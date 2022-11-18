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

#include "manifest/manifest_builder.h"

#include "absl/time/clock.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/util.h"
#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"
#include "manifest/content_id.h"
#include "manifest/manifest_iterator.h"
#include "manifest/manifest_printer.h"

namespace cdc_ft {

namespace {

// Helper function that tries to parse data as any of the protos written to the
// store and returns its text proto representation.
//
// In order to disambiguate the proto auto-detection logic, you can temporarily
// assign globally unique field numbers to all fields in manifest.proto.
std::string ToTextProto(const ContentIdProto& content_id, const void* data,
                        size_t size) {
  std::string text_proto;
  std::string proto_name = "(unknown proto format)";

  ManifestProto manifest_pb;
  AssetListProto asset_list_pb;
  ChunkListProto chunk_list_pb;
  int isize = static_cast<int>(size);

  ManifestPrinter printer;

  if (manifest_pb.ParseFromArray(data, isize) &&
      !manifest_pb.GetReflection()
           ->GetUnknownFields(manifest_pb)
           .field_count()) {
    printer.PrintToString(manifest_pb, &text_proto);
    proto_name = manifest_pb.GetTypeName();
  } else if (asset_list_pb.ParseFromArray(data, isize) &&
             !asset_list_pb.GetReflection()
                  ->GetUnknownFields(asset_list_pb)
                  .field_count()) {
    printer.PrintToString(asset_list_pb, &text_proto);
    proto_name = asset_list_pb.GetTypeName();
  } else if (chunk_list_pb.ParseFromArray(data, isize) &&
             !chunk_list_pb.GetReflection()
                  ->GetUnknownFields(chunk_list_pb)
                  .field_count()) {
    printer.PrintToString(chunk_list_pb, &text_proto);
    proto_name = chunk_list_pb.GetTypeName();
  }
  return absl::StrFormat("# %s => %s (size: %d)\n%s",
                         ContentId::ToHexString(content_id), proto_name, isize,
                         text_proto);
}

class ManifestBuilderTest : public ::testing::Test {
 public:
  ManifestBuilderTest() = default;

  void SetUp() override {
    ASSERT_OK(cache_.Wipe());
    content_ids_.clear();
    expected_assets_.clear();
    // 4KiB
    cdc_params_.set_avg_chunk_size(4u << 10);
  }
  void TearDown() override {}

 protected:
  // A list of strings.
  using StringList = std::vector<std::string>;
  // A set of strings.
  using StringSet = std::unordered_set<std::string>;
  using TypeStringsMap = std::unordered_map<AssetProto::Type, StringSet>;
  // Maps a file path to a list of chunks representing the file contents.
  using AssetMap = std::unordered_map<std::string, StringList>;
  // Maps a chunk by its content to the content ID.
  using ContentIdMap = std::unordered_map<std::string, ContentIdProto>;

  // Identifes a symlink if this is the first chunk of a file.
  static constexpr char kSymlinkPrefix[] = "->";

  // The size of adding one reference to an IndirectAssetList to a DIRECTORY
  // asset.
  static constexpr size_t kIndirectAssetListRefSize = 24;

  // The overhead for adding a variable-size field (like another proto) to a
  // proto message.
  static constexpr size_t kProtoFieldOverhead = 2;

  // Returns the maximum proto size that we expect to encounter. This is
  // slightly higher than the avg_chunk_size due to some overhead bytes which
  // are not accounted for, for the sake of simplicity:
  // - Since the size is only enforced on the directory level and the manifest
  // root node includes the root directory asset and the CdcParamsProto, the
  // latter can cause the limit to be slightly exceeded.
  // - When the first asset is pushed to an indirect asset list, the
  // indirectlist reference also creates an overhead that is not accounted for.
  size_t ActualMaxProtoSize() const {
    return cdc_params_.avg_chunk_size() + kProtoFieldOverhead +
           cdc_params_.ByteSizeLong() + kIndirectAssetListRefSize;
  }

  // Adds a list of pseudo assets to the |builder|. The AssetMap maps file names
  // to a list of chunks (just strings). If the chunk list is empty, it's
  // considered to be a directory, otherwise a file. To represent an empty file,
  // add a single empty string as a chunk.
  absl::Status AddAssets(const AssetMap& assets, ManifestBuilder* builder) {
    for (auto [path, chunks] : assets) {
      AssetProto::Type type = GetAssetType(chunks);
      AssetBuilder ab;
      // Add the asset.
      ASSIGN_OR_RETURN(ab, builder->GetOrCreateAsset(path, type));
      switch (type) {
        case AssetProto::DIRECTORY:
          break;
        case AssetProto::FILE:
          // Add the chunks, if any.
          for (std::string chunk : chunks) {
            if (chunk.empty()) continue;
            ContentIdProto content_id = ContentId::FromDataString(chunk);
            ab.AppendChunk(content_id, chunk.size());
            content_ids_[chunk] = content_id;
          }
          break;
        case AssetProto::SYMLINK:
          ab.SetSymlinkTarget(chunks.back());
          break;
        default:
          return absl::InternalError(absl::StrFormat(
              "Unhandled asset type: %s", AssetProto::Type_Name(type)));
      }

      // Add the asset to the set of expected assets.
      std::string unix_path = path::ToUnix(path);
      expected_assets_[type].insert(unix_path);
      // Add all directories leading up to this asset.
      std::vector<absl::string_view> parts = SplitString(unix_path, '/', false);
      for (size_t i = 1; i < parts.size(); ++i) {
        expected_assets_[AssetProto::DIRECTORY].insert(
            JoinStrings(parts, 0, i, '/'));
      }
    }
    return absl::OkStatus();
  }

  // Verifies that the manifest identified by |manifest_id| correctly and
  // completely describes the given |assets| map. In particular, it verifies
  // that:
  // - all assets and intermediate directories exist
  // - no extra assets or directories exist
  // - FILE assets have the correct chunks w/ offsets
  // - FILE assets have the correct size
  // - SYMLINK assets point to the right target
  void VerifyAssets(const AssetMap& assets, const ContentIdProto& manifest_id) {
    std::unordered_map<AssetProto::Type, StringSet> found;

    // Verify the manifest by iterating over it.
    ManifestIterator iter(&cache_);
    ASSERT_OK(iter.Open(manifest_id));

    const AssetProto* asset;
    while ((asset = iter.NextEntry()) != nullptr) {
      std::string assetPath =
          path::JoinUnix(path::ToUnix(iter.RelativePath()), asset->name());

      // Evaluate expected file size.
      StringList chunks;
      if (auto it = assets.find(assetPath); it != assets.end()) {
        chunks = it->second;
      } else {
        // For windows paths, we need to search through all assets.
        for (auto [name, asset_chunks] : assets) {
          if (path::ToUnix(name) == assetPath) {
            chunks = asset_chunks;
            break;
          }
        }
      }
      size_t file_size = GetFileSize(chunks);

      AssetProto::Type type = GetAssetType(chunks);
      EXPECT_EQ(type, asset->type());
      found[type].insert(assetPath);

      // Verify type specific proto fields.
      switch (type) {
        case AssetProto::DIRECTORY:
          break;
        case AssetProto::FILE:
          EXPECT_EQ(asset->file_size(), file_size);
          VerifyChunks(chunks, asset);
          break;
        case AssetProto::SYMLINK:
          EXPECT_EQ(chunks.size(), 2);
          EXPECT_EQ(asset->symlink_target(), chunks.back());
          break;
        default:
          ASSERT_TRUE(false)
              << "Unexpected asset type: " << AssetProto::Type_Name(type);
      }
    }

    // Make sure there was no error processing the manifest.
    EXPECT_OK(iter.Status());

    // Make sure all expected assets were found.
    EXPECT_EQ(found, expected_assets_);
  }

  // Verifies that the given |file| proto consists of the chunks given as
  // |chunks|.
  void VerifyChunks(const StringList& expected_chunks, const AssetProto* file) {
    uint64_t offset = 0;
    // Verify the direct chunks.
    VerifyChunkList(expected_chunks, file->file_chunks(), 0, &offset);
    // Verify all indirect chunks.
    int chunk_index = file->file_chunks_size();
    for (const IndirectChunkListProto& indirect_chunks :
         file->file_indirect_chunks()) {
      EXPECT_EQ(offset, indirect_chunks.offset());
      ASSERT_GT(indirect_chunks.chunk_list_id().blake3_sum_160().size(), 0);
      ChunkListProto chunks;
      ASSERT_OK(cache_.GetProto(indirect_chunks.chunk_list_id(), &chunks));
      VerifyChunkList(expected_chunks, chunks.chunks(), chunk_index, &offset);
      chunk_index += chunks.chunks_size();
    }
    // Make sure we found all the chunks.
    if (file->file_size() > 0) EXPECT_EQ(expected_chunks.size(), chunk_index);
  }

  void VerifyChunkList(const StringList& expected_chunks,
                       const RepeatedChunkRefProto& actual_chunks,
                       int chunk_index, size_t* offset) {
    ASSERT_LE(chunk_index + actual_chunks.size(), expected_chunks.size());
    uint64_t relative_offset = 0;
    for (const ChunkRefProto& chunk_ref : actual_chunks) {
      const std::string& chunk = expected_chunks[chunk_index++];
      ContentIdProto expected_id = content_ids_.at(chunk);
      ASSERT_GT(expected_id.blake3_sum_160().size(), 0);
      ASSERT_EQ(expected_id, chunk_ref.chunk_id());
      EXPECT_EQ(relative_offset, chunk_ref.offset());
      relative_offset += chunk.size();
    }
    *offset += relative_offset;
  }

  AssetProto::Type GetAssetType(const StringList& chunks) const {
    // Asset with an empty chunk list: DIRECTORY
    if (chunks.empty()) return AssetProto::DIRECTORY;
    // Asset with a single chunk starting with kSymlinkPrefix: SYMLINK
    if (chunks.size() == 2 && chunks[0] == kSymlinkPrefix) {
      return AssetProto::SYMLINK;
    }
    // Otherwise: FILE
    return AssetProto::FILE;
  }

  size_t GetFileSize(const StringList& chunks) const {
    size_t file_size = 0;
    for (const std::string& chunk : chunks) {
      file_size += chunk.size();
    }
    return file_size;
  }

  // Iterates over the manifest identifed by |manifest_id| and returns the list
  // of assets. Does not return the relative asset path.
  std::vector<AssetProto> GetAllAssets(const ContentIdProto& manifest_id) {
    // Verify the manifest by iterating over it.
    ManifestIterator iter(&cache_);
    EXPECT_OK(iter.Open(manifest_id));

    std::vector<AssetProto> assets;
    const AssetProto* asset;
    while ((asset = iter.NextEntry()) != nullptr) {
      assets.push_back(*asset);
    }
    EXPECT_OK(iter.Status());
    return assets;
  }

  // Default CDC parameters.
  CdcParamsProto cdc_params_;
  // Maps chunks back to content IDs.
  ContentIdMap content_ids_;
  // List of expected assets.
  TypeStringsMap expected_assets_;
  MemDataStore cache_;
};

TEST_F(ManifestBuilderTest, SimpleFiles) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["file2.txt"] = {"c", "d"};
  assets["file3.txt"] = {"e"};
  assets["file4.txt"] = {""};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, FilesDirsCreatedOnlyOnce) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a"};
  assets["dir1"] = {};

  bool created = false;
  EXPECT_OK(
      builder.GetOrCreateAsset("file1.txt", AssetProto::FILE, false, &created));
  EXPECT_TRUE(created);
  EXPECT_OK(
      builder.GetOrCreateAsset("file1.txt", AssetProto::FILE, false, &created));
  EXPECT_FALSE(created);

  EXPECT_OK(
      builder.GetOrCreateAsset("dir1", AssetProto::DIRECTORY, false, &created));
  EXPECT_TRUE(created);
  EXPECT_OK(
      builder.GetOrCreateAsset("dir1", AssetProto::DIRECTORY, false, &created));
  EXPECT_FALSE(created);

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, GetAssetsOfUnkonwnType) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a"};
  assets["dir1"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  bool created = false;

  // Get existing assets, force_create == false
  EXPECT_OK(builder.GetOrCreateAsset("file1.txt", AssetProto::UNKNOWN, false,
                                     &created));
  EXPECT_FALSE(created);
  EXPECT_OK(
      builder.GetOrCreateAsset("dir1", AssetProto::UNKNOWN, false, &created));
  EXPECT_FALSE(created);

  // Get existing assets, force_create == true
  EXPECT_OK(builder.GetOrCreateAsset("file1.txt", AssetProto::UNKNOWN, true,
                                     &created));
  EXPECT_FALSE(created);
  EXPECT_OK(
      builder.GetOrCreateAsset("dir1", AssetProto::UNKNOWN, true, &created));
  EXPECT_FALSE(created);

  // Get the root directory.
  EXPECT_OK(builder.GetOrCreateAsset("", AssetProto::UNKNOWN));

  // Get non-existing file fails, force_create = false
  EXPECT_NOT_OK(
      builder.GetOrCreateAsset("does_not_exist", AssetProto::UNKNOWN, false));

  // Get non-existing file fails, force_create = true
  EXPECT_NOT_OK(
      builder.GetOrCreateAsset("does_not_exist", AssetProto::UNKNOWN, true));

  // Get non-existing file fails, no sub-directories are created.
  EXPECT_NOT_OK(builder.GetOrCreateAsset("new_dir1/does_not_exist",
                                         AssetProto::UNKNOWN, false));
  EXPECT_NOT_OK(builder.GetOrCreateAsset("new_dir2/does_not_exist",
                                         AssetProto::UNKNOWN, true));

  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, Deduplication) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a"};
  assets["file2.txt"] = {"a", "a"};
  assets["file3.txt"] = {"a", "a", "a"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, TruncateChunks) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["file2.txt"] = {"c", "d"};
  assets["file3.txt"] = {""};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  absl::StatusOr<AssetBuilder> file2 =
      builder.GetOrCreateAsset("file2.txt", AssetProto::FILE);
  ASSERT_OK(file2);
  file2->TruncateChunks();
  ASSERT_OK(builder.Flush());
  assets["file2.txt"] = {""};
  VerifyAssets(assets, builder.ManifestId());

  absl::StatusOr<AssetBuilder> file3 =
      builder.GetOrCreateAsset("file3.txt", AssetProto::FILE);
  ASSERT_OK(file3);
  file3->TruncateChunks();
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, SimpleDirs) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir1"] = {};
  assets["dir2"] = {};
  assets["dir2/dir3"] = {};
  assets["dir2/dir3/dir4"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, DirsAreCreated) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["dir1/file1.txt"] = {"c", "d"};
  assets["dir1/file2.txt"] = {""};
  assets["dir1/dir2/file3.txt"] = {"e"};
  assets["dir3/dir4/dir5"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, DirsWithWindowsPathSep) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir1\\file1.txt"] = {"a", "b"};
  assets["dir1\\file2.txt"] = {""};
  assets["dir1\\dir2\\file3.txt"] = {"c"};
  assets["dir3\\dir4\\dir5"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, SomeDirsAreCreated) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir1"] = {};
  assets["dir1/file1.txt"] = {"a"};
  assets["dir1/dir2/file2.txt"] = {"b"};
  assets["dir1/dir2/dir3"] = {};
  assets["dir4/dir5/dir6"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, Symlinks) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir1"] = {};
  assets["dir1/file1.txt"] = {"a"};
  assets["file2.txt"] = {"b"};
  assets["link1"] = {kSymlinkPrefix, "dir1"};
  assets["link2"] = {kSymlinkPrefix, "dir1/file1.txt"};
  assets["link3"] = {kSymlinkPrefix, "file2.txt"};
  assets["link4"] = {kSymlinkPrefix, "does_not_exist"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, Spaces) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir 1"] = {};
  assets["dir 1/file 1.txt"] = {"a"};
  assets["this is file2.txt"] = {"b"};
  assets["link with spaces"] = {kSymlinkPrefix, "dir 1/file 1.txt"};
  assets["a path/to some/file with spaces.txt"] = {"c", "d"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, FilePermissions) {
  ManifestBuilder builder(cdc_params_, &cache_);
  std::unordered_map<std::string, uint32_t> expected_perms;
  expected_perms["file_0777.txt"] = 0777u;
  expected_perms["file_0755.txt"] = 0755u;
  expected_perms["file_0644.txt"] = 0644u;
  expected_perms["file_0664.txt"] = 0664u;
  expected_perms["file_0444.txt"] = 0444u;
  expected_perms["file_0640.txt"] = 0640u;

  for (auto [name, perms] : expected_perms) {
    absl::StatusOr<AssetBuilder> ab =
        builder.GetOrCreateAsset(name, AssetProto::FILE);
    ASSERT_OK(ab);
    ab->SetPermissions(perms);
  }
  ASSERT_OK(builder.Flush());
  std::vector<AssetProto> assets = GetAllAssets(builder.ManifestId());
  EXPECT_EQ(assets.size(), expected_perms.size());
  for (const AssetProto& asset : assets) {
    EXPECT_EQ(asset.permissions(), expected_perms[asset.name()]);
  }
}

TEST_F(ManifestBuilderTest, DirPermissions) {
  ManifestBuilder builder(cdc_params_, &cache_);
  std::unordered_map<std::string, uint32_t> expected_perms;
  expected_perms["dir_0777"] = 0777u;
  expected_perms["dir_0755"] = 0755u;
  expected_perms["dir_0750"] = 0750u;
  expected_perms["dir_0700"] = 0700u;

  for (auto [name, perms] : expected_perms) {
    absl::StatusOr<AssetBuilder> ab =
        builder.GetOrCreateAsset(name, AssetProto::DIRECTORY);
    ASSERT_OK(ab);
    ab->SetPermissions(perms);
  }
  ASSERT_OK(builder.Flush());
  std::vector<AssetProto> assets = GetAllAssets(builder.ManifestId());
  EXPECT_EQ(assets.size(), expected_perms.size());
  for (const AssetProto& asset : assets) {
    EXPECT_EQ(asset.permissions(), expected_perms[asset.name()]);
  }
}

TEST_F(ManifestBuilderTest, DefaultPermissions) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["dir1"] = {};
  assets["dir1/file1.txt"] = {"a"};
  assets["file2.txt"] = {"b"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());

  std::vector<AssetProto> asset_pbs = GetAllAssets(builder.ManifestId());
  for (const AssetProto& asset : asset_pbs) {
    if (asset.type() == AssetProto::DIRECTORY) {
      EXPECT_EQ(asset.permissions(), ManifestBuilder::kDefaultDirPerms);
    } else {
      EXPECT_EQ(asset.permissions(), ManifestBuilder::kDefaultFilePerms);
    }
  }
}

TEST_F(ManifestBuilderTest, FileMTime) {
  ManifestBuilder builder(cdc_params_, &cache_);
  std::unordered_map<std::string, int64_t> expected_mtime;
  expected_mtime["file1.txt"] = 0;
  expected_mtime["file2.txt"] = 42;
  expected_mtime["file3.txt"] = absl::ToUnixSeconds(absl::Now());
  // A negative time value 42 days before the Unix epoch.
  expected_mtime["file4.txt"] =
      absl::ToUnixSeconds(absl::UnixEpoch() - absl::Hours(24 * 42));

  for (auto [name, mtime] : expected_mtime) {
    absl::StatusOr<AssetBuilder> ab =
        builder.GetOrCreateAsset(name, AssetProto::FILE);
    ASSERT_OK(ab);
    ab->SetMtimeSeconds(mtime);
  }
  ASSERT_OK(builder.Flush());
  std::vector<AssetProto> assets = GetAllAssets(builder.ManifestId());
  EXPECT_EQ(assets.size(), expected_mtime.size());
  for (const AssetProto& asset : assets) {
    EXPECT_EQ(asset.mtime_seconds(), expected_mtime[asset.name()]);
  }
}

TEST_F(ManifestBuilderTest, DirMTime) {
  ManifestBuilder builder(cdc_params_, &cache_);
  std::unordered_map<std::string, int64_t> expected_mtime;
  expected_mtime["dir1"] = 0;
  expected_mtime["dir2"] = 42;
  expected_mtime["dir3"] = absl::ToUnixSeconds(absl::Now());
  // A negative time value 42 days before the Unix epoch.
  expected_mtime["dir4"] =
      absl::ToUnixSeconds(absl::UnixEpoch() - absl::Hours(24 * 42));

  // Auto-create "dir2" first with a file to see if the mtime is correctly
  // overridden.
  absl::StatusOr<AssetBuilder> ab =
      builder.GetOrCreateAsset("dir1/file1.txt", AssetProto::FILE);
  ASSERT_OK(ab);

  for (auto [name, mtime] : expected_mtime) {
    ab = builder.GetOrCreateAsset(name, AssetProto::DIRECTORY);
    ASSERT_OK(ab);
    ab->SetMtimeSeconds(mtime);
  }
  ASSERT_OK(builder.Flush());
  std::vector<AssetProto> assets = GetAllAssets(builder.ManifestId());
  EXPECT_EQ(assets.size(), expected_mtime.size() + 1);  // +1 file
  for (const AssetProto& asset : assets) {
    if (asset.type() == AssetProto::FILE) {
      // The file's timestamp must be current.
      EXPECT_LE(asset.mtime_seconds(), absl::ToUnixSeconds(absl::Now()));
      EXPECT_GE(asset.mtime_seconds() + 60, absl::ToUnixSeconds(absl::Now()));
    } else {
      EXPECT_EQ(asset.mtime_seconds(), expected_mtime[asset.name()])
          << "Asset: " << asset.name();
    }
  }
}

TEST_F(ManifestBuilderTest, IndirectAssets) {
  // A proto with just the type and name set is serialized to 4 + sizeof(name)
  // bytes.
  //
  // FILE assets:
  // - Each ChunkRef proto asset with a small offset (<128) takes up 28 bytes.
  // - With 0 chunks:  4 + sizeof(name)
  // - With 1 chunk:  32 + sizeof(name)
  // - With 2 chunks: 60 + sizeof(name)
  //
  // DIRECTORY assets:
  // - Each IndirectChunkList proto asset with a small offset (<128) takes up 28
  //   bytes (same as for a ChunkRef in a file).
  //
  // ChunkList protos:
  // - Again, each ChunkRef proto adds 28 bytes.
  //
  // AssetList proto:
  // - Each Asset in the list has an overhead of 2 additional bytes.
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // 61 bytes each, so one AssetList of 128 bytes can fit 2 assets.
  assets["a"] = {"a", "a"};
  assets["b"] = {"b", "b"};
  assets["c"] = {"c", "c"};
  assets["d"] = {"d", "d"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto, 1 AssetList proto, 4 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 6);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());
  // Verify that the chunk size is not exeeded.
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }
}

TEST_F(ManifestBuilderTest, IndirectChunks) {
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each chunk adds 28 bytes so one IndirectChunkList of 128 bytes can fit 4
  // chunks (= 112 bytes).
  assets["a"] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto and 3 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 4);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());
  // Verify that the chunk size is not exeeded.
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }
}

TEST_F(ManifestBuilderTest, IndirectChunksFlushedTwice) {
  cdc_params_.set_avg_chunk_size(256);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets, more_assets;
  // Each chunk adds 28 bytes so one IndirectChunkList of 256 bytes can fit 9
  // chunks (= 252 bytes). This asset will still be considered large even after
  // it was flushed.
  assets["a"] = {"a", "b", "c", "d", "e", "f", "g", "h"};
  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());

  // Add more FILEs to the root directory such that it exceeds the chunk size
  // limit. The next Flush() call will also try to push chunks for FILE "a" to
  // indirect lists again, which must be ignored.
  more_assets["b"] = {"i"};
  more_assets["c"] = {"i"};
  more_assets["d"] = {"i"};
  more_assets["e"] = {"i"};
  more_assets["f"] = {"i"};
  ASSERT_OK(AddAssets(more_assets, &builder));
  ASSERT_OK(builder.Flush());

  assets.insert(more_assets.begin(), more_assets.end());
  VerifyAssets(assets, builder.ManifestId());

  // Expecting 2 manifest protos (old and new one), 1 ChunkList proto, and 1
  // AssetList proto.
  EXPECT_EQ(cache_.Chunks().size(), 4);
  // 1 new manifest, 1 new ChunkList, 1 updated AssetList.
  EXPECT_EQ(builder.FlushedContentIds().size(), 3);
  // Verify that the chunk size is not exeeded.
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }
}

TEST_F(ManifestBuilderTest, IndirectChunksAndAssets) {
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each chunk adds 28 bytes so one IndirectChunkList of 128 bytes can fit 4
  // chunks (= 112 bytes).
  assets["a"] = {"a", "b", "c", "d"};
  assets["b"] = {"b", "c", "d", "e"};
  assets["c"] = {"c", "d", "e", "f"};
  assets["d"] = {"d", "e", "f", "g"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 Manifest proto, 1 AssetList proto, 4 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 6);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());
  // Verify that the chunk size is not exeeded.
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }
}

TEST_F(ManifestBuilderTest, DirectAndIndirectChunks) {
  // In order to have both direct and indirect chunks, we need a minimum "large
  // asset" size of 64 bytes, which translates to a chunk size of 64 << 4 = 1024
  // bytes.
  cdc_params_.set_avg_chunk_size(1024);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each chunk adds 30 bytes each, so one IndirectChunkList of
  // 1024 bytes can fit 34 chunks (= 1020 bytes).
  assets["a"] = {"00", "01", "02", "03", "04", "05", "06", "07", "08",
                 "09", "10", "11", "12", "13", "14", "15", "16", "17",
                 "18", "19", "20", "21", "22", "23", "24", "25", "26",
                 "27", "28", "29", "30", "31", "32", "33", "34", "35"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto and 1 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 2);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());
  // Verify that the chunk size is not exeeded.
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }
}

TEST_F(ManifestBuilderTest, SetChunksWithIndirectChunks) {
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each chunk adds 28 bytes so one IndirectChunkList of 128 bytes can fit 4
  // chunks (= 112 bytes).
  assets["a"] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto and 3 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 4);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());

  // Create a new chunk list consisting of a single chunk.
  RepeatedChunkRefProto chunks;
  ChunkRefProto* chunk = chunks.Add();
  assets["a"] = {"new chunk data"};
  *chunk->mutable_chunk_id() = ContentId::FromDataString(assets["a"][0]);
  content_ids_[assets["a"][0]] = chunk->chunk_id();

  // Set the single chunk list.
  auto asset = builder.GetOrCreateAsset("a", AssetProto::FILE);
  ASSERT_OK(asset);
  asset->SetChunks(chunks, assets["a"][0].size());
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Only the manifest was updated.
  EXPECT_EQ(builder.FlushedContentIds().size(), 1);

  // Set to an empty chunk list.
  asset = builder.GetOrCreateAsset("a", AssetProto::FILE);
  ASSERT_OK(asset);
  asset->SetChunks(RepeatedChunkRefProto(), 0);
  assets["a"] = {""};
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Only the manifest was updated.
  EXPECT_EQ(builder.FlushedContentIds().size(), 1);
}

TEST_F(ManifestBuilderTest, SwapChunksWithIndirectChunks) {
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each chunk adds 28 bytes so one IndirectChunkList of 128 bytes can fit 4
  // chunks (= 112 bytes).
  assets["a"] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto and 3 ChunkList protos.
  EXPECT_EQ(cache_.Chunks().size(), 4);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());

  // Create a new chunk list consisting of a single chunk.
  RepeatedChunkRefProto chunks;
  ChunkRefProto* chunk = chunks.Add();
  assets["a"] = {"new chunk data"};
  *chunk->mutable_chunk_id() = ContentId::FromDataString(assets["a"][0]);
  content_ids_[assets["a"][0]] = chunk->chunk_id();

  // Swap with the single chunk list.
  auto asset = builder.GetOrCreateAsset("a", AssetProto::FILE);
  ASSERT_OK(asset);
  asset->SwapChunks(&chunks, assets["a"][0].size());
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Only the manifest changed.
  EXPECT_EQ(builder.FlushedContentIds().size(), 1);

  // Swap with an empty chunk list.
  chunks.Clear();
  asset = builder.GetOrCreateAsset("a", AssetProto::FILE);
  ASSERT_OK(asset);
  asset->SwapChunks(&chunks, 0);
  assets["a"] = {""};
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Only the manifest changed.
  EXPECT_EQ(builder.FlushedContentIds().size(), 1);
}

TEST_F(ManifestBuilderTest, DeleteDirectAssets) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["dir1/file1.txt"] = {"c", "d"};
  assets["dir1/file2.txt"] = {""};
  assets["dir1/dir2/file3.txt"] = {"e"};
  assets["dir3/dir4/dir5"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  EXPECT_OK(builder.DeleteAsset("file1.txt"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::FILE].erase("file1.txt");
  VerifyAssets(assets, builder.ManifestId());

  EXPECT_OK(builder.DeleteAsset("dir1/file2.txt"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::FILE].erase("dir1/file2.txt");
  VerifyAssets(assets, builder.ManifestId());

  EXPECT_OK(builder.DeleteAsset("dir1/dir2"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::DIRECTORY].erase("dir1/dir2");
  expected_assets_[AssetProto::FILE].erase("dir1/dir2/file3.txt");
  VerifyAssets(assets, builder.ManifestId());

  EXPECT_OK(builder.DeleteAsset("dir3"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::DIRECTORY].erase("dir3");
  expected_assets_[AssetProto::DIRECTORY].erase("dir3/dir4");
  expected_assets_[AssetProto::DIRECTORY].erase("dir3/dir4/dir5");
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, DeleteIndirectAssets) {
  // A proto with just the type and name set is serialized to 4 + sizeof(name)
  // bytes.
  //
  // FILE assets:
  // - Each ChunkRef proto asset with a small offset (<128) takes up 28 bytes.
  // - With 0 chunks:  4 + sizeof(name)
  //
  // DIRECTORY assets:
  // - Each IndirectChunkList proto asset with a small offset (<128) takes up 28
  //   bytes (same as for a ChunkRef in a file).
  //
  // AssetList proto:
  // - Each Asset in the list has an overhead of 2 additional bytes.
  cdc_params_.set_avg_chunk_size(64);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  // Each empty file weighs 17 bytes in the list, so one AssetList of 64 bytes
  // can fit up to 3 FILE assets.
  assets["f1"] = {""};
  assets["d1/f2"] = {""};
  assets["d1/f3"] = {""};
  assets["d1/f4"] = {""};
  assets["d1/d2/f5"] = {""};
  assets["d1/d2/f6"] = {""};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
  // Expecting 1 manifest proto, 5 AssetList protos.
  EXPECT_EQ(cache_.Chunks().size(), 5);
  EXPECT_EQ(builder.FlushedContentIds().size(), cache_.Chunks().size());
  size_t max_proto_size = ActualMaxProtoSize();
  for (const auto& [content_id, chunk] : cache_.Chunks()) {
    EXPECT_LE(chunk.size(), max_proto_size)
        << ToTextProto(content_id, chunk.data(), chunk.size());
  }

  EXPECT_OK(builder.DeleteAsset("f1"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::FILE].erase("f1");
  VerifyAssets(assets, builder.ManifestId());
  // 1 manifest + at least 1 AssetList
  EXPECT_GT(builder.FlushedContentIds().size(), 1);

  EXPECT_OK(builder.DeleteAsset("d1/f3"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::FILE].erase("d1/f3");
  VerifyAssets(assets, builder.ManifestId());
  // 1 manifest + at least 2 AssetLists
  EXPECT_GT(builder.FlushedContentIds().size(), 2);

  EXPECT_OK(builder.DeleteAsset("d1/d2"));
  ASSERT_OK(builder.Flush());
  expected_assets_[AssetProto::DIRECTORY].erase("d1/d2");
  expected_assets_[AssetProto::FILE].erase("d1/d2/f5");
  expected_assets_[AssetProto::FILE].erase("d1/d2/f6");
  VerifyAssets(assets, builder.ManifestId());
  // 1 manifest + at least 2 AssetLists
  EXPECT_GT(builder.FlushedContentIds().size(), 2);
}

TEST_F(ManifestBuilderTest, LoadAndUpdateManifest) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["dir1/file1.txt"] = {"c", "d"};
  assets["dir1/file2.txt"] = {""};
  assets["dir1/dir2/file3.txt"] = {"e"};
  assets["dir3/dir4/dir5"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  ManifestBuilder builder2(cdc_params_, &cache_);
  ASSERT_OK(builder2.LoadManifest(builder.ManifestId()));

  EXPECT_OK(builder2.DeleteAsset("file1.txt"));
  EXPECT_OK(builder2.DeleteAsset("dir1/dir2"));
  ASSERT_OK(builder2.Flush());
  expected_assets_[AssetProto::FILE].erase("file1.txt");
  expected_assets_[AssetProto::DIRECTORY].erase("dir1/dir2");
  expected_assets_[AssetProto::FILE].erase("dir1/dir2/file3.txt");
  VerifyAssets(assets, builder2.ManifestId());
}

TEST_F(ManifestBuilderTest, LoadAndUpdateManifestFromString) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["file1.txt"] = {"a", "b"};
  assets["dir1/file1.txt"] = {"c", "d"};
  assets["dir1/file2.txt"] = {""};
  assets["dir1/dir2/file3.txt"] = {"e"};
  assets["dir3/dir4/dir5"] = {};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  ManifestBuilder builder2(cdc_params_, &cache_);
  ASSERT_OK(
      builder2.LoadManifest(ContentId::ToHexString(builder.ManifestId())));

  EXPECT_OK(builder2.DeleteAsset("file1.txt"));
  EXPECT_OK(builder2.DeleteAsset("dir1/dir2"));
  ASSERT_OK(builder2.Flush());
  expected_assets_[AssetProto::FILE].erase("file1.txt");
  expected_assets_[AssetProto::DIRECTORY].erase("dir1/dir2");
  expected_assets_[AssetProto::FILE].erase("dir1/dir2/file3.txt");
  VerifyAssets(assets, builder2.ManifestId());
}

TEST_F(ManifestBuilderTest, ForceCreateDirectory) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["a"] = {"a", "b"};
  assets["c/d"] = {"c", "d"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  // This is supposed to fail since "a" is a file, not a directory.
  auto asset = builder.GetOrCreateAsset("a/b", AssetProto::FILE, false);
  EXPECT_NOT_OK(asset);
  // This is supposed to fail since "c/d" is a file, not a directory.
  asset = builder.GetOrCreateAsset("c/d/e", AssetProto::FILE, false);
  EXPECT_NOT_OK(asset);

  // Force-create a new file, turning "a" into a directory.
  asset = builder.GetOrCreateAsset("a/b", AssetProto::FILE, true);
  EXPECT_OK(asset);
  ASSERT_OK(builder.Flush());
  assets["a"] = {};
  assets["a/b"] = {""};
  expected_assets_[AssetProto::FILE].erase("a");
  expected_assets_[AssetProto::DIRECTORY].insert("a");
  expected_assets_[AssetProto::FILE].insert("a/b");
  VerifyAssets(assets, builder.ManifestId());

  // Force-create a new file, turning "c/d" into a directory.
  asset = builder.GetOrCreateAsset("c/d/e", AssetProto::FILE, true);
  EXPECT_OK(asset);
  ASSERT_OK(builder.Flush());
  assets["c/d"] = {};
  assets["c/d/e"] = {""};
  expected_assets_[AssetProto::FILE].erase("c/d");
  expected_assets_[AssetProto::DIRECTORY].insert("c/d");
  expected_assets_[AssetProto::FILE].insert("c/d/e");
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, ForceCreateFile) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["a/b"] = {"a", "b"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  // Force-create a new file, deleting "a/b" and turning "a" into a file.
  auto asset = builder.GetOrCreateAsset("a", AssetProto::FILE, true);
  EXPECT_OK(asset);
  ASSERT_OK(builder.Flush());
  assets.clear();
  assets["a"] = {""};
  expected_assets_.clear();
  expected_assets_[AssetProto::FILE].insert("a");
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, ForceCreateFileInSubdir) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets["a/b/c"] = {"a", "b"};
  assets["a/b/d"] = {"c", "d"};
  assets["a/e"] = {"c", "d"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());

  // Force-create a new file, deleting a/b/* and turning "a/b" into a file.
  auto asset = builder.GetOrCreateAsset("a/b", AssetProto::FILE, true);
  EXPECT_OK(asset);
  ASSERT_OK(builder.Flush());
  assets.clear();
  assets["a/b"] = {""};
  assets["a/e"] = {"c", "d"};
  expected_assets_[AssetProto::FILE].erase("a/b/c");
  expected_assets_[AssetProto::FILE].erase("a/b/d");
  expected_assets_[AssetProto::DIRECTORY].erase("a/b");
  expected_assets_[AssetProto::FILE].insert("a/b");
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, GetRootDirectory) {
  ManifestBuilder builder(cdc_params_, &cache_);

  auto root_dir = builder.GetOrCreateAsset("", AssetProto::DIRECTORY);
  ASSERT_OK(root_dir);
  // Make sure this is the root directory asset.
  EXPECT_TRUE(root_dir->Name().empty());
  EXPECT_EQ(root_dir->Type(), AssetProto::DIRECTORY);
  EXPECT_EQ(root_dir->Proto()->dir_assets_size(), 0);
  ASSERT_OK(builder.Flush());
  VerifyAssets(AssetMap(), builder.ManifestId());
}

TEST_F(ManifestBuilderTest, Unicode) {
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;
  assets[u8"dir ❤️"] = {};
  assets[u8"dir ❤️/file 💩.txt"] = {"a", "b"};
  assets[u8"dir ❤️/dir 😀/file 😉.txt"] = {"c", "d"};
  assets[u8"❤️/💩/😀"] = {"e", "f"};
  assets[u8"link ➡️"] = {kSymlinkPrefix, u8"dir ❤️/dir 😀"};

  ASSERT_OK(AddAssets(assets, &builder));
  ASSERT_OK(builder.Flush());
  VerifyAssets(assets, builder.ManifestId());
}

TEST_F(ManifestBuilderTest, GetManifest) {
  ManifestBuilder builder(cdc_params_, &cache_);
  EXPECT_EQ(builder.Manifest()->cdc_params().SerializeAsString(),
            cdc_params_.SerializeAsString());
}

TEST_F(ManifestBuilderTest, CreateFileLookup) {
  cdc_params_.set_avg_chunk_size(128);
  ManifestBuilder builder(cdc_params_, &cache_);
  AssetMap assets;

  assets["a"] = {"a", "a"};
  assets["b"] = {"b", "b"};
  assets["dir/aa"] = {"aa", "aa"};
  assets["dir/bb"] = {"bb", "bb"};

  ASSERT_OK(AddAssets(assets, &builder));

  ManifestBuilder::FileLookupMap lookup = builder.CreateFileLookup();
  ASSERT_EQ(lookup.size(), 4);
  EXPECT_EQ(lookup["a"]->name(), "a");
  EXPECT_EQ(lookup["b"]->name(), "b");
  EXPECT_EQ(lookup["dir/aa"]->name(), "aa");
  EXPECT_EQ(lookup["dir/bb"]->name(), "bb");

  ASSERT_OK(builder.Flush());

  // Builder2 won't have the indirect asset lists loaded.
  // Assets in indirect lists won't be in the lookup.
  ManifestBuilder builder2(cdc_params_, &cache_);
  EXPECT_OK(builder2.LoadManifest(builder.ManifestId()));

  lookup = builder2.CreateFileLookup();
  ASSERT_EQ(lookup.size(), 2);
  EXPECT_EQ(lookup["dir/aa"]->name(), "aa");
  EXPECT_EQ(lookup["dir/bb"]->name(), "bb");
}

}  // namespace
}  // namespace cdc_ft
