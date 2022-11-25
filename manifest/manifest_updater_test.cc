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

#include "manifest/manifest_updater.h"

#include "absl/strings/match.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "data_store/mem_data_store.h"
#include "fastcdc/fastcdc.h"
#include "gtest/gtest.h"
#include "manifest/file_chunk_map.h"
#include "manifest/manifest_builder.h"
#include "manifest/manifest_iterator.h"
#include "manifest/manifest_test_base.h"

namespace cdc_ft {

void PrintTo(const AssetInfo& ai, std::ostream* o) {
  *o << "path=" << ai.path << ", type=" << ai.type << ", mtime=" << ai.mtime
     << ", size=" << ai.size;
}

namespace {

constexpr uint64_t kFileSizeA = 8;   // a.txt
constexpr uint64_t kFileSizeB = 32;  // subdir/b.txt
constexpr uint64_t kFileSizeC = 1;   // subdir/c.txt
constexpr uint64_t kFileSizeD = 1;   // subdir/d.txt
constexpr uint64_t kTotalFileSize =
    kFileSizeA + kFileSizeB + kFileSizeC + kFileSizeD;

class ManifestUpdaterTest : public ManifestTestBase {
 public:
  ManifestUpdaterTest()
      : ManifestTestBase(GetTestDataDir("manifest_updater")) {}

  void SetUp() override {
    path::CreateDirRec(empty_dir_).IgnoreError();
    cfg_.num_threads = 1;
  }

  void TearDown() override { path::RemoveDirRec(empty_dir_).IgnoreError(); }

 protected:
  std::string empty_dir_ = path::Join(path::GetTempDir(), "empty");
};

// Runs UpdateAll() on an empty dir.
TEST_F(ManifestUpdaterTest, UpdateAll_EmptySrcDirectory) {
  cfg_.src_dir = empty_dir_;
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  UpdaterStats stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);

  // Store should contain a chunk for the manifest id and one for the manifest.
  EXPECT_EQ(data_store_.Chunks().size(), 2);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, updater.ManifestId()));
}

// Runs UpdateAll() on a non-empty dir.
TEST_F(ManifestUpdaterTest, UpdateAll_NonEmptySrcDirectory) {
  // Contains a.txt and subdir/b.txt.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 5);
  EXPECT_EQ(stats.total_files_added_or_updated, 4);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 4);
  EXPECT_EQ(stats.total_processed_bytes, kTotalFileSize);

  // Store should contain a chunk for the manifest id and one for the manifest.
  EXPECT_EQ(data_store_.Chunks().size(), 2);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      updater.ManifestId()));
}

// Runs UpdateAll() with existing manifest that misses a file.
TEST_F(ManifestUpdaterTest, UpdateAll_AddFileIncremental) {
  // Create a manifest with "subdir/b.txt" missing.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  EXPECT_OK(updater.Update(
      MakeDeleteOps({"subdir/b.txt", "subdir/c.txt", "subdir/d.txt"}),
      &file_chunks_, nullptr));
  ASSERT_NO_FATAL_FAILURE(
      ExpectManifestEquals({"a.txt", "subdir"}, updater.ManifestId()));

  // UpdateAll() should compute the proper diff from {"a.txt", "subdir"} to
  // {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"} and
  // only add/update one file.
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 3);
  EXPECT_EQ(stats.total_files_added_or_updated, 3);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 3);
  EXPECT_EQ(stats.total_processed_bytes, kFileSizeB + kFileSizeC + kFileSizeD);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      updater.ManifestId()));
}

// Runs UpdateAll() with existing manifest that has an excessive file.
TEST_F(ManifestUpdaterTest, UpdateAll_DeleteFileIncremental) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  // Smuggle c.txt into the manifest.
  CdcParamsProto params;
  params.set_min_chunk_size(cfg_.min_chunk_size);
  params.set_avg_chunk_size(cfg_.avg_chunk_size);
  params.set_max_chunk_size(cfg_.max_chunk_size);
  ManifestBuilder mb(params, &data_store_);
  EXPECT_OK(mb.LoadManifest(updater.ManifestId()));
  EXPECT_OK(mb.GetOrCreateAsset("c.txt", AssetProto::FILE));
  EXPECT_OK(mb.Flush());
  std::string id_str = mb.ManifestId().SerializeAsString();
  EXPECT_OK(data_store_.Put(manifest_store_id_, id_str.data(), id_str.size()));

  // UpdateAll() should compute the proper diff from
  // {"a.txt", "c.txt", "subdir", "subdir/b.txt"} to
  // {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"} and
  // only delete one file.
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 1);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      updater.ManifestId()));
}

// UpdateAll() removes unreferenced manifest chunks.
TEST_F(ManifestUpdaterTest, UpdateAll_PrunesUnreferencedChunks) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.Update(MakeUpdateOps({"a.txt"}), &file_chunks_, nullptr));
  // 1 for manifest id, 1 for manifest, 1 indirect assets.
  EXPECT_EQ(data_store_.Chunks().size(), 3);

  EXPECT_OK(updater.Update(
      MakeUpdateOps({"subdir/b.txt", "subdir/c.txt", "subdir/d.txt"}),
      &file_chunks_, nullptr));
  // 1 for manifest id, 1 for manifest, 5 indirect assets.
  // 2 additional chunks from the first Update() that are now unreferenced.
  // -1, because the indirect asset for "a.txt" is deduplicated
  EXPECT_EQ(data_store_.Chunks().size(), 8)
      << "Manifest: " << ContentId::ToHexString(updater.ManifestId())
      << std::endl
      << DumpDataStoreProtos();

  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  // 1 for manifest id, 1 for manifest, 5 indirect assets.
  // Pruning has removed the 2 unreferenced ones.
  EXPECT_EQ(data_store_.Chunks().size(), 7)
      << "Manifest: " << ContentId::ToHexString(updater.ManifestId())
      << std::endl
      << DumpDataStoreProtos();
}

// UpdateAll() recovers if there are missing referenced manifest chunks.
TEST_F(ManifestUpdaterTest, UpdateAll_RecoversFromMissingChunks) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.Update(MakeUpdateOps({"a.txt"}), &file_chunks_, nullptr));
  // 1 for manifest id, 1 for manifest, 1 indirect assets.
  EXPECT_EQ(data_store_.Chunks().size(), 3)
      << "Manifest: " << ContentId::ToHexString(updater.ManifestId())
      << std::endl
      << DumpDataStoreProtos();

  // Remove one of the indirect chunks list.
  for (const auto& [id, _] : data_store_.Chunks()) {
    if (id != ManifestUpdater::GetManifestStoreId() &&
        id != updater.ManifestId()) {
      data_store_.Chunks().erase(id);
      break;
    }
  }

  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  // 1 for manifest id, 1 for manifest, 5 indirect assets.
  // There would be 8 chunks without the removal above, see UpdateAll_Prune.
  EXPECT_EQ(data_store_.Chunks().size(), 7)
      << "Manifest: " << ContentId::ToHexString(updater.ManifestId())
      << std::endl
      << DumpDataStoreProtos();
}

// Verifies that |file_chunks_| contains the expected chunks after UpdateAll().
TEST_F(ManifestUpdaterTest, UpdateAll_FileChunkMapFromScratch) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  ValidateChunkLookup("a.txt", true);
  ValidateChunkLookup("subdir/b.txt", true);
  ValidateChunkLookup("subdir/c.txt", true);
  ValidateChunkLookup("subdir/d.txt", true);
}

// Verifies that |file_chunks_| contains the expected chunks after UpdateAll().
TEST_F(ManifestUpdaterTest, UpdateAll_FileChunkMapAfterUpdate) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  ASSERT_OK(updater.UpdateAll(&file_chunks_));
  // The file chunks will be populated again by UpdateAll().
  file_chunks_.Clear();
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  ValidateChunkLookup("a.txt", true);
  ValidateChunkLookup("subdir/b.txt", true);
  ValidateChunkLookup("subdir/c.txt", true);
  ValidateChunkLookup("subdir/d.txt", true);
}

// Verifies that the intermediate manifest contains the expected files.
TEST_F(ManifestUpdaterTest, UpdateAll_PushIntermediateManifest) {
  ContentIdProto intermediate_id;
  auto push_manifest = [&intermediate_id](const ContentIdProto& manifest_id) {
    // Catch the first (= intermediate) manifest.
    if (intermediate_id == ContentIdProto()) {
      intermediate_id = manifest_id;
    }
  };

  // Contains a.txt and subdir/b.txt.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_, push_manifest));

  // Double check that the files in the final manifest are no longer in
  // progress.
  EXPECT_FALSE(InProgress(updater.ManifestId(), "a.txt"));
  EXPECT_FALSE(InProgress(updater.ManifestId(), "subdir/b.txt"));
  EXPECT_FALSE(InProgress(updater.ManifestId(), "subdir/c.txt"));
  EXPECT_FALSE(InProgress(updater.ManifestId(), "subdir/d.txt"));

  // Verify that the intermediate manifest is there, but it is empty.
  std::string ser_id = intermediate_id.SerializeAsString();
  EXPECT_OK(data_store_.Put(manifest_store_id_, ser_id.data(), ser_id.size()));
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, intermediate_id));
  // The root directory of the intermediate manifest is in progress.
  EXPECT_TRUE(InProgress(intermediate_id, ""));
}

// Runs Update() with a single file to be added.
TEST_F(ManifestUpdaterTest, Update_AddFile) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.Update(MakeUpdateOps({"a.txt"}), &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 1);
  EXPECT_EQ(stats.total_files_added_or_updated, 1);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 1);
  EXPECT_EQ(stats.total_processed_bytes, kFileSizeA);
  ASSERT_NO_FATAL_FAILURE(
      ExpectManifestEquals({"a.txt"}, updater.ManifestId()));
}

// Runs Update() with a single file to be added. The file is in a dir that is
// not contained in the manifest yet, so the dir will get auto-created.
TEST_F(ManifestUpdaterTest, Update_AddFileAutoCreateSubdir) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(
      updater.Update(MakeUpdateOps({"subdir/b.txt"}), &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 1);
  EXPECT_EQ(stats.total_files_added_or_updated, 1);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 1);
  EXPECT_EQ(stats.total_processed_bytes, kFileSizeB);

  // Note: The manifest does NOT contain the proper "subdir" asset now. Since it
  //       was auto-created because of "subdir/b.txt", it does not have the
  //       proper file time.
  std::vector<AssetInfoForTest> manifest_ais =
      GetAllManifestAssets(updater.ManifestId());
  std::vector<AssetInfoForTest> expected_ais =
      MakeAssetInfos({"subdir", "subdir/b.txt"});
  ExpectAssetInfosEqual(manifest_ais, expected_ais, false);
  manifest_ais[0].info.mtime = expected_ais[0].info.mtime;
  ExpectAssetInfosEqual(manifest_ais, expected_ais, true);
}

// Calls Update() with a single file to be deleted.
TEST_F(ManifestUpdaterTest, Update_DeleteFiles) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  EXPECT_OK(updater.Update(MakeDeleteOps({"a.txt"}), &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 1);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      updater.ManifestId()));

  // Delete another one in a subdirectory.
  EXPECT_OK(
      updater.Update(MakeDeleteOps({"subdir/b.txt"}), &file_chunks_, nullptr));
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"subdir", "subdir/c.txt", "subdir/d.txt"}, updater.ManifestId()));
}

// Calls Update() with a single dir to be deleted.
TEST_F(ManifestUpdaterTest, Update_DeleteDir) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  EXPECT_OK(updater.Update(MakeDeleteOps({"subdir"}), &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 1);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);
  ASSERT_NO_FATAL_FAILURE(
      ExpectManifestEquals({"a.txt"}, updater.ManifestId()));
}

// Calls Update() with a non-existing asset to be deleted.
TEST_F(ManifestUpdaterTest, Update_DeleteNonExistingAsset) {
  cfg_.src_dir = empty_dir_;
  ManifestUpdater updater(&data_store_, cfg_);
  // We need to craft AssetInfos for non-existing assets manually.
  AssetInfo ai{"non_existing", AssetProto::DIRECTORY};
  ManifestUpdater::OperationList ops{{Operator::kDelete, ai}};
  EXPECT_OK(updater.Update(&ops, &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_deleted, 1);
}

// Calls Update() with a non-existing file to be added.
TEST_F(ManifestUpdaterTest, Update_AddNonExistingFile) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  // Note that Update() succeeds even through the "non_existing" file failed.
  AssetInfo ai;
  ai.path = "non_existing";
  ManifestUpdater::OperationList ops{
      {Operator::kAdd, ai}, {Operator::kAdd, MakeAssetInfo("a.txt").info}};
  EXPECT_OK(updater.Update(&ops, &file_chunks_, nullptr));

  const UpdaterStats& stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 2);
  EXPECT_EQ(stats.total_files_added_or_updated, 1);
  EXPECT_EQ(stats.total_files_failed, 1);
  // "non_existing" and "a.txt" were still added, but the former is empty.
  std::vector<AssetInfoForTest> manifest_ais =
      GetAllManifestAssets(updater.ManifestId());
  std::vector<AssetInfoForTest> expected_ais = {AssetInfoForTest{ai},
                                                MakeAssetInfo("a.txt")};
  ExpectAssetInfosEqual(manifest_ais, expected_ais);
}

// Verifies that the intermediate manifest contains the expected files.
TEST_F(ManifestUpdaterTest, Update_PushIntermediateManifest) {
  // Create a manifest without a.txt.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  EXPECT_OK(updater.Update(
      MakeDeleteOps({"subdir/b.txt", "subdir/c.txt", "subdir/d.txt"}),
      &file_chunks_, nullptr));

  // Add a.txt back and check intermediate manifest.
  ContentIdProto intermediate_id;
  auto push_manifest = [&intermediate_id](const ContentIdProto& manifest_id) {
    // Catch the first (= intermediate) manifest.
    if (intermediate_id == ContentIdProto()) {
      intermediate_id = manifest_id;
    }
  };
  EXPECT_OK(updater.Update(
      MakeUpdateOps({"subdir/b.txt", "subdir/c.txt", "subdir/d.txt"}),
      &file_chunks_, push_manifest));
  EXPECT_GT(intermediate_id.blake3_sum_160().size(), 0);

  // Only file a.txt is done in the intermediate manifest, all others are in
  // progress.
  EXPECT_FALSE(InProgress(intermediate_id, "a.txt"));
  EXPECT_TRUE(InProgress(intermediate_id, "subdir/b.txt"));
  EXPECT_TRUE(InProgress(intermediate_id, "subdir/c.txt"));
  EXPECT_TRUE(InProgress(intermediate_id, "subdir/d.txt"));
}

// Verifies that |file_chunks_| contains the expected chunks after Update().
TEST_F(ManifestUpdaterTest, Update_FileChunkMap) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  // Add a.txt.
  EXPECT_OK(updater.Update(MakeUpdateOps({"a.txt"}), &file_chunks_, nullptr));
  ValidateChunkLookup("a.txt", true);
  ValidateChunkLookup("subdir/b.txt", false);

  // Add subdir/b.txt.
  EXPECT_OK(
      updater.Update(MakeUpdateOps({"subdir/b.txt"}), &file_chunks_, nullptr));
  ValidateChunkLookup("a.txt", true);
  ValidateChunkLookup("subdir/b.txt", true);

  // Remove a.txt.
  EXPECT_OK(updater.Update(MakeDeleteOps({"a.txt"}), &file_chunks_, nullptr));
  ValidateChunkLookup("a.txt", false);
  ValidateChunkLookup("subdir/b.txt", true);
}

// Verifies that |file_chunks_| contains the expected chunks an intermediate
// update (and does not deadlock!).
TEST_F(ManifestUpdaterTest, Update_IntermediateFileChunkMap) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  // Add a.txt.
  EXPECT_OK(updater.Update(MakeUpdateOps({"a.txt"}), &file_chunks_, nullptr));

  // Add subdir/b.txt and check intermediate lookups.
  int count = 0;
  auto push_manifest = [this, &count](const ContentIdProto&) {
    ++count;
    ValidateChunkLookup("a.txt", true);
    // The first (= intermediate) manifest does not have the chunks, the second
    // (= final) does.
    ValidateChunkLookup("subdir/b.txt", count > 1);
  };

  EXPECT_OK(updater.Update(MakeUpdateOps({"subdir/b.txt"}), &file_chunks_,
                           push_manifest));
}

// A call to ManifestId() returns the manifest id!!!
TEST_F(ManifestUpdaterTest, ManifestId) {
  cfg_.src_dir = empty_dir_;
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  ContentIdProto manifest_id;
  EXPECT_OK(data_store_.GetProto(manifest_store_id_, &manifest_id));
  EXPECT_EQ(updater.ManifestId(), manifest_id);
}

TEST_F(ManifestUpdaterTest, VerifyPermissions) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  ManifestIterator manifest_iter(&data_store_);
  EXPECT_OK(manifest_iter.Open(updater.ManifestId()));
  const AssetProto* entry;
  while ((entry = manifest_iter.NextEntry()) != nullptr) {
    switch (entry->type()) {
      case AssetProto::FILE:
        EXPECT_EQ(entry->permissions(), ManifestBuilder::kDefaultFilePerms);
        break;
      case AssetProto::DIRECTORY:
        EXPECT_EQ(entry->permissions(), ManifestBuilder::kDefaultDirPerms);
        break;
      case AssetProto::SYMLINK:
        // Symlinks don't have their own permissions.
        break;
      default:
        FAIL() << "Unhandled type: " << AssetProto::Type_Name(entry->type());
        break;
    }
  }
}

TEST_F(ManifestUpdaterTest, VerifyIntermediateFilesAreExecutable) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  int count = 0;
  auto push_intermediate_manifest = [this, &count](
                                        const ContentIdProto& manifest_id) {
    ++count;
    ManifestIterator manifest_iter(&data_store_);
    EXPECT_OK(manifest_iter.Open(manifest_id));
    const AssetProto* entry;
    while ((entry = manifest_iter.NextEntry()) != nullptr) {
      switch (entry->type()) {
        case AssetProto::FILE:
          if (count == 1) {
            // While the manifest is in-progress, all files are set to be
            // executable.
            EXPECT_EQ(entry->permissions(), ManifestUpdater::kExecutablePerms);
          } else {
            EXPECT_EQ(entry->permissions(), ManifestBuilder::kDefaultFilePerms);
          }
          break;
        case AssetProto::DIRECTORY:
          EXPECT_EQ(entry->permissions(), ManifestBuilder::kDefaultDirPerms);
          break;
        default:
          FAIL() << "Unhandled type: " << AssetProto::Type_Name(entry->type());
          break;
      }
    }
  };

  // Add subdir/b.txt and verify the file permissions.
  EXPECT_OK(updater.Update(MakeUpdateOps({"subdir/b.txt"}), &file_chunks_,
                           push_intermediate_manifest));
  EXPECT_EQ(updater.Stats().total_files_added_or_updated, 1);
}

// Makes sure that executables are properly detected.
TEST_F(ManifestUpdaterTest, DetectExecutables) {
  cfg_.src_dir = path::Join(base_dir_, "executables");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  ContentIdProto manifest_id;
  EXPECT_OK(data_store_.GetProto(manifest_store_id_, &manifest_id));

  ManifestIterator manifest_iter(&data_store_);
  EXPECT_OK(manifest_iter.Open(manifest_id));

  std::unordered_map<std::string, uint32_t> path_to_perms;
  const AssetProto* entry;
  while ((entry = manifest_iter.NextEntry()) != nullptr)
    path_to_perms[entry->name()] = entry->permissions();
  EXPECT_OK(manifest_iter.Status());

  EXPECT_EQ(path_to_perms["game.elf"], ManifestUpdater::kExecutablePerms);
  EXPECT_EQ(path_to_perms["win.exe"], ManifestUpdater::kExecutablePerms);
  EXPECT_EQ(path_to_perms["script.sh"], ManifestUpdater::kExecutablePerms);
  EXPECT_EQ(path_to_perms["normal.txt"], ManifestBuilder::kDefaultFilePerms);
}

TEST_F(ManifestUpdaterTest, UpdateAll_LargeIntermediateIndirectDirAssets) {
  // Reduce chunk sizes to produce a bunch of indirect lists.
  cfg_.min_chunk_size = 8;
  cfg_.avg_chunk_size = 16;
  cfg_.max_chunk_size = 32;

  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);

  // Run UpdateAll() with intermediate manifest push. The push causes a Flush()
  // call to the manifest builder, which pushes some assets to indirect lists.
  // This used to invalidate pointers and cause asserts to trigger.
  EXPECT_OK(updater.UpdateAll(&file_chunks_, [](const ContentIdProto&) {}));
}

// Runs increamental UpdateAll() on an empty dir.
TEST_F(ManifestUpdaterTest, UpdateAll_EmptySrcDirectory_Incremental) {
  cfg_.src_dir = empty_dir_;
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  CdcParamsProto params;
  params.set_min_chunk_size(cfg_.min_chunk_size);
  params.set_avg_chunk_size(cfg_.avg_chunk_size);
  params.set_max_chunk_size(cfg_.max_chunk_size);
  ManifestBuilder mb(params, &data_store_);
  EXPECT_OK(mb.LoadManifest(updater.ManifestId()));
  EXPECT_OK(mb.GetOrCreateAsset("folder1", AssetProto::DIRECTORY));
  EXPECT_OK(mb.DeleteAsset("folder1"));
}

TEST_F(ManifestUpdaterTest, UpdateAll_FileAsRootFails) {
  cfg_.src_dir = path::Join(base_dir_, "non_empty", "a.txt");
  ManifestUpdater updater(&data_store_, cfg_);
  auto status = updater.UpdateAll(&file_chunks_);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::IsFailedPrecondition(status)) << status.ToString();
}

TEST_F(ManifestUpdaterTest, UpdateAll_RootNotExistFails) {
  cfg_.src_dir = path::Join(base_dir_, "non-existing");
  ManifestUpdater updater(&data_store_, cfg_);
  auto status = updater.UpdateAll(&file_chunks_);
  EXPECT_NOT_OK(status);
  EXPECT_TRUE(absl::IsNotFound(status)) << status.ToString();
}

// Runs UpdateAll() multiple times on an empty dir with no changes.
TEST_F(ManifestUpdaterTest, UpdateAll_EmptySrcDirectoryMultiTimesNoChange) {
  cfg_.src_dir = empty_dir_;
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  UpdaterStats stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);

  // Store should contain a chunk for the manifest id and one for the manifest.
  EXPECT_EQ(data_store_.Chunks().size(), 2);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, updater.ManifestId()));

  // No new changes should be done.
  EXPECT_OK(updater.UpdateAll(&file_chunks_));
  stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);
}

// Runs UpdateAll() multiple times on a non-empty dir with no changes.
TEST_F(ManifestUpdaterTest, UpdateAll_NonEmptySrcDirectoryMultiTimesNoChange) {
  // Contains a.txt and subdir/b.txt.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  ManifestUpdater updater(&data_store_, cfg_);
  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  UpdaterStats stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 5);
  EXPECT_EQ(stats.total_files_added_or_updated, 4);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 4);
  EXPECT_EQ(stats.total_processed_bytes, kTotalFileSize);

  // Store should contain a chunk for the manifest id and one for the manifest.
  EXPECT_EQ(data_store_.Chunks().size(), 2);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      updater.ManifestId()));

  EXPECT_OK(updater.UpdateAll(&file_chunks_));

  // No new changes should be done.
  stats = updater.Stats();
  EXPECT_EQ(stats.total_assets_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_added_or_updated, 0);
  EXPECT_EQ(stats.total_files_failed, 0);
  EXPECT_EQ(stats.total_assets_deleted, 0);
  EXPECT_EQ(stats.total_chunks, 0);
  EXPECT_EQ(stats.total_processed_bytes, 0);
}

TEST_F(ManifestUpdaterTest, IsValidDir) {
  EXPECT_OK(ManifestUpdater::IsValidDir(path::Join(base_dir_, "non_empty")));
  EXPECT_TRUE(absl::IsNotFound(
      ManifestUpdater::IsValidDir(path::Join(base_dir_, "non-existing"))));
  EXPECT_TRUE(absl::IsFailedPrecondition(ManifestUpdater::IsValidDir(
      path::Join(base_dir_, "non_empty", "a.txt"))));
  EXPECT_TRUE(
      absl::IsFailedPrecondition(ManifestUpdater::IsValidDir("relative_dir")));
}

}  // namespace
}  // namespace cdc_ft
