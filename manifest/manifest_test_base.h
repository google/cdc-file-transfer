/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MANIFEST_MANIFEST_TEST_BASE_H_
#define MANIFEST_MANIFEST_TEST_BASE_H_

#include <initializer_list>

#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"
#include "manifest/file_chunk_map.h"
#include "manifest/manifest_updater.h"

namespace cdc_ft {

// Test helper class to compare expected and actual manifests.
class ManifestTestBase : public ::testing::Test {
 public:
  struct AssetInfoForTest {
    AssetInfo info;
    bool in_progress = false;

    bool operator==(const AssetInfoForTest& other) const {
      return info == other.info && in_progress == other.in_progress;
    }

    bool operator!=(const AssetInfoForTest& other) const {
      return !(*this == other);
    }

    // Compares by file path.
    bool operator<(const AssetInfoForTest& other) const {
      return info.path < other.info.path;
    }
  };

  explicit ManifestTestBase(std::string base_dir);
  ~ManifestTestBase() = default;

 protected:
  using Operation = ManifestUpdater::Operation;
  using Operator = ManifestUpdater::Operator;

  // Returns the list of assets in the manifest stored in |data_store_|.
  std::vector<AssetInfoForTest> GetAllManifestAssets(
      ContentIdProto actual_manifest_id);

  // Creates AssetInfo from the real files at |rel_path|.
  // The path is relative to |cfg_.src_dir|.
  AssetInfoForTest MakeAssetInfo(const std::string& rel_path);

  // Creates AssetInfos from the real files at |rel_paths|.
  // The paths are relative to |cfg_.src_dir|.
  std::vector<AssetInfoForTest> MakeAssetInfos(
      std::initializer_list<std::string> rel_paths);

  // Creates |op| operations for the given list of file paths.
  // The paths are relative to |cfg_.src_dir|.
  ManifestUpdater::OperationList* MakeOps(
      Operator op, std::initializer_list<std::string> rel_paths);

  // Creates kDelete operations for the given list of file paths.
  // The paths are relative to |cfg_.src_dir|.
  ManifestUpdater::OperationList* MakeDeleteOps(
      std::initializer_list<std::string> rel_paths);

  // Creates kUpdate operations from the real files at |rel_paths|.
  // The paths are relative to |cfg_.src_dir|.
  ManifestUpdater::OperationList* MakeUpdateOps(
      std::initializer_list<std::string> rel_paths);

  // Expects that |a| and |b| are (not) equal, independently of order.
  void ExpectAssetInfosEqual(std::vector<AssetInfoForTest> a,
                             std::vector<AssetInfoForTest> b,
                             bool equal = true);

  // Compares the contents of the manifest to the real files at |rel_paths|.
  // The paths are relative to |cfg_.src_dir|.
  void ExpectManifestEquals(std::initializer_list<std::string> rel_paths,
                            const ContentIdProto& got_manifest_id);

  // Returns true if the file at Unix |path| contains file chunks in the
  // manifest referenced by |manifest_id|.
  // Expects the file to be present.
  bool InProgress(const ContentIdProto& manifest_id, const char* path);

  // Validates that all file chunks in the file at |rel_path| are present in
  // |file_chunks_| if |expect_contained| is true. Otherwise, validates that
  // none of the chunks are present.
  void ValidateChunkLookup(const std::string& rel_path, bool expect_contained);

  // Tries to parse all stored data chunks as manifest protos and formats them
  // as text protos. In order to disambiguate the proto auto-detection logic,
  // you can temporarily assign globally unique field numbers to all fields in
  // manifest.proto.
  //
  // Sample output:
  //
  // # aa8bef577a9af66e9330140c394e5fce557bd677 =>
  //    cdc_ft.proto.Manifest (size: 48)
  // root_dir {
  //   type: DIRECTORY
  //   mtime_seconds: 1663935163
  //   permissions: 493
  //   dir_indirect_assets {
  //     blake3_sum_160: "27b0cd2923714d143f32ec5394a02421fc89f5bc"
  //   }
  // }
  // cdc_params {
  //   min_chunk_size: 8
  //   avg_chunk_size: 16
  //   max_chunk_size: 32
  // }
  // # 27b0cd2923714d143f32ec5394a02421fc89f5bc =>
  //     cdc_ft.proto.AssetList (size: 52)
  // assets {
  //   name: "a.txt"
  //   type: FILE
  //   mtime_seconds: 1653999616
  //   permissions: 420
  //   file_size: 8
  //   file_chunks {
  //     chunk_id {
  //       blake3_sum_160: "b1e57baceafdc3b03ab5189cb245757799874fbf"
  //     }
  //   }
  //   in_progress: true
  // }
  std::string DumpDataStoreProtos() const;

  std::string base_dir_;
  MemDataStore data_store_;
  UpdaterConfig cfg_;

  FileChunkMap file_chunks_{/*enable_stats=*/false};
  ManifestUpdater::OperationList ops_;
  ContentIdProto manifest_store_id_ = ManifestUpdater::GetManifestStoreId();
};

}  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_TEST_BASE_H_
