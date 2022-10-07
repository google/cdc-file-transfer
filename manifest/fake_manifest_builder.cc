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

#include <vector>

#include "absl/strings/str_format.h"
#include "common/path.h"
#include "data_store/mem_data_store.h"
#include "fastcdc/fastcdc.h"

namespace cdc_ft {
namespace {

constexpr size_t kAvgChunkSize = 1024 * 256;
constexpr size_t kMinChunkSize = kAvgChunkSize / 2;
constexpr size_t kMaxChunkSize = kAvgChunkSize * 4;

// Builds a data blob for faking a large file that contains
//   <line number> - <60 random letters>
std::vector<char> BuildLargeFileData(int num_lines) {
  std::vector<char> data;
  char filler[60] = {0};
  for (int n = 0; n < num_lines; ++n) {
    for (size_t k = 0; k < sizeof(filler); ++k) {
      filler[k] = (rand() % 26) + 'a';
    }
    std::string n_str = std::to_string(n);
    data.insert(data.end(), n_str.c_str(), n_str.c_str() + n_str.size());
    data.push_back('-');
    data.insert(data.end(), filler, filler + sizeof(filler));
    data.push_back('\n');
  }
  return data;
}

void UpdateFileContent(AssetProto* asset, MemDataStore* const store,
                       const std::vector<char>& data) {
  uint64_t offset = 0;
  auto chunk_handler = [asset, store, &offset](const void* data, size_t size) {
    const char* char_data = reinterpret_cast<const char*>(data);
    std::vector<char> data_vec;
    data_vec.insert(data_vec.end(), char_data, char_data + size);
    ChunkRefProto* chunk_ref = asset->add_file_chunks();
    *chunk_ref->mutable_chunk_id() = store->AddData(data_vec);
    chunk_ref->set_offset(offset);
    offset += size;
  };

  fastcdc::Config config(kMinChunkSize, kAvgChunkSize, kMaxChunkSize);
  fastcdc::Chunker chunker(config, chunk_handler);

  chunker.Process(reinterpret_cast<const uint8_t*>(data.data()), data.size());
  chunker.Finalize();
}

AssetProto* FindAsset(AssetProto* dir_asset, const char* name) {
  assert(dir_asset);
  assert(dir_asset->type() == AssetProto::DIRECTORY);
  for (AssetProto& asset : *dir_asset->mutable_dir_assets()) {
    if (asset.name() == name) {
      return &asset;
    }
  }
  return nullptr;
}

}  // namespace

FakeManifestBuilder::FakeManifestBuilder(MemDataStore* store) : store_(store) {
  manifest_.mutable_root_dir()->set_type(AssetProto::DIRECTORY);
  manifest_.mutable_root_dir()->set_permissions(kRootDirPerms);
}

FakeManifestBuilder::~FakeManifestBuilder() = default;

void FakeManifestBuilder::AddFile(AssetProto* dir_asset, const char* name,
                                  int64_t mtime_sec, uint32_t permissions,
                                  const std::vector<char>& data) {
  assert(dir_asset);
  AssetProto* asset = dir_asset->add_dir_assets();
  asset->set_name(name);
  asset->set_type(AssetProto::FILE);
  asset->set_file_size(data.size());
  asset->set_mtime_seconds(mtime_sec);
  asset->set_permissions(permissions);

  UpdateFileContent(asset, store_, data);
}

AssetProto* FakeManifestBuilder::AddDirectory(AssetProto* dir_asset,
                                              const char* name,
                                              int64_t mtime_sec,
                                              uint32_t permissions) {
  assert(dir_asset);
  AssetProto* asset = dir_asset->add_dir_assets();
  asset->set_name(name);
  asset->set_type(AssetProto::DIRECTORY);
  asset->set_mtime_seconds(mtime_sec);
  asset->set_permissions(permissions);
  return asset;
}

ContentIdProto FakeManifestBuilder::BuildTestData() {
  const uint32_t kFileMode =
      path::MODE_IRUSR | path::MODE_IWUSR | path::MODE_IRGRP | path::MODE_IROTH;
  const uint32_t kDirMode = path::MODE_IRGRP | path::MODE_IXGRP |
                            path::MODE_IROTH | path::MODE_IXOTH |
                            path::MODE_IRWXU;
  const int64_t kModTime = 1614843754;

  // root
  // |- file1.txt
  // |- fio_test
  //    |- large_file1.txt
  //    |- ...
  //    |- large_file9.txt
  // |- a
  //    |- file2.txt
  //    |- b
  //       |- file3.txt

  AssetProto* fio_test_dir =
      AddDirectory(Root(), "fio_test", kModTime, kDirMode);

  // 500k lines generate a ~33 MB file.
  std::vector<char> data = BuildLargeFileData(500000);
  for (int n = 1; n < 9; ++n) {
    std::string filename = absl::StrFormat("large_file%i.txt", n);
    AddFile(fio_test_dir, filename.c_str(), kModTime, kFileMode, data);
  }

  AddFile(Root(), "file1.txt", kModTime, kFileMode, {'1', '3', '3', '7', '\n'});

  AssetProto* a_dir = AddDirectory(Root(), "a", kModTime, kDirMode);

  AddFile(a_dir, "file2.txt", kModTime, kFileMode,
          {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd', '!', '\n'});

  AssetProto* b_dir = AddDirectory(a_dir, "b", kModTime, kDirMode);

  AddFile(
      b_dir, "file3.txt", kModTime, kFileMode,
      {0127, 0105, 0122, 0040, 0104, 0101, 0123, 0040, 0114, 0111, 0105, 0123,
       0124, 0040, 0111, 0123, 0124, 0040, 0104, 0117, 0117, 0106, 0012});

  return store_->AddProto(manifest_);
}

const ManifestProto* FakeManifestBuilder::Manifest() const {
  return &manifest_;
}

AssetProto* FakeManifestBuilder::Root() { return manifest_.mutable_root_dir(); }

void FakeManifestBuilder::ModifyFile(AssetProto* dir_asset, const char* name,
                                     int64_t mtime_sec, uint32_t permissions,
                                     const std::vector<char>& data) {
  assert(dir_asset);
  AssetProto* asset = FindAsset(dir_asset, name);
  assert(asset && asset->type() == AssetProto::FILE);
  asset->set_file_size(data.size());
  asset->set_mtime_seconds(mtime_sec);
  asset->set_permissions(permissions);
  asset->clear_file_chunks();
  asset->clear_file_indirect_chunks();

  UpdateFileContent(asset, store_, data);
}
}  // namespace cdc_ft
