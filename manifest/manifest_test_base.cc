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

#include "manifest/manifest_test_base.h"

#include "common/path.h"
#include "common/status_test_macros.h"
#include "fastcdc/fastcdc.h"
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

  if (size > 0) {
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
  }
  return absl::StrFormat("# %s => %s (size: %d)\n%s",
                         ContentId::ToHexString(content_id), proto_name, isize,
                         text_proto);
}
}  // namespace

// Prints an AssetInfo object.
std::ostream& operator<<(std::ostream& os,
                         const ManifestTestBase::AssetInfoForTest& ai) {
  os << "{.path = \"" << ai.info.path
     << "\", .type = " << AssetProto::Type_Name(ai.info.type)
     << ", .mtime = " << ai.info.mtime << ", .size = " << ai.info.size
     << ", .in_progress = " << (ai.in_progress ? "true" : "false") << "}";
  return os;
}

ManifestTestBase::ManifestTestBase(std::string base_dir)
    : ::testing::Test(), base_dir_(base_dir) {}

std::vector<ManifestTestBase::AssetInfoForTest>
ManifestTestBase::GetAllManifestAssets(ContentIdProto actual_manifest_id) {
  ContentIdProto expected_manifest_id;
  EXPECT_OK(data_store_.GetProto(manifest_store_id_, &expected_manifest_id));
  EXPECT_EQ(ContentId::ToHexString(expected_manifest_id),
            ContentId::ToHexString(actual_manifest_id))
      << DumpDataStoreProtos();

  ManifestIterator manifest_iter(&data_store_);
  EXPECT_OK(manifest_iter.Open(expected_manifest_id));

  std::vector<AssetInfoForTest> assets;
  const AssetProto* entry;
  while ((entry = manifest_iter.NextEntry()) != nullptr) {
    AssetInfoForTest ai;
    ai.info.path = path::JoinUnix(manifest_iter.RelativePath(), entry->name());
    ai.info.type = entry->type();
    ai.info.mtime = entry->mtime_seconds();
    ai.info.size = entry->file_size();
    ai.in_progress = entry->in_progress();
    assets.push_back(std::move(ai));
  }

  EXPECT_OK(manifest_iter.Status());
  return assets;
}

ManifestTestBase::AssetInfoForTest ManifestTestBase::MakeAssetInfo(
    const std::string& rel_path) {
  std::string full_path = path::Join(cfg_.src_dir, rel_path);
  path::Stats stats;
  EXPECT_OK(path::GetStats(full_path, &stats));
  // Don't use the stats.modified_time as this returns timestamps in the
  // machine's local time, whereas GetFileTime() returns UTC time.
  time_t mtime;
  EXPECT_OK(path::GetFileTime(full_path, &mtime));

  AssetInfoForTest ai;
  ai.info.path = rel_path;
  ai.info.type =
      stats.mode & path::MODE_IFDIR ? AssetProto::DIRECTORY : AssetProto::FILE;
  ai.info.mtime = static_cast<int64_t>(mtime);
  ai.info.size = ai.info.type == AssetProto::DIRECTORY ? 0 : stats.size;
  return ai;
}

std::vector<ManifestTestBase::AssetInfoForTest>
ManifestTestBase::MakeAssetInfos(std::initializer_list<std::string> rel_paths) {
  std::vector<AssetInfoForTest> ais;
  for (const std::string& rel_path : rel_paths) {
    ais.push_back(MakeAssetInfo(rel_path));
  }
  return ais;
}

ManifestUpdater::OperationList* ManifestTestBase::MakeOps(
    Operator op, std::initializer_list<std::string> rel_paths) {
  ops_.clear();
  ops_.reserve(rel_paths.size());
  for (const auto& rel_path : rel_paths) {
    ops_.emplace_back(op, MakeAssetInfo(rel_path).info);
  }
  return &ops_;
}

ManifestUpdater::OperationList* ManifestTestBase::MakeDeleteOps(
    std::initializer_list<std::string> rel_paths) {
  return MakeOps(Operator::kDelete, rel_paths);
}

ManifestUpdater::OperationList* ManifestTestBase::MakeUpdateOps(
    std::initializer_list<std::string> rel_paths) {
  return MakeOps(Operator::kUpdate, rel_paths);
}

void ManifestTestBase::ExpectAssetInfosEqual(std::vector<AssetInfoForTest> a,
                                             std::vector<AssetInfoForTest> b,
                                             bool equal) {
  std::sort(a.begin(), a.end());
  std::sort(b.begin(), b.end());
  if (equal) {
    EXPECT_EQ(a, b);
  } else {
    EXPECT_NE(a, b);
  }
}

void ManifestTestBase::ExpectManifestEquals(
    std::initializer_list<std::string> rel_paths,
    const ContentIdProto& actual_manifest_id) {
  std::vector<AssetInfoForTest> actual_ais =
      GetAllManifestAssets(actual_manifest_id);
  std::vector<AssetInfoForTest> expected_ais = MakeAssetInfos(rel_paths);
  ExpectAssetInfosEqual(actual_ais, expected_ais);
}

bool ManifestTestBase::InProgress(const ContentIdProto& manifest_id,
                                  const char* path) {
  // Special case: the root directory is not returned by the manifest iterator.
  if (absl::string_view(path) == "") {
    ManifestProto manifest;
    EXPECT_OK(data_store_.GetProto(manifest_id, &manifest));
    return manifest.root_dir().in_progress();
  }

  ManifestIterator manifest_iter(&data_store_);
  EXPECT_OK(manifest_iter.Open(manifest_id));
  if (!manifest_iter.Status().ok()) return false;

  const AssetProto* entry;
  while ((entry = manifest_iter.NextEntry()) != nullptr) {
    if (path == path::JoinUnix(manifest_iter.RelativePath(), entry->name()))
      return entry->in_progress();
  }

  EXPECT_TRUE(false) << "'" << path << "' not found in manifest";
  return false;
}

void ManifestTestBase::ValidateChunkLookup(const std::string& rel_path,
                                           bool expect_contained) {
  Buffer file;
  EXPECT_OK(path::ReadFile(path::Join(cfg_.src_dir, rel_path), &file));

  uint64_t offset = 0;
  auto handler = [&file, &offset, &rel_path, file_chunks = &file_chunks_,
                  expect_contained](const void* data, size_t size) {
    ContentIdProto id = ContentId::FromArray(data, size);

    std::string lookup_path;
    uint64_t lookup_offset = 0;
    uint32_t lookup_size = 0;
    EXPECT_EQ(
        file_chunks->Lookup(id, &lookup_path, &lookup_offset, &lookup_size),
        expect_contained);
    if (expect_contained) {
      EXPECT_EQ(lookup_path, rel_path);
      EXPECT_EQ(lookup_size, size);

      // The offset can be ambiguous since the file might contain duplicate
      // data. Make sure that the actual data is the same.
      EXPECT_LE(offset + size, file.size());
      EXPECT_LE(lookup_offset + size, file.size());
      EXPECT_EQ(memcmp(file.data() + offset, file.data() + lookup_offset, size),
                0);
    }

    offset += size;
  };
  fastcdc::Config cdc_cfg(cfg_.min_chunk_size, cfg_.avg_chunk_size,
                          cfg_.max_chunk_size);
  fastcdc::Chunker chunker(cdc_cfg, handler);

  chunker.Process(reinterpret_cast<uint8_t*>(file.data()), file.size());
  chunker.Finalize();
}

std::string ManifestTestBase::DumpDataStoreProtos() const {
  std::string s;
  for (const auto& [content_id, chunk] : data_store_.Chunks()) {
    s += ToTextProto(content_id, chunk.data(), chunk.size());
  }
  return s;
}

}  // namespace cdc_ft
