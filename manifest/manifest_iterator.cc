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

#include "manifest/manifest_iterator.h"

#include <google/protobuf/text_format.h>

#include <cassert>
#include <fstream>

#include "absl/strings/str_format.h"
#include "common/errno_mapping.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_macros.h"
#include "manifest/content_id.h"

namespace cdc_ft {

// Holds the iteration state for an opened DIRECTORY asset.
struct ManifestIterator::OpenedDirectory {
  OpenedDirectory(AssetProto* dir) : dir(dir) {}
  ~OpenedDirectory() = default;

  // The DIRECTORY proto that is being iterated over. The object is owned by the
  // parent OpenedDirectory struct.
  AssetProto* dir;

  // Holds the currently loaded indirect asset list.
  std::unique_ptr<AssetListProto> asset_list;

  // Index of the next direct asset to be returned from this directory. If the
  // index is equal to dir->dir_assets_size(), all direct assets have been
  // exhausted.
  int next_asset = 0;

  // Index of the next indirect asset list to be read. If the index is equal to
  // dir->dir_indirect_assets_size(), all indirect asset lists have been
  // exhausted.
  int next_asset_list = 0;

  // Index of the next asset of the currently loaded indirect asset list. If the
  // index is equal to asset_list->assets_size(), all assets in this list have
  // been exhausted.
  int next_asset_list_asset = 0;
};

ManifestIterator::ManifestIterator(DataStoreReader* data_store)
    : last_opened_dir_(nullptr), data_store_(data_store) {
  assert(data_store_ != nullptr);
}

ManifestIterator::~ManifestIterator() = default;

absl::Status ManifestIterator::Open(const ContentIdProto& manifest_id) {
  Reset();
  status_ = data_store_->GetProto(manifest_id, &manifest_);
  if (status_.ok()) dirs_.emplace_back(manifest_.mutable_root_dir());
  return status_;
}

absl::Status ManifestIterator::Open(const std::string& manifest_file) {
  Reset();
  errno = 0;
  // Open input file.
  std::ifstream fin(manifest_file, std::ios_base::in | std::ios_base::binary);
  if (!fin) {
    std::string msg =
        absl::StrFormat("failed to open file '%s' for reading", manifest_file);
    if (errno) {
      status_ = ErrnoToCanonicalStatus(errno, "%s", msg);
    } else {
      status_ = absl::UnknownError(msg);
    }
    return status_;
  }
  // Parse proto.
  if (!manifest_.ParseFromIstream(&fin)) {
    status_ = absl::InternalError(absl::StrFormat(
        "failed to parse Manifest proto from file '%s'", manifest_file));
    return status_;
  }
  dirs_.emplace_back(manifest_.mutable_root_dir());
  return absl::OkStatus();
}

bool ManifestIterator::Valid() const { return !dirs_.empty() && status_.ok(); }

AssetProto* ManifestIterator::MutableAsset(RepeatedAssetProto* assets,
                                           int index) {
  AssetProto* asset_pb = assets->Mutable(index);
  // Recurse into sub-directories.
  if (asset_pb->type() == AssetProto::DIRECTORY) dirs_.emplace_back(asset_pb);
  return asset_pb;
}

void ManifestIterator::UpdateRelPath(const OpenedDirectory* od) {
  if (last_opened_dir_ == od) return;
  rel_path_.resize(0);
  for (const auto& opened_dir : dirs_) {
    path::AppendUnix(&rel_path_, opened_dir.dir->name());
  }
  last_opened_dir_ = od;
}

const AssetProto* ManifestIterator::NextEntry() {
  while (!dirs_.empty() && status_.ok()) {
    OpenedDirectory* od = &dirs_.back();
    UpdateRelPath(od);

    // First, iterate over the direct assets.
    if (od->next_asset >= 0 && od->next_asset < od->dir->dir_assets_size()) {
      return MutableAsset(od->dir->mutable_dir_assets(), od->next_asset++);
    }

    // Next, iterate over the currently loaded indirect asset list.
    assert(od->next_asset_list_asset >= 0);
    if (od->asset_list &&
        od->next_asset_list_asset < od->asset_list->assets_size()) {
      return MutableAsset(od->asset_list->mutable_assets(),
                          od->next_asset_list_asset++);
    }

    // Finally, load the next AssetListProto from the indirect assets.
    assert(od->next_asset_list >= 0);
    if (od->next_asset_list < od->dir->dir_indirect_assets_size()) {
      // Create the proto, if needed.
      if (!od->asset_list) od->asset_list = std::make_unique<AssetListProto>();
      // Read the AssetListProto from the chunk store.
      const ContentIdProto& asset_list_id =
          od->dir->dir_indirect_assets(od->next_asset_list++);
      od->next_asset_list_asset = 0;
      status_ = data_store_->GetProto(asset_list_id, od->asset_list.get());
      if (!status_.ok()) return nullptr;
      // Restart the loop to read the first asset from the list.
      continue;
    }

    // Nothing more to visit, we are done with this node.
    dirs_.pop_back();
  }
  return nullptr;
}

void ManifestIterator::Reset() {
  dirs_.clear();
  last_opened_dir_ = nullptr;
  status_ = absl::OkStatus();
  rel_path_.resize(0);
}

}  // namespace cdc_ft
