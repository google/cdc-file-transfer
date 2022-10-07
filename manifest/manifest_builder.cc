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

#include <cassert>
#include <deque>

#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/util.h"
#include "manifest/asset_builder.h"
#include "manifest/content_id.h"

namespace cdc_ft {

namespace {

// Splits the given Unix path into its components.
inline std::vector<absl::string_view> SplitUnixPath(const std::string& path) {
  return SplitString(path, '/', false);
}

// Joins the given path components using the Unix path separator. This function
// assumes that none of the path components have trailing path separators.
inline std::string JoinUnixPath(const std::vector<absl::string_view>& path) {
  return JoinStrings(path, 0, path.size(), '/');
}

}  // namespace

ManifestBuilder::ManifestBuilder(CdcParamsProto cdc_params,
                                 DataStoreWriter* chunk_store)
    : data_store_(chunk_store), cdc_params_(std::move(cdc_params)) {
  Reset();
}

ManifestBuilder::~ManifestBuilder() = default;

absl::Status ManifestBuilder::LoadManifest(const std::string& manifest_hex_id) {
  ContentIdProto manifest_id;
  if (!ContentId::FromHexString(manifest_hex_id, &manifest_id)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid manifest ID: '%s'", manifest_hex_id));
  }
  return LoadManifest(manifest_id);
}

absl::Status ManifestBuilder::LoadManifest(const ContentIdProto& manifest_id) {
  Reset();
  RETURN_IF_ERROR(data_store_->GetProto(manifest_id, manifest_));
  manifest_id_.CopyFrom(manifest_id);
  return absl::OkStatus();
}

void ManifestBuilder::Reset() {
  asset_lists_.clear();
  manifest_id_.Clear();
  manifest_bytes_written_ = 0;
  manifest_chunks_written_ = 0;
  arena_.Reset();
  manifest_ = MakeProto<ManifestProto>();
  *manifest_->mutable_cdc_params() = cdc_params_;
}

absl::StatusOr<AssetBuilder> ManifestBuilder::GetOrCreateAsset(
    const std::string& path, AssetProto::Type type, bool force_create,
    bool* created) {
  // We must keep |unix_path| allocated while the string_views in |parts| are
  // being used.
  if (created) *created = false;
  std::string unix_path = path::ToUnix(path);
  std::vector<absl::string_view> parts = SplitUnixPath(unix_path);
  absl::string_view name;
  if (!parts.empty()) {
    name = parts.back();
    parts.pop_back();
  }
  DirCreateMode create_mode =
      force_create ? DirCreateMode::kForceCreate : DirCreateMode::kCreate;
  AssetProto* dir;
  ASSIGN_OR_RETURN(dir, FindOrCreateDirPath(parts, create_mode),
                   "Failed to create directory '%s'", JoinUnixPath(parts));

  if (name.empty()) {
    // Special case: return the root directory for a DIRECTORY with empty name.
    if (type == AssetProto::DIRECTORY) return AssetBuilder(dir, std::string());
    return absl::InvalidArgumentError("Empty path given");
  }

  // Check if the asset already exists.
  absl::StatusOr<AssetProto*> result = FindAssetInDir(name, dir);
  AssetProto* asset = nullptr;
  if (result.ok()) {
    asset = result.value();
    // Verify that both assets are of the same type.
    if (asset->type() != type) {
      if (force_create) {
        RETURN_IF_ERROR(DeleteAsset(path));
        asset = nullptr;
      } else {
        return absl::AlreadyExistsError(absl::StrFormat(
            "Asset '%s' already exists in '%s' as %s.", path,
            JoinUnixPath(parts), AssetProto::Type_Name(asset->type())));
      }
    }
  } else if (!absl::IsNotFound(result.status())) {
    // Return any unexpected error.
    return result.status();
  }
  // Create the asset if it was not found or it was deleted.
  if (!asset) {
    asset = dir->add_dir_assets();
    InitNewAsset(name, type, asset);
    if (created) *created = true;
  }
  return AssetBuilder(asset, path::ToUnix(path::DirName(path)));
}

absl::Status ManifestBuilder::DeleteAsset(const std::string& path) {
  // We must keep |unix_path| allocated while the string_views in |parts| are
  // being used.
  std::string unix_path = path::ToUnix(path);
  std::vector<absl::string_view> parts = SplitUnixPath(unix_path);
  if (parts.empty()) return absl::InvalidArgumentError("Empty path given");
  absl::string_view name = parts.back();
  parts.pop_back();
  absl::StatusOr<AssetProto*> dir =
      FindOrCreateDirPath(parts, DirCreateMode::kNoCreate);
  if (!dir.ok()) {
    // We can get an absl::InvalidArgumentError here if one of the path
    // components is not a directory, which means the asset to be deleted does
    // not exist.
    if (absl::IsNotFound(dir.status()) ||
        absl::IsInvalidArgument(dir.status())) {
      return absl::OkStatus();
    }
    // Return any unexpected error.
    return WrapStatus(dir.status(), "Failed to look up path '%s'",
                      JoinUnixPath(parts));
  }

  // Check if the asset exists.
  return DeleteAssetFromDir(name, *dir);
}

absl::StatusOr<AssetProto*> ManifestBuilder::FindOrCreateDirPath(
    const std::vector<absl::string_view>& path, DirCreateMode create_dirs) {
  // Create the first manifest, if needed, independent of |create_dirs|.
  if (!manifest_->has_root_dir()) {
    InitNewAsset(absl::string_view(), AssetProto::DIRECTORY,
                 manifest_->mutable_root_dir());
  }
  return FindOrCreateDirPathRec(path, 0, manifest_->mutable_root_dir(),
                                create_dirs);
}

absl::StatusOr<AssetProto*> ManifestBuilder::FindOrCreateDirPathRec(
    const std::vector<absl::string_view>& path, size_t path_idx,
    AssetProto* dir, DirCreateMode create_dirs) {
  if (path_idx >= path.size()) return dir;
  absl::string_view name = path[path_idx];

  // Try to find the name in the direct assets.
  bool overwrite = create_dirs == DirCreateMode::kForceCreate;
  absl::StatusOr<AssetProto*> result = FindMutableAssetInList(
      name, AssetProto::DIRECTORY, overwrite, dir->mutable_dir_assets());
  if (result.ok()) {
    // Recurse into the sub-directory.
    return FindOrCreateDirPathRec(path, path_idx + 1, result.value(),
                                  create_dirs);
  }
  if (!absl::IsNotFound(result.status())) {
    // Return any unexpected error.
    return result;
  }

  // Try to find the name in the list of indirect assets.
  for (const ContentIdProto& asset_list_id : dir->dir_indirect_assets()) {
    AssetListProto* asset_list;
    ASSIGN_OR_RETURN(asset_list, GetAssetList(asset_list_id));
    // In theory it can happen that the loaded asset_list is empty, in which
    // case it is null.
    if (!asset_list) continue;
    result = FindMutableAssetInList(name, AssetProto::DIRECTORY, overwrite,
                                    asset_list->mutable_assets());
    if (result.ok()) {
      // Recurse into the sub-directory.
      return FindOrCreateDirPathRec(path, path_idx + 1, result.value(),
                                    create_dirs);
    }
    if (!absl::IsNotFound(result.status())) {
      // Return any unexpected error.
      return WrapStatus(result.status(),
                        "Failed to look up directory '%s' in AssetListProto %s",
                        name, ContentId::ToHexString(asset_list_id));
    }
  }

  // If we're not supposed to create the directory, return an error.
  if (create_dirs == DirCreateMode::kNoCreate) {
    return absl::NotFoundError(absl::string_view());
  }

  // Create the missing directory.
  AssetProto* child = dir->add_dir_assets();
  InitNewAsset(name, AssetProto::DIRECTORY, child);
  return FindOrCreateDirPathRec(path, path_idx + 1, child, create_dirs);
}

absl::StatusOr<AssetProto*> ManifestBuilder::FindAssetInDir(
    absl::string_view name, AssetProto* dir) {
  if (dir->type() != AssetProto::DIRECTORY) {
    return WrongAssetTypeError(dir->name(), dir->type(), AssetProto::DIRECTORY);
  }

  // Try to find the name in the direct assets.
  absl::StatusOr<AssetProto*> result =
      FindMutableAssetInList(name, dir->mutable_dir_assets());
  if (result.ok()) {
    return result.value();
  }
  if (!absl::IsNotFound(result.status())) {
    // Return any unexpected error.
    return result;
  }

  // Try to find the name in the list of indirect assets.
  for (const ContentIdProto& asset_list_id : dir->dir_indirect_assets()) {
    AssetListProto* asset_list;
    ASSIGN_OR_RETURN(asset_list, GetAssetList(asset_list_id),
                     "Failed to look up asset '%s' in directory '%s'", name,
                     dir->name());
    result = FindMutableAssetInList(name, asset_list->mutable_assets());
    if (result.ok()) {
      return result.value();
    }
    if (!absl::IsNotFound(result.status())) {
      // Return any unexpected error.
      return result;
    }
  }

  return absl::NotFoundError(absl::string_view());
}

absl::StatusOr<AssetProto*> ManifestBuilder::FindMutableAssetInList(
    absl::string_view name, RepeatedAssetProto* assets) const {
  for (AssetProto& asset : *assets) {
    if (asset.name() == name) return &asset;
  }
  return absl::NotFoundError(absl::string_view());
}

absl::StatusOr<AssetProto*> ManifestBuilder::FindMutableAssetInList(
    absl::string_view name, AssetProto::Type type, bool overwrite,
    RepeatedAssetProto* assets) const {
  AssetProto* asset;
  ASSIGN_OR_RETURN(asset, FindMutableAssetInList(name, assets));
  if (asset->type() != type) {
    // Return an error if the asset is not of the desired type and we're not
    // supposed to overwrite it.
    if (!overwrite) {
      return WrongAssetTypeError(asset->name(), asset->type(), type);
    }
    // Replace the asset with the new type.
    InitNewAsset(std::string(asset->name()), type, asset);
  }

  return asset;
}

absl::Status ManifestBuilder::DeleteAssetFromDir(absl::string_view name,
                                                 AssetProto* dir) {
  if (dir->type() != AssetProto::DIRECTORY) {
    return WrongAssetTypeError(dir->name(), dir->type(), AssetProto::DIRECTORY);
  }

  // Try to find the name in the direct assets.
  if (DeleteAssetFromList(name, dir->mutable_dir_assets())) {
    return absl::OkStatus();
  }

  // Try to find the name in the list of indirect assets.
  for (const ContentIdProto& asset_list_id : dir->dir_indirect_assets()) {
    AssetListProto* asset_list;
    ASSIGN_OR_RETURN(asset_list, GetAssetList(asset_list_id),
                     "Failed to look up asset '%s' in directory '%s'", name,
                     dir->name());
    if (DeleteAssetFromList(name, asset_list->mutable_assets())) {
      return absl::OkStatus();
    }
  }
  return absl::OkStatus();
}

bool ManifestBuilder::DeleteAssetFromList(absl::string_view name,
                                          RepeatedAssetProto* assets) const {
  for (int i = 0; i < assets->size(); ++i) {
    if (assets->at(i).name() == name) {
      // Move the asset to the end of the list, then remove it, to avoid all
      // other elements being moved.
      if (i != assets->size() - 1) {
        assets->SwapElements(i, assets->size() - 1);
      }
      assets->RemoveLast();
      return true;
    }
  }
  return false;
}

void ManifestBuilder::InitNewAsset(absl::string_view name,
                                   AssetProto::Type type,
                                   AssetProto* asset) const {
  asset->Clear();
  asset->set_name(name.data(), name.size());
  asset->set_type(type);
  asset->set_mtime_seconds(absl::ToUnixSeconds(absl::Now()));
  asset->set_permissions(type == AssetProto::DIRECTORY ? kDefaultDirPerms
                                                       : kDefaultFilePerms);
}

absl::StatusOr<AssetListProto*> ManifestBuilder::GetAssetList(
    const ContentIdProto& id) {
  // See if we loaded this proto already.
  AssetListMap::iterator it = asset_lists_.find(id);
  if (it != asset_lists_.end()) return it->second;
  // If not, we need to load it.
  AssetListProto* asset_list = MakeProto<AssetListProto>();
  RETURN_IF_ERROR(data_store_->GetProto(id, asset_list),
                  "Failed to read the AssetListProto with ID %s from storage",
                  ContentId::ToHexString(id));
  asset_lists_[id] = asset_list;
  return asset_list;
}

absl::StatusOr<AssetListProto*> ManifestBuilder::TakeOutAssetList(
    const ContentIdProto& id) {
  AssetListProto* list;
  ASSIGN_OR_RETURN(list, GetAssetList(id));
  asset_lists_.erase(id);
  return list;
}

absl::Status ManifestBuilder::WrongAssetTypeError(
    absl::string_view name, AssetProto::Type found,
    AssetProto::Type expected) const {
  return absl::InvalidArgumentError(absl::StrFormat(
      "Asset '%s' is of type %s, expected %s.", name,
      AssetProto::Type_Name(found), AssetProto::Type_Name(expected)));
}

size_t ManifestBuilder::ManifestBytesWritten() const {
  return manifest_bytes_written_;
}

size_t ManifestBuilder::ManifestsChunksWritten() const {
  return manifest_chunks_written_;
}

const ContentIdProto& ManifestBuilder::ManifestId() const {
  return manifest_id_;
}

const ManifestProto* ManifestBuilder::Manifest() const { return manifest_; }

const std::vector<ContentIdProto>& ManifestBuilder::FlushedContentIds() const {
  return flushed_content_ids_;
}

absl::Status ManifestBuilder::FlushDir(AssetProto* dir) {
  // Flush all direct assets.
  RETURN_IF_ERROR(FlushAssetList(dir->mutable_dir_assets()),
                  "Failed to flush directs assets of directory '%s'",
                  dir->name());

  RepeatedAssetProto overflow;
  RepeatedContentIdProto* indirect_assets = dir->mutable_dir_indirect_assets();

  // Flush all indirect asset lists that were previously loaded.
  RepeatedContentIdProto::iterator it = indirect_assets->begin();
  while (it != indirect_assets->end()) {
    ContentIdProto& asset_list_id = *it;
    // Skip any list that was never loaded.
    AssetListMap::iterator asset_list_it = asset_lists_.find(asset_list_id);
    if (asset_list_it == asset_lists_.end()) {
      ++it;
      continue;
    }
    AssetListProto* asset_list = asset_list_it->second;
    // Flush the list and enforce the chunk size limit.
    RETURN_IF_ERROR(FlushAssetList(asset_list->mutable_assets()),
                    "Failed to flush indirect asset list %s in directory '%s'",
                    ContentId::ToHexString(asset_list_id), dir->name());
    EnforceAssetListProtoSize(asset_list, &overflow);
    // If the asset list is empty, just delete it from the indirect asset list.
    if (asset_list->assets_size() <= 0) {
      it = indirect_assets->erase(it);
      continue;
    }
    // Write the list to the chunk store and update the content ID.
    RETURN_IF_ERROR(WriteProto(*asset_list, &asset_list_id),
                    "Failed to write indirect asset list proto for directory "
                    "'%s' to storage",
                    dir->name());
    // If the content ID changed, we need to update the list's key in the map.
    if (asset_list_it->first != asset_list_id) {
      AssetListProto* list = asset_list_it->second;
      asset_lists_.erase(asset_list_it);
      asset_lists_[asset_list_id] = list;
    }
    ++it;
  }

  // Enforce size limit for this DIRECTORY asset.
  RETURN_IF_ERROR(EnforceDirProtoSize(dir, &overflow));
  // Add the overflown assets to the indirect assets list.
  return AppendAllocatedIndirectAssets(dir, &overflow);
}

absl::Status ManifestBuilder::FlushAssetList(RepeatedAssetProto* assets) {
  // Flush all sub-directories.
  for (AssetProto& asset : *assets) {
    if (asset.type() == AssetProto::DIRECTORY)
      RETURN_IF_ERROR(FlushDir(&asset));
  }
  return absl::OkStatus();
}

inline void SortByProtoSizeDesc(RepeatedAssetProto* assets) {
  std::sort(assets->begin(), assets->end(),
            [](const AssetProto& a, const AssetProto& b) -> bool {
              // Compare greater than for descending order.
              return a.ByteSizeLong() > b.ByteSizeLong();
            });
}

absl::Status ManifestBuilder::EnforceDirProtoSize(
    AssetProto* dir, RepeatedAssetProto* overflow) {
  // A max. size of zero means no limit.
  const size_t max_size = manifest_->cdc_params().avg_chunk_size();
  if (!max_size) return absl::OkStatus();
  // We cannot change the size of non-directory assets.
  if (dir->type() != AssetProto::DIRECTORY) return absl::OkStatus();
  // Calculate the full proto size only once.
  size_t proto_size = dir->ByteSizeLong();
  if (proto_size <= max_size) return absl::OkStatus();
  // Sort asset list by size so that we start with the largest assets.
  SortByProtoSizeDesc(dir->mutable_dir_assets());
  // Enforce the size limit of large FILE assets, where "large" is defined as
  // 1/16th of the target chunk size.
  const size_t max_asset_proto_size = max_size >> 4;
  if (max_asset_proto_size) {
    for (AssetProto& asset : *dir->mutable_dir_assets()) {
      size_t asset_proto_size = asset.ByteSizeLong();
      // Stop if the remaining assets are no longer large.
      if (proto_size <= max_size || asset_proto_size <= max_asset_proto_size) {
        break;
      }
      if (asset.type() != AssetProto::FILE) continue;
      RETURN_IF_ERROR(EnforceFileProtoSize(&asset, max_asset_proto_size));
      // Adjust the directory proto size.
      proto_size = proto_size + asset.ByteSizeLong() - asset_proto_size;
    }
  }
  // Move assets to the overflow list until the limit is respected.
  while (dir->dir_assets_size() && proto_size > max_size) {
    // Use the UnsafeArena* function to avoid a heap copy of the message.
    AssetProto* asset = dir->mutable_dir_assets()->UnsafeArenaReleaseLast();
    proto_size -= asset->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    // When the estimates get us below the limit, calculate the accurate size.
    if (proto_size <= max_size) proto_size = dir->ByteSizeLong();
    overflow->UnsafeArenaAddAllocated(asset);
  }
  // At this point, we might still be over the size limit for a combination of
  // a very small chunk size and a very large directories. There's nothing we
  // can do about it with the current structure of the manifest proto.
  if (proto_size > max_size) {
    LOG_WARNING(
        "Manifest for directory '%s' is over the configured chunk size limit "
        "(%d > %d). Consider increasing the chunk size.",
        dir->name(), proto_size, max_size);
  }
  return absl::OkStatus();
}

absl::Status ManifestBuilder::EnforceFileProtoSize(
    AssetProto* file, size_t max_asset_proto_size) {
  if (!max_asset_proto_size) return absl::OkStatus();
  assert(file->type() == AssetProto::FILE);
  // If there is only a single direct chunk, we cannot reduce the proto size.
  if (file->file_chunks_size() <= 1) return absl::OkStatus();
  // We expect no indirect chunk lists at this point. If we ever decide to
  // "rebalance" existing manifests with a smaller chunk size, we need to push
  // the indirect chunks before the existing ones.
  if (file->file_indirect_chunks_size() > 0) {
    return MakeStatus(
        "Given asset '%s' already has %d indirect chunk lists which is not "
        "supported",
        file->name(), file->file_indirect_chunks_size());
  }
  std::deque<ChunkRefProto*> overflow;
  size_t proto_size = file->ByteSizeLong();
  // Remove chunks until the size limit is respected.
  while (file->file_chunks_size() && proto_size > max_asset_proto_size) {
    // Use the UnsafeArena* function to avoid a heap copy of the message.
    ChunkRefProto* ref = file->mutable_file_chunks()->UnsafeArenaReleaseLast();
    proto_size -= ref->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    // When the estimates get us below the limit, calculate the accurate size.
    if (proto_size <= max_asset_proto_size) proto_size = file->ByteSizeLong();
    overflow.push_back(ref);
  }
  if (overflow.empty()) return absl::OkStatus();

  // Move chunks to indirect chunk lists. All proto memory is owned by the
  // |arena_|, we don't need to worry about leaking memory here.
  ChunkListProto* chunk_list = MakeProto<ChunkListProto>();
  size_t chunk_list_size = 0;
  uint64_t chunk_list_offset = overflow.back()->offset();
  const size_t max_size = manifest_->cdc_params().avg_chunk_size();
  while (!overflow.empty()) {
    ChunkRefProto* chunk_ref = overflow.back();
    overflow.pop_back();
    // Convert the chunk's absolute offset to a relative one.
    uint64_t chunk_absolute_offset = chunk_ref->offset();
    chunk_ref->set_offset(chunk_absolute_offset - chunk_list_offset);
    size_t chunkref_proto_size =
        chunk_ref->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    // Write back a full chunk list and set offset and content ID accordingly.
    if (chunk_list_size > 0 &&
        chunk_list_size + chunkref_proto_size > max_size) {
      RETURN_IF_ERROR(WriteBackChunkList(chunk_list_offset, *chunk_list,
                                         file->add_file_indirect_chunks()));
      chunk_list->Clear();
      chunk_list_size = 0;
      // The first chunk in the list defines the chunk list's offset.
      chunk_list_offset = chunk_absolute_offset;
      chunk_ref->set_offset(0);
      chunkref_proto_size =
          chunk_ref->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    }
    // Move chunk reference to the indirect list. Use the UnsafeArena* function
    // again to pass ownership without copying the data.
    chunk_list->mutable_chunks()->UnsafeArenaAddAllocated(chunk_ref);
    chunk_list_size += chunkref_proto_size;
    // When the estimates get us above the limit, calculate the accurate size.
    if (chunk_list_size > max_size)
      chunk_list_size = chunk_list->ByteSizeLong();
  }
  // Write back final chunk list.
  return WriteBackChunkList(chunk_list_offset, *chunk_list,
                            file->add_file_indirect_chunks());
}

bool ManifestBuilder::EnforceAssetListProtoSize(
    AssetListProto* asset_list, RepeatedAssetProto* overflow) const {
  // A max. size of zero means no limit.
  const size_t max_size = manifest_->cdc_params().avg_chunk_size();
  if (!max_size) return false;
  size_t proto_size = asset_list->ByteSizeLong();
  bool changed = false;
  while (proto_size > max_size) {
    // Use the UnsafeArena* function to avoid a heap copy of the message.
    AssetProto* asset = asset_list->mutable_assets()->UnsafeArenaReleaseLast();
    proto_size -= asset->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    // When the estimates get us below the limit, calculate the accurate size.
    if (proto_size <= max_size) proto_size = asset_list->ByteSizeLong();
    overflow->UnsafeArenaAddAllocated(asset);
    changed = true;
  }
  return changed;
}

absl::Status ManifestBuilder::WriteBackAssetList(
    AssetListProto* asset_list, ContentIdProto* asset_list_id) {
  RETURN_IF_ERROR(WriteProto(*asset_list, asset_list_id),
                  "Failed to write back AssetListProto");
  asset_lists_[*asset_list_id] = asset_list;
  return absl::OkStatus();
}

absl::Status ManifestBuilder::WriteBackChunkList(
    uint64_t chunk_list_offset, const ChunkListProto& chunk_list,
    IndirectChunkListProto* indirect_chunk_list) {
  assert(chunk_list.chunks_size() > 0);
  RETURN_IF_ERROR(
      WriteProto(chunk_list, indirect_chunk_list->mutable_chunk_list_id()));
  indirect_chunk_list->set_offset(chunk_list_offset);
  return absl::OkStatus();
}

absl::Status ManifestBuilder::WriteProto(
    const google::protobuf::MessageLite& proto, ContentIdProto* content_id) {
  size_t proto_size = 0;
  RETURN_IF_ERROR(data_store_->PutProto(proto, content_id, &proto_size));
  flushed_content_ids_.push_back(*content_id);
  // Update stats.
  manifest_bytes_written_ += proto_size;
  ++manifest_chunks_written_;
  return absl::OkStatus();
}

absl::Status ManifestBuilder::AppendAllocatedIndirectAssets(
    AssetProto* dir, RepeatedAssetProto* assets) {
  if (assets->empty()) return absl::OkStatus();

  // The max. manifest chunk size that we try to stay under.
  const size_t max_size = manifest_->cdc_params().avg_chunk_size();
  // Use asset_list to track the last allocated list, if any.
  AssetListProto* asset_list = nullptr;
  // Index to the indirect asset list within |dir| currently in use. Defaults to
  // zero, which means that if |dir| does not have any indirect asset lists, the
  // code below will create the first one and store it at index zero.
  int asset_list_index = 0;
  // Approximate byte size of the asset list proto currently in use. This size
  // is updated with the byte size of any asset proto that is appended to the
  // list, but ignores any overhead from the embedding proto format (which
  // should be negliable).
  size_t proto_size = 0;

  // Find or create the AssetListProto where we can append the assets.
  if (dir->dir_indirect_assets_size() > 0) {
    // Load the last indirect asset list and see if we can append to it.
    asset_list_index = dir->dir_indirect_assets_size() - 1;
    const ContentIdProto& asset_list_id =
        dir->dir_indirect_assets(asset_list_index);
    // Take out the asset list from its original location since the content ID
    // will be updated anyway once we append more assets to it.
    ASSIGN_OR_RETURN(asset_list, TakeOutAssetList(asset_list_id));
    proto_size = asset_list->ByteSizeLong();
  } else {
    // Add the first indirect asset to |dir|, asset_list_index is already
    // initialized to zero.
    dir->add_dir_indirect_assets();
    asset_list = MakeProto<AssetListProto>();
  }

  while (!assets->empty()) {
    // Use the UnsafeArena* function to avoid a heap copy of the message. Even
    // though it is released from the proto, the memory is still owned by the
    // |arena_| and shares its lifetime.
    AssetProto* asset = assets->UnsafeArenaReleaseLast();
    size_t asset_proto_size =
        asset->ByteSizeLong() + kRepeatedProtoFieldOverhead;
    // See if we need to create a new AssetListProto.
    if (max_size > 0 && proto_size > 0 &&
        proto_size + asset_proto_size > max_size) {
      // Write back the full list to the data store.
      RETURN_IF_ERROR(
          WriteBackAssetList(
              asset_list, dir->mutable_dir_indirect_assets(asset_list_index)),
          "Failed to write back asset list for directory '%s'", dir->name());
      // Create a new list.
      asset_list = MakeProto<AssetListProto>();
      proto_size = 0;
      asset_list_index = dir->dir_indirect_assets_size();
      dir->add_dir_indirect_assets();
    }
    // Append the allocated asset to the current list.
    asset_list->mutable_assets()->UnsafeArenaAddAllocated(asset);
    proto_size += asset_proto_size;
  }

  // Write back the final asset list.
  RETURN_IF_ERROR(
      WriteBackAssetList(asset_list,
                         dir->mutable_dir_indirect_assets(asset_list_index)),
      "Failed to write back final asset list for directory '%s'", dir->name());
  return absl::OkStatus();
}

absl::StatusOr<ContentIdProto> ManifestBuilder::Flush() {
  manifest_bytes_written_ = 0;
  manifest_chunks_written_ = 0;
  flushed_content_ids_.clear();
  if (!manifest_->has_root_dir()) {
    InitNewAsset("", AssetProto::DIRECTORY, manifest_->mutable_root_dir());
  }
  RETURN_IF_ERROR(FlushDir(manifest_->mutable_root_dir()));
  RETURN_IF_ERROR(WriteProto(*manifest_, &manifest_id_));
  return manifest_id_;
}

ManifestBuilder::FileLookupMap ManifestBuilder::CreateFileLookup() {
  std::unordered_map<std::string, AssetProto*> lookup;
  CreateFileLookupRec(std::string(), manifest_->mutable_root_dir(), lookup);
  return lookup;
}

void ManifestBuilder::CreateFileLookupRec(const std::string& rel_path,
                                          AssetProto* asset,
                                          FileLookupMap& lookup) {
  std::string rel_file_path = path::JoinUnix(rel_path, asset->name());
  if (asset->type() == AssetProto::FILE) {
    lookup[rel_file_path] = asset;
    return;
  }

  // Handle all direct assets.
  for (AssetProto& child : *asset->mutable_dir_assets())
    CreateFileLookupRec(rel_file_path, &child, lookup);

  // Add all (loaded!) indirect assets as well.
  for (const ContentIdProto& id : asset->dir_indirect_assets()) {
    const auto iter = asset_lists_.find(id);
    if (iter == asset_lists_.end()) continue;
    AssetListProto* asset_list = iter->second;
    assert(asset_list);
    for (AssetProto& child : *asset_list->mutable_assets())
      CreateFileLookupRec(rel_file_path, &child, lookup);
  }
}

// Returns used CDC parameters
CdcParamsProto ManifestBuilder::CdcParameters() const {
  return manifest_->cdc_params();
}

template <typename T>
T* ManifestBuilder::MakeProto() {
  return google::protobuf::Arena::CreateMessage<T>(&arena_);
}

}  // namespace cdc_ft
