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

#ifndef MANIFEST_ASSET_BUILDER_H_
#define MANIFEST_ASSET_BUILDER_H_

#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

class AssetBuilder {
 public:
  AssetBuilder();

  // Creates a new AssetBuilder referencing the given |proto| and relative path
  // |rel_path|. Ownership of |proto| remains with the caller and must remain
  // valid while the AssetBuilder is being used.
  AssetBuilder(AssetProto* proto, const std::string& rel_path);
  ~AssetBuilder();

  // The assignment operator ignores the constant member |empty_|.
  AssetBuilder& operator=(const AssetBuilder& other);

  // Returns the modification timestamp of this asset.
  uint64_t MtimeSeconds() const { return proto_ ? proto_->mtime_seconds() : 0; }

  // Sets the modification timestamp of this asset to |mtime|.
  void SetMtimeSeconds(uint64_t mtime) {
    if (proto_) proto_->set_mtime_seconds(mtime);
  }

  // Returns the permission bits of this asset (RWX for user, group, world, in
  // that order).
  uint32_t Permissions() const { return proto_ ? proto_->permissions() : 0; }

  // Sets the permission bits of this asset to |perms|.
  void SetPermissions(uint32_t perms) {
    if (proto_) proto_->set_permissions(perms);
  }

  // Returns the file name of this asset.
  const std::string& Name() const { return proto_ ? proto_->name() : empty_; }

  // Returns the asset type.
  AssetProto::Type Type() const {
    return proto_ ? proto_->type() : AssetProto::UNKNOWN;
  }

  // Returns the Unix path of the directory containing this asset relative to
  // the manifest root directory, as specified during construction or
  // SetProto().
  const std::string& RelativePath() const { return rel_path_; }

  // Returns the path and file name of this asset relative to the manifest root
  // directory.
  std::string RelativeFilePath() const;

  // Returns this asset's in_progress status.
  bool InProgress() const;

  // Sets the asset's in_progress status.
  void SetInProgress(bool in_progress);

  // For FILE assets, appends the chunk with the given |content_id| and |len| to
  // the list of chunks. The chunk's offset will be auto-determined based on the
  // current file size.
  //
  // Asserts that the asset is actually of type FILE and that the file does not
  // have any associated indirect chunk lists.
  void AppendChunk(const ContentIdProto& content_id, size_t len);

  // For FILE assets, removes all chunks from this file and resets the file size
  // to zero.
  //
  // Asserts that the asset is actually of type FILE.
  void TruncateChunks();

  // Sets this file's chunks from the ones given in the provided |chunks| list
  // and the total size to |file_size|. Copies the proto contents, clears all
  // indirect chunk lists.
  //
  // Asserts that the asset is actually of type FILE.
  void SetChunks(const RepeatedChunkRefProto& chunks, uint64_t file_size);

  // Swaps this file's chunks with the ones given in the provided |chunks| list
  // and sets the total size to |file_size|. This avoids copying the data.
  // Clears all indirect chunk lists.
  //
  // Asserts that the asset is actually of type FILE.
  void SwapChunks(RepeatedChunkRefProto* chunks, uint64_t file_size);

  // Sets this file's size.
  //
  // Asserts that the asset is actually of type FILE.
  void SetFileSize(uint64_t file_size);

  // For DIRECTORY assets, adds a new direct asset to the end of the list. Does
  // *not* verify if an asset with that name already exists.
  //
  // Asserts that the asset is actually of type DIRECTORY.
  AssetBuilder AppendAsset(const std::string& name, AssetProto::Type type);

  // Returns the symlink target for symlinks.
  const std::string& SymlinkTarget() const {
    return proto_ ? proto_->symlink_target() : empty_;
  }

  // Sets the target for symlinks.
  void SetSymlinkTarget(const std::string& target) {
    if (proto_) proto_->set_symlink_target(target);
  }

  // Returns a pointer to the proto that this AssetBuilder references.
  const AssetProto* Proto() const { return proto_; }
  AssetProto* Proto() { return proto_; }

  // Sets the |proto| and relative path |rel_path| this AssetBuilder is
  // referring to. Ownership of |proto| remains with the caller and must remain
  // valid while the AssetBuilder is being used.
  void SetProto(AssetProto* proto, const std::string& rel_path);

 private:
  // Resets this AssetBuilder.
  void Clear();

  // Empty string to return as reference when no proto is set.
  const std::string empty_;

  // The proto this AssetBuilder refers to.
  AssetProto* proto_ = nullptr;

  // The path leading to this asset relative to the manfest root.
  std::string rel_path_;
};

}  // namespace cdc_ft

#endif  // MANIFEST_ASSET_BUILDER_H_
