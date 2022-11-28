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

#ifndef MANIFEST_MANIFEST_BUILDER_H_
#define MANIFEST_MANIFEST_BUILDER_H_

#include <cstddef>
#include <list>

#include "absl/status/statusor.h"
#include "data_store/data_store_writer.h"
#include "google/protobuf/arena.h"
#include "manifest/asset_builder.h"
#include "manifest/content_id.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// The ManifestBuilder class is used to create a manifest proto for the assets
// (DIRECTORY, FILE, and SYMLINK) that are added incrementally. The proto is
// finalized with a call to Flush(). When the CdcParamsProto given during
// construction specifies an average chunk size, then the manifest will be split
// into balanced chunks of at most this size.
class ManifestBuilder {
 public:
  // Default permission bits for new directories and files, respectively.
  static constexpr uint32_t kDefaultDirPerms = 0755u;
  static constexpr uint32_t kDefaultFilePerms = 0644u;

  // Maps relative Unix file paths to the corresponding file asset proto.
  using FileLookupMap = std::unordered_map<std::string, AssetProto*>;

  // Creates a new builder which reads from/writes to the given |data_store|.
  // The |cdc_params| are included in the resulting manifest proto and influence
  // the size of the manifest chunks which are written back to the
  // |chunk_store|.
  ManifestBuilder(CdcParamsProto cdc_params, DataStoreWriter* data_store);
  ~ManifestBuilder();

  // Loads the manifest identified by |manifest_id| from the data store. Returns
  // an absl::NotFoundError if the manifest ID does not exist or other errors if
  // is not a valid manifest proto.
  absl::Status LoadManifest(const ContentIdProto& manifest_id);

  // Loads the manifest identified by the hexadecimal representation
  // |manifest_hex_id| from the data store. Returns an error if the string
  // representation is invalid or if the manifest ID does not exist or is not a
  // valid manifest proto.
  absl::Status LoadManifest(const std::string& manifest_hex_id);

  // Returns the asset identified by the given Windows or Unix |path| or creates
  // a new one of type |type| if it does not exist yet. The |path| is relative
  // to the manifest's root directory. If the asset is created, any missing
  // directories in |path| that lead up to the asset are automatically
  // created as DIRECTORY assets with default permissions. Use a DIRECTORY
  // |type| with an empty |path| to retrieve the root directory asset.
  //
  // If an asset at |path| exists but is of different |type|, the outcome
  // depends on |force_create|. If this is set to false (the default), an
  // absl::AlreadyExistsError is returned. If it is set to true, the existing
  // asset is removed (recursively for directories) and a new asset with the
  // same name is created instead.
  //
  // When |type| is UNKNOWN, an existing assets of any type is returned, no new
  // asset is created when it does not exist, nor are any of the directories
  // that lead up to that asset.
  //
  // When |created| is given, then it will be set to true if that asset was
  // actually added, otherwise it will be set to false.
  absl::StatusOr<AssetBuilder> GetOrCreateAsset(const std::string& path,
                                                AssetProto::Type type,
                                                bool force_create = false,
                                                bool* created = nullptr);

  // Deletes the asset with the given |path|. If the asset is of type DIRECTORY,
  // the entire directory is deleted recursively. If no asset with this path
  // exists, the function returns success.
  absl::Status DeleteAsset(const std::string& path);

  // Updates the manifest to reflect all changes that were done. Splits the
  // manifest into chunks of sizes as specified by the CdcParamsProto given
  // during construction.
  //
  // Calling this function might invalidate pointers to wrapped protos that were
  // returned by GetOrCreateAsset() or AssetBuilder methods.
  absl::StatusOr<ContentIdProto> Flush();

  // Creates a lookup of relative Unix file paths to protos of all loaded
  // protos. The lookup does not contain unloaded indirect dir assets.
  FileLookupMap CreateFileLookup();

  // Returns the content ID of the manifest which was valid after the last call
  // to Flush().
  const ContentIdProto& ManifestId() const;

  // Gets the manifest proto which was valid after the last call to Flush().
  const ManifestProto* Manifest() const;

  // Returns a list of the content IDs of all manifest chunks that have been
  // written back to the data store during the last call of Flush().
  const std::vector<ContentIdProto>& FlushedContentIds() const;

  // Access statistics after Flush() about the manifest that was built.
  size_t ManifestBytesWritten() const;
  size_t ManifestsChunksWritten() const;

  // Returns used CDC parameters
  CdcParamsProto CdcParameters() const;

 private:
  // Map for storing loaded AssetListProtos by content ID. The protos are
  // allocated on the arena which owns the memory.
  using AssetListMap = std::unordered_map<ContentIdProto, AssetListProto*>;

  // Clears all loaded and/or changed data and resets the statictics.
  void Reset();

  // Decides if and how directories are created.
  enum class DirCreateMode {
    // No directories are created and absl::NotFoundError might be returned.
    kNoCreate,
    // Missing directories are created, but absl::InvalidArgumentError might be
    // returned in case a non-directory asset with the same name exists.
    kCreate,
    // Missing directories are created, any asset of a different type will be
    // replaced with a DIRECTORY asset.
    kForceCreate
  };

  // Follows the given |path| components along DIRECTORY assets and returns the
  // final DIRECTORY on success.
  //
  // |create_dirs| determines if and when any missing DIRECTORY asset along the
  // way are created and what errors can be expected.
  absl::StatusOr<AssetProto*> FindOrCreateDirPath(
      const std::vector<absl::string_view>& path, DirCreateMode create_dirs);
  absl::StatusOr<AssetProto*> FindOrCreateDirPathRec(
      const std::vector<absl::string_view>& path, size_t path_idx,
      AssetProto* dir, DirCreateMode create_dirs);

  // Searches for an asset with the given |name| in the given DIRECTORY asset.
  // Does not recurse into sub-directories. If no such asset is found, an
  // absl::NotFoundError is returned.
  absl::StatusOr<AssetProto*> FindAssetInDir(absl::string_view name,
                                             AssetProto* dir);

  // Searches for an asset by its |name| in the given list of |assets|. If no
  // such asset is found, an absl::NotFoundError is returned.
  absl::StatusOr<AssetProto*> FindMutableAssetInList(
      absl::string_view name, RepeatedAssetProto* assets) const;

  // Searches for an asset by its |name| and |type| in the given list of assets.
  // If no such asset is found, an absl::NotFoundError is returned.
  //
  // If an asset with that name exists of a different type, the outcome is
  // conditional on |overwrite|. If |overwrite| is true, then the existing
  // asset's type will be replaced with the given type and the asset is
  // returned. If |overwrite| is false, an absl::InvalidArgumentError is
  // returned.
  absl::StatusOr<AssetProto*> FindMutableAssetInList(
      absl::string_view name, AssetProto::Type type, bool overwrite,
      RepeatedAssetProto* assets) const;

  // Deletes an asset with the given |name| in the given DIRECTORY asset. Does
  // not recurse into sub-directories. If no such asset is found, success is
  // returned.
  absl::Status DeleteAssetFromDir(absl::string_view name, AssetProto* dir);

  // Deletes an asset by its |name| in the given list of |assets|. Returns true
  // if the asset was found and deleted, false otherwise.
  bool DeleteAssetFromList(absl::string_view name,
                           RepeatedAssetProto* assets) const;

  // Initializes the given empty asset as an asset of the given |type| with
  // default values for permissions and timestamps. Does not clear the proto or
  // reset any other fields.
  void InitNewAsset(absl::string_view name, AssetProto::Type type,
                    AssetProto* asset) const;

  // Retrieves the AssetListProto referenced by the given content |id|. If the
  // proto has been previously loaded, the stored (and potentially modified)
  // proto is returned. Otherwise, the proto is read from the chunk store.
  absl::StatusOr<AssetListProto*> GetAssetList(const ContentIdProto& id);

  // Like GetAssetList(), but removes the AssetListProto from the |asset_lists_|
  // mapping.
  absl::StatusOr<AssetListProto*> TakeOutAssetList(const ContentIdProto& id);

  // Convenience wrapper function for returning an error that the asset with the
  // given |name| did not match the |expected| asset type.
  absl::Status WrongAssetTypeError(absl::string_view name,
                                   AssetProto::Type found,
                                   AssetProto::Type expected) const;

  // Flushes all pending information for |dir| and all sub-directories, enforces
  // the chunk size limit, updates the content IDs, and writes the chunks to the
  // chunk store.
  absl::Status FlushDir(AssetProto* dir);

  // Flushes all DIRECTORY assets in the given list recursively.
  absl::Status FlushAssetList(RepeatedAssetProto* assets);

  // Enforces the chunk size limit for the given DIRECTORY asset |dir|. Any
  // direct asset that does not fit is moved to the |overflow| list. Returns
  // true if at least one asset was moved, otherwise returns false.
  absl::Status EnforceDirProtoSize(AssetProto* dir,
                                   RepeatedAssetProto* overflow);

  // Enforces the chunk size limit for the given FILE asset |file| to be at most
  // |max_size|. Any chunk that does not fit is moved to the file's indirect
  // chunk list.
  absl::Status EnforceFileProtoSize(AssetProto* file, size_t max_size);

  // Enforces the chunk size limit for the given |asset_list|. Any asset that
  // does no longer fit is moved to the |overflow| list. Returns true if at
  // least one asset was moved, otherwise returns false.
  bool EnforceAssetListProtoSize(AssetListProto* asset_list,
                                 RepeatedAssetProto* overflow) const;

  // Appends the given list of allocated |assets| to the DIRECTORY asset |dir|.
  // Ownership of the items in |assets| is passed on to |dir|.
  absl::Status AppendAllocatedIndirectAssets(AssetProto* dir,
                                             RepeatedAssetProto* assets);

  // Writes the given AssetListProto to storage and updates |asset_list_id| with
  // the list's content ID. If the call succeeds, the |asset_lists_| map is
  // updated such that the resulting |asset_list_id| is referencing the
  // |asset_list|.
  absl::Status WriteBackAssetList(AssetListProto* asset_list,
                                  ContentIdProto* asset_list_id);

  // Writes the given ChunkListProto |chunk_list| to storage and updates
  // |indirect_chunk_list| with the given |chunk_list_offset| and the resulting
  // content ID.
  absl::Status WriteBackChunkList(uint64_t chunk_list_offset,
                                  const ChunkListProto& chunk_list,
                                  IndirectChunkListProto* indirect_chunk_list);

  // Wrapper around ChunkStore::WriteProto() which keeps track of chunks and
  // bytes written.
  absl::Status WriteProto(const google::protobuf::MessageLite& proto,
                          ContentIdProto* content_id);

  // Recursively iterates assets, adding all loaded file protos into |lookup|.
  // |rel_path| is the relative Unix directory path containing the |asset|.
  void CreateFileLookupRec(const std::string& rel_path, AssetProto* asset,
                           FileLookupMap& lookup);

  // Convenient wrapper to allocate a proto message on the arena.
  template <typename T>
  T* MakeProto();

  // Constant overhead in bytes per repeated proto field.
  static constexpr size_t kRepeatedProtoFieldOverhead = 2;

  // Data store to read and write manifest chunks.
  DataStoreWriter* data_store_;

  // Content ID of the resulting manifest, updated in Flush().
  ContentIdProto manifest_id_;

  // Content IDs of all manifest chunks that were written back to the data store
  // during the last call of Flush().
  std::vector<ContentIdProto> flushed_content_ids_;

  // Holds the manifest proto under construction.
  ManifestProto* manifest_ = nullptr;

  // CDC params used for the manifest.
  CdcParamsProto cdc_params_;

  // List of AssetListProtos loaded from data_store_.
  AssetListMap asset_lists_;

  // Useful stats.
  size_t manifest_bytes_written_ = 0;
  size_t manifest_chunks_written_ = 0;

  // Arena for protos allocated by this builder.
  google::protobuf::Arena arena_;
};

}  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_BUILDER_H_
