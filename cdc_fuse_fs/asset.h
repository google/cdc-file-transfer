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

#ifndef CDC_FUSE_FS_ASSET_H_
#define CDC_FUSE_FS_ASSET_H_

#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "manifest/content_id.h"

namespace cdc_ft {

class Buffer;
class DataStoreReader;

// Wraps an asset proto for reading and adds additional functionality like name
// lookup maps and lazy loading of directory assets and file chunks.
// This class is accessed from multiple threads and has to be THREAD-SAFE.
class Asset {
 public:
  // Inode key type (cmp. fuse_ino_t).
  using ino_t = uint64_t;

  // Creates a new asset. Must call Initialize() before using it.
  Asset();
  ~Asset();

  // Make it non-copyable, non-assignable to prevent accidental misuse.
  Asset(const Asset& other) = delete;
  Asset& operator=(const Asset& other) = delete;

  // Initialize the class. Must be called right after creation.
  // NOT thread-safe! (OK as usually no other threads have access at this time.)
  void Initialize(ino_t parent_ino, DataStoreReader* data_store_reader,
                  const AssetProto* proto);

  // Returns the parent inode id passed to Initialize().
  // Thread-safe.
  ino_t parent_ino() const { return parent_ino_; }

  // Returns the asset proto passed to Initialize().
  // Thread-safe.
  const AssetProto* proto() const { return proto_; }

  // Returns all child asset protos. Loads them if necessary.
  // Returns an error if loading an indirect asset list fails.
  // Returns an InvalidArugmentError if *this is not a directory asset.
  // |proto_| must be set.
  // Thread-safe.
  absl::StatusOr<std::vector<const AssetProto*>> GetAllChildProtos()
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns loaded children's protos. Thread-safe.
  std::vector<const AssetProto*> GetLoadedChildProtos() const;

  // For directory assets, looks up a child asset by name. Loads indirect asset
  // lists if needed. Returns an error if loading asset lists fails.
  // Returns nullptr if the asset cannot be found.
  // Returns an InvalidArugmentError if *this is not a directory asset.
  // |proto_| must be set.
  // Thread-safe.
  absl::StatusOr<const AssetProto*> Lookup(const char* name)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // For file assets, reads |size| bytes from the file, starting from |offset|,
  // and puts the result into |data|. Returns the number of bytes read or 0 if
  // |offset| >= file size. Loads indirect chunk lists if needed.
  // Returns an error if loading chunk lists fails.
  // Returns an InvalidArugmentError if *this is not a file asset.
  // |proto_| must be set.
  // Thread-safe.
  absl::StatusOr<uint64_t> Read(uint64_t offset, void* data, uint64_t size);

  size_t GetNumFetchedFileChunkListsForTesting() ABSL_LOCKS_EXCLUDED(mutex_);
  size_t GetNumFetchedDirAssetsListsForTesting() ABSL_LOCKS_EXCLUDED(mutex_);

  // Updates asset proto, all corresponding internal structures are cleaned up.
  // This is an expensive operation as the previously created internal
  // structures are removed. Thread-safe.
  void UpdateProto(const AssetProto* proto) ABSL_LOCKS_EXCLUDED(mutex_);

  // Checks consistency of the asset, for example: directory assets should not
  // contain any file chunks. Any discovered inconsistencies are defined in
  // |warning|.
  bool IsConsistent(std::string* warning) const ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  // Loads the next indirect directory asset list.
  // Returns true if a list was fetched.
  // Returns false if all lists have already been fetched.
  // Returns an error if fetching an indirect asset list failed.
  // |proto_| must be set.
  absl::StatusOr<bool> FetchNextDirAssetList() ABSL_LOCKS_EXCLUDED(mutex_);

  // Puts all assets from |list| into |proto_lookup_|.
  void UpdateProtoLookup(const RepeatedAssetProto& list)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Returns the index of the indirect chunk list that |offset| falls into or -1
  // if |offset| is contained in the direct chunk list. Returns the number of
  // indirect lists in |proto_| if |offset| is larger or equal to the file size.
  // |proto_| must be set.
  int FindChunkList(uint64_t offset);

  // Returns the index of the chunk that |chunk_offset| falls into. The offsets
  // in list |chunks| are interpreted relative to |chunk_list_offset|.
  int FindChunk(const RepeatedChunkRefProto& chunks, uint64_t chunk_list_offset,
                uint64_t chunk_offset);

  // Gets the direct or an indirect chunk list. Fetches indirect chunk lists if
  // necessary. |list_idx| must be in [-1, number of indirect chunk lists].
  //
  // Returns the direct chunk list if |list_idx| is -1. Returns nullptr if
  // |list_idx| equals the number of indirect chunk lists. Returns the indirect
  // chunk list at index |list_idx| otherwise. Returns an error if fetching an
  // indirect chunk list fails.
  // |proto_| must be set.
  absl::StatusOr<const RepeatedChunkRefProto*> GetChunkRefList(int list_idx)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the absolute offset of the chunk list with index |list_idx|.
  // |list_idx| must be in [-1, number of indirect chunk lists]. -1 refers to
  // the direct chunk list, in which case 0 is returned. If |list_idx| equals
  // the number of indirect chunk lists, the file size is returned. Otherwise,
  // the corresponding indirect chunk list's offset is returned.
  // |proto_| must be set.
  uint64_t ChunkListOffset(int list_idx) const;

  // Returns the chunk size of the chunk with index |chunk_idx| on the chunk
  // list with index |list_idx| and corresponding proto |chunk_refs|.
  // |list_idx| must be in [-1, number of indirect chunk lists - 1].
  // |chunk_idx| must be in [0, chunk_refs->size()].
  // |proto_| must be set.
  uint64_t ChunkSize(int list_idx, int chunk_idx,
                     const RepeatedChunkRefProto* chunk_refs);

  // Parent inode, for ".." in dir listings.
  ino_t parent_ino_ = 0;

  // Interface for loading content (chunks, assets).
  DataStoreReader* data_store_reader_ = nullptr;

  // Corresponding asset proto.
  const AssetProto* proto_ = nullptr;

  // RW mutex for increased thread-safetiness.
  mutable absl::Mutex mutex_;

  // Maps asset proto names to asset protos for all protos loaded so far.
  // The string views point directly into asset protos.
  std::unordered_map<absl::string_view, const AssetProto*> proto_lookup_
      ABSL_GUARDED_BY(mutex_);

  // Fetched |file_indirect_chunks| chunk lists.
  std::vector<std::unique_ptr<ChunkListProto>> file_chunk_lists_
      ABSL_GUARDED_BY(mutex_);

  // Fetched |dir_indirect_assets| fields so far.
  std::vector<std::unique_ptr<AssetListProto>> dir_asset_lists_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_ASSET_H_
