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

#ifndef MANIFEST_FILE_CHUNK_MAP_H_
#define MANIFEST_FILE_CHUNK_MAP_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "manifest/content_id.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

class StatsPrinter;

// A file chunk, used by the FileChunkMap.
struct FileChunk {
  // Id of the chunk.
  ContentIdProto content_id;

  // Absolute offset of the chunk in the file.
  uint64_t offset = 0;

  FileChunk(ContentIdProto content_id, uint64_t offset)
      : content_id(std::move(content_id)), offset(offset) {}
};

// Manages chunk lookups by content id. The class can be populated by passing it
// to ManifestUpdater and then used to look up chunks by calling Lookup().
class FileChunkMap {
 public:
  // If |enable_stats| is true, keeps detailed statistics on chunk access
  // patterns.
  explicit FileChunkMap(bool enable_stats);
  ~FileChunkMap();

  FileChunkMap(FileChunkMap&) = delete;
  FileChunkMap& operator=(FileChunkMap&) = delete;

  // Initializes a new entry for |path| or clears the existing one and sets the
  // |file_size|. If |chunks| is not null, moves the contents of |chunks| to
  // this file's chunk list.
  void Init(std::string path, uint64_t file_size,
            std::vector<FileChunk>* chunks = nullptr);

  // Appends the chunks in |list| to the entry for |path|. |list_offset| is
  // added to all chunk offsets in |list|. Copies ContentIdProtos from the list.
  // The operation is queued and gets applied by calling FlushUpdates().
  void AppendCopy(std::string path, const RepeatedChunkRefProto& list,
                  uint64_t list_offset);

  // Same as above, but modifies |list| by moving ContentIdProtos off the list.
  // The operation is queued and gets applied by calling FlushUpdates().
  void AppendMove(std::string path, RepeatedChunkRefProto* list,
                  uint64_t list_offset);

  // Removes the entry for |path|.
  // The operation is queued and gets applied by calling FlushUpdates().
  void Remove(std::string path);

  // Clears all entries.
  // The operation is queued and gets applied by calling FlushUpdates().
  void Clear();

  // Flushes all updates made by the above functions.
  void FlushUpdates() ABSL_LOCKS_EXCLUDED(mutex_);

  // Looks up the file |path|, the chunk |offset| and chunk |size| by the given
  // |content_id|. Returns false if the entry does not exist.
  bool Lookup(const ContentIdProto& content_id, std::string* path,
              uint64_t* offset, uint32_t* size) ABSL_LOCKS_EXCLUDED(mutex_);

  // Records that a chunk with the given |content_id| was streamed from the
  // workstation.
  // |thread_id| is the id of the thread that requested the chunk on the
  // gamelet, usually the hash of the std::thread::id.
  // No-op if |enable_stats| was false in the constructor.
  void RecordStreamedChunk(const ContentIdProto& content_id, size_t thread_id)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Records that a chunk with the given |content_id| is cached on the gamelet.
  // No-op if |enable_stats| was false in the constructor.
  void RecordCachedChunk(const ContentIdProto& content_id)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Prints detailed chunk statistics.
  // No-op if |enable_stats| was false in the constructor.
  void PrintStats() ABSL_LOCKS_EXCLUDED(mutex_);

  bool HasStats() const;

 private:
  struct File {
    // All chunks in the file.
    std::vector<FileChunk> chunks;

    // Total file size.
    uint64_t size = 0;
  };

  enum class FileUpdateType { kInit, kAppend, kRemove, kClear };

  struct FileUpdate {
    FileUpdateType type = FileUpdateType::kInit;
    std::string path;
    uint64_t file_size = 0;
    std::vector<FileChunk> chunks;

    FileUpdate(FileUpdateType type, std::string path)
        : type(type), path(std::move(path)) {}
  };

  struct ChunkLocation {
    // Asset path, also key into |path_to_file_| map.
    const std::string* path = nullptr;

    // Index into |path_to_file_[*path].chunks|.
    uint32_t index = 0;
  };

  // Keeps a pointer to a content id proto and compares by value.
  struct ContentIdRef {
    const ContentIdProto* content_id;

    explicit ContentIdRef(const ContentIdProto& content_id)
        : content_id(&content_id) {}

    bool operator==(const ContentIdRef& other) const {
      return *content_id == *other.content_id;
    }
    bool operator!=(const ContentIdRef& other) const {
      return !(*this == other);
    }
  };

  struct ContentIdRefHash {
    std::size_t operator()(const ContentIdRef& ref) const noexcept {
      return hash(*ref.content_id);
    }
    std::hash<ContentIdProto> hash;
  };

  // Updates |id_to_chunk_|. Also rebuilds |stats_| if present.
  void UpdateIdToChunkMap() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Finds a chunk its by |content_id|.
  // |path| returns the relative Unix path of a file that contains the chunk.
  // |offset| returns the offset of the chunk in the file.
  // |size| returns the size of the chunk.
  // |index| returns the index of the chunk in the File struct.
  // All output variables are optional.
  // Calls MaybeUpdateIdToChunkMap().
  // Returns true if the chunk was found.
  bool FindChunk(const ContentIdProto& content_id, std::string* path,
                 uint64_t* offset, uint32_t* size, size_t* index)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Queued updates.
  std::vector<FileUpdate> file_updates_;

  // Maps the relative Unix path of assets to its file size and chunks.
  using PathToFileMap = absl::flat_hash_map<std::string, File>;
  PathToFileMap path_to_file_ ABSL_GUARDED_BY(mutex_);

  // Maps content id to path and chunk index.
  using IdToChunkMap =
      absl::flat_hash_map<ContentIdRef, ChunkLocation, ContentIdRefHash>;
  IdToChunkMap id_to_chunk_ ABSL_GUARDED_BY(mutex_);

  size_t total_chunks_ ABSL_GUARDED_BY(mutex_) = 0;

  // Keeps detailed chunk access statistics.
  // Only used if |enable_stats| was set to true in the constructor.
  std::unique_ptr<StatsPrinter> stats_ ABSL_GUARDED_BY(mutex_);

  // All chunks streamed from/cached on the gamelet.
  // The data is used to rebuild stats in case of a the manifest update.
  // Only used if |enable_stats| was set to true in the constructor.
  absl::flat_hash_map<ContentIdProto, size_t> streamed_chunks_to_thread_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_set<ContentIdProto> cached_chunks_ ABSL_GUARDED_BY(mutex_);

  mutable absl::Mutex mutex_;
};

};  // namespace cdc_ft

#endif  // MANIFEST_FILE_CHUNK_MAP_H_
