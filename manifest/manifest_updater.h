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

#ifndef MANIFEST_MANIFEST_UPDATER_H_
#define MANIFEST_MANIFEST_UPDATER_H_

#include <list>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "common/buffer.h"
#include "manifest/asset_builder.h"
#include "manifest/file_chunk_map.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {
namespace fastcdc {
struct Config;
}

class AssetBuilder;
class DataStoreWriter;
class DirScannerTask;
class FileChunkerTask;
class ManifestBuilder;
class Threadpool;

struct UpdaterConfig {
  // Source directory from which to build the manifest from recursively.
  std::string src_dir;

  // Minimum allowed chunk size.
  size_t min_chunk_size = 128 << 10;

  // Target average chunk size.
  size_t avg_chunk_size = 256 << 10;

  // Maximum allowed chunk size.
  size_t max_chunk_size = 1024 << 10;

  // Size of the chunker thread pool. Defaults to the number of available CPUs.
  uint32_t num_threads = 0;
};

struct UpdaterStats {
  // Total no. of assets that were added or updated.
  size_t total_assets_added_or_updated = 0;

  // Total no. of assets of type FILE that were added or updated.
  size_t total_files_added_or_updated = 0;

  // Total no. of files where processing failed.
  size_t total_files_failed = 0;

  // Total no. of directories where processing failed.
  size_t total_dirs_failed = 0;

  // Total no. of assets that were deleted (not counting subdirectory files).
  size_t total_assets_deleted = 0;

  // Total no. of chunks created.
  size_t total_chunks = 0;

  // Total no. of bytes processed from the files added or updated.
  size_t total_processed_bytes = 0;
};

struct AssetInfo {
  // Unix path to the asset relative to the source directory.
  std::string path;

  // Type (file, dir, etc.)
  AssetProto::Type type = AssetProto::FILE;

  // Modification time in seconds since Epoch.
  int64_t mtime = 0;

  // File size (0 for directories).
  uint64_t size = 0;

  // File chunks (empty for directories). This list is ignored when comparing
  // one AssetInfo to another.
  std::vector<FileChunk> chunks;

  // Appends the chunks from |list| to |chunks|.
  void AppendCopyChunks(const RepeatedChunkRefProto& list,
                        uint64_t list_offset);

  // Appends the chunks from |list| to |chunks|, but moves the data out of
  // |list| instead of copying, wherever possible.
  void AppendMoveChunks(RepeatedChunkRefProto* list, uint64_t list_offset);

  bool operator==(const AssetInfo& other) const {
    return path == other.path && type == other.type && mtime == other.mtime &&
           size == other.size;
  }
  bool operator!=(const AssetInfo& other) const { return !(*this == other); }

  // Compares by file path.
  bool operator<(const AssetInfo& other) const { return path < other.path; }
};

// Incrementally updates a manifest
class ManifestUpdater {
 public:
  // Selects the update operation to be performed.
  enum class Operator { kAdd, kUpdate, kDelete, kKeep };

  // Represents an update operation that shall be applied to the owned manifest.
  struct Operation {
    Operation() {}
    Operation(Operator op, AssetInfo info) : op(op), info(std::move(info)) {}

    Operator op;
    AssetInfo info;
  };

  using OperationList = std::vector<Operation>;

  // Permissions for executable files.
  static constexpr uint32_t kExecutablePerms = 0755u;

  // Id of the chunk that stores the manifest id.
  static ContentIdProto GetManifestStoreId();

  // Returns an error if |dir| does not exist or it is not a directory.
  static absl::Status IsValidDir(std::string dir);

  using PushIntermediateManifest =
      std::function<void(const ContentIdProto& manifest_id)>;

  // |data_store| is used to store manifest chunks. File data chunks are not
  // stored explicitly as they can be read from the original files.
  // |cfg| determines the source directory to update the manifest from as well
  // as configuration details about chunking.
  ManifestUpdater(DataStoreWriter* data_store, UpdaterConfig cfg);
  ~ManifestUpdater();

  ManifestUpdater(const ManifestUpdater&) = delete;
  ManifestUpdater& operator=(const ManifestUpdater&) = delete;

  // Reads the full source directory and syncs the manifest to it. Prunes old,
  // unreferenced manifest chunks. Updates and flushes |file_chunks|.
  //
  // If a valid |push_intermediate_manifest| is passed, then a manifest is
  // flushed after the root directory has been added, but before all files and
  // directories have been processed. That means, the manifest does not yet
  // contains all assets, all incomplete assets are set to in-progress.
  absl::Status UpdateAll(FileChunkMap* file_chunks,
                         PushIntermediateManifest push_intermediate_manifest =
                             PushIntermediateManifest());

  // Updates the manifest by applying the |operations| list. Deletions are
  // handled first to make the outcome independent of the order in the list.
  // Also updates and flushes |file_chunks| with the changes made. See
  // UpdateAll() for a description of |push_intermediate_manifest|.
  //
  // All paths should be Unix paths. If |recursive| is true, then a directory
  // scanner task is enqueued for each directory that is added to the manifest.
  // This is only needed during UpdateAll(). When the manifest is updated in
  // response to file watcher changes, then |recursive| should be set to false.
  absl::Status Update(OperationList* operations, FileChunkMap* file_chunks,
                      PushIntermediateManifest push_intermediate_manifest =
                          PushIntermediateManifest(),
                      bool recursive = false);

  // Content id of the current manifest.
  const ContentIdProto& ManifestId() const { return manifest_id_; }

  // Returns stats created from the last call to UpdateAll() or Update().
  const UpdaterStats& Stats() const { return stats_; }

  // Returns the manifest updater configuration.
  const UpdaterConfig& Config() const { return cfg_; }

  // Returns an empty manifest.
  ContentIdProto DefaultManifestId();

 private:
  // Adds enough pending assets from |queue_| as tasks to the |pool| to keep all
  // worker threads busy. Returns the number of tasks that were added.
  size_t QueueTasks(Threadpool* pool, const fastcdc::Config* cdc_cfg,
                    ManifestBuilder* manifest_builder);

  // Applies the |operatio ns| list to the manifest owned by the
  // |manifest_builder|. First, all deletions are handled and the corresponding
  // files are removed from the |file_chunks| map, then all added or updated
  // assets are processed. This guarantees that the outcome is independent of
  // the order in the list.
  //
  // If |parent| is non-null, then it must be of type DIRECTORY and all added
  // assets are made direct children of |parent|. The function does *not* verify
  // that all children have |parent| as directory path.
  //
  // Enqueues tasks to chunk the given files for files that were added or
  // updated. If |recursive| is true, then it will also enqueue directory
  // scanner tasks for all given directories.
  absl::Status ApplyOperations(std::vector<Operation>* operations,
                               FileChunkMap* file_chunks,
                               ManifestBuilder* manifest_builder,
                               AssetBuilder* parent, bool recursive);

  // Handles the results of a completed FileChunkerTask.
  absl::Status HandleFileChunkerResult(FileChunkerTask* task,
                                       FileChunkMap* file_chunks,
                                       ManifestBuilder* manifest_builder);

  // Handles the results of a completed DirScannerTask.
  absl::Status HandleDirScannerResult(
      DirScannerTask* task, FileChunkMap* file_chunks,
      ManifestBuilder* manifest_builder,
      std::unordered_set<ContentIdProto>* manifest_content_ids);

  // Represents an asset that has not been fully processed yet.
  struct PendingAsset {
    PendingAsset() {}
    PendingAsset(AssetProto::Type type, std::string relative_path,
                 std::string filename)
        : type(type),
          relative_path(std::move(relative_path)),
          filename(std::move(filename)) {}

    // The asset type (either FILE or DIRECTORY).
    AssetProto::Type type = AssetProto::UNKNOWN;

    // Relative unix path of the directory containing this asset.
    std::string relative_path;

    // File name of the asset that still needs processing.
    std::string filename;
  };

  // Queue of pending assets waiting for completion.
  std::list<PendingAsset> queue_;

  // Pool of pre-allocated buffers
  std::vector<Buffer> buffers_;

  // Store for manifest chunks and the manifest id itself.
  DataStoreWriter* const data_store_;

  // Source directory to build the manifest from and configuration details.
  UpdaterConfig cfg_;

  // ID of the manifest chunk.
  ContentIdProto manifest_id_;

  // Stats for the last Update*() operation.
  UpdaterStats stats_;
};

};  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_UPDATER_H_
