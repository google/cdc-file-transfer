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
#include "manifest/pending_assets_queue.h"

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

  using PushManifestHandler =
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
  absl::Status UpdateAll(
      FileChunkMap* file_chunks,
      PushManifestHandler push_manifest_handler = PushManifestHandler());

  // Updates the manifest by applying the |operations| list. Deletions are
  // handled first to make the outcome independent of the order in the list.
  // Also updates and flushes |file_chunks| with the changes made. See
  // UpdateAll() for a description of |push_intermediate_manifest|.
  //
  // All paths should be Unix paths. If |recursive| is true, then a directory
  // scanner task is enqueued for each directory that is added to the manifest.
  // This is only needed during UpdateAll(). When the manifest is updated in
  // response to file watcher changes, then |recursive| should be set to false.
  absl::Status Update(
      OperationList* operations, FileChunkMap* file_chunks,
      PushManifestHandler push_manifest_handler = PushManifestHandler(),
      bool recursive = false);

  // Content id of the current manifest.
  const ContentIdProto& ManifestId() const { return manifest_id_; }

  // Returns stats created from the last call to UpdateAll() or Update().
  const UpdaterStats& Stats() const { return stats_; }

  // Returns the manifest updater configuration.
  const UpdaterConfig& Config() const { return cfg_; }

  // Returns an empty manifest.
  ContentIdProto DefaultManifestId();

  // Appends the given |rel_paths| to the list of assets to prioritize. All
  // paths must be given as Unix paths.
  void AddPriorityAssets(std::vector<std::string> rel_paths)
      ABSL_LOCKS_EXCLUDED(priority_mutex_);

 private:
  // Holds the number of queued tasks returned by QueueTasks().
  struct QueueTasksResult {
    size_t dir_scanners = 0, file_chunkers = 0;
  };

  // Adds enough pending assets from |queue_| as tasks to the |pool| to keep all
  // worker threads busy. If |drain_dir_scanner_tasks| is true, only
  // FileChunkerTasks are queued, others are skipped. Returns the number of
  // tasks that were queued as a QueueTaskResult.
  QueueTasksResult QueueTasks(bool drain_dir_scanner_tasks, Threadpool* pool,
                              const fastcdc::Config* cdc_cfg);

  // Modifies the list of queued tasks to prioritize those assets that were
  // previously selected using the AddPriorityAssets() method.
  void PrioritizeQueuedAssets() ABSL_LOCKS_EXCLUDED(priority_mutex_);

  // Returns true if all of the following conditions are satisfied:
  // - |push_manifest_handler| is valid
  // - the flush deadline that was set by a prioritized asset is due
  // - the manifest was not flushed recently
  bool WantManifestFlushed(PushManifestHandler push_manifest_handler) const;

  // Checks if it is safe and desired to flush the manifest, then calls
  // FlushAndPushManifest() if that is the case.
  // |dir_scanner_tasks_queued| must be the number of currently queued
  // DirScannerTasks.
  // |file_chunks| is updated by the flush operation, if it is executed.
  // |push_manifest_handler| is invoked if the manifest gets flushed and pushed.
  absl::Status MaybeFlushAndPushManifest(
      size_t dir_scanner_tasks_queued, FileChunkMap* file_chunks,
      std::unordered_set<ContentIdProto>* manifest_content_ids,
      PushManifestHandler push_manifest_handler);

  // Flushes the in-progress manifest and the updates queued in |file_chunks|.
  // If |push_manifest_handler| is not nullptr, it is invoked with the resulting
  // manifest ID.
  absl::Status FlushAndPushManifest(
      FileChunkMap* file_chunks,
      std::unordered_set<ContentIdProto>* manifest_content_ids,
      PushManifestHandler push_manifest_handler);

  // Applies the |operations| list to the manifest owned by the manifest
  // builder. First, all deletions are handled and the corresponding files are
  // removed from the |file_chunks| map, then all added or updated assets are
  // processed. This guarantees that the outcome is independent of the order in
  // the list.
  //
  // If |parent| is non-null, then it must be of type DIRECTORY and all added
  // assets are made direct children of |parent|. The function does *not* verify
  // that all children have |parent| as directory path. This is used to
  // efficently handle the result of a DirScannerTask.
  //
  // Enqueues tasks to chunk the given files for files that were added or
  // updated. If |recursive| is true, then it will also enqueue directory
  // scanner tasks for all given directories. All follow-up tasks have the given
  // |deadline| set, which determines the deadline after which the manifest
  // should be flushed.
  absl::Status ApplyOperations(std::vector<Operation>* operations,
                               FileChunkMap* file_chunks, AssetBuilder* parent,
                               absl::Time deadline, bool recursive);

  // Handles the results of a completed FileChunkerTask.
  absl::Status HandleFileChunkerResult(FileChunkerTask* task,
                                       FileChunkMap* file_chunks);

  // Handles the results of a completed DirScannerTask.
  absl::Status HandleDirScannerResult(
      DirScannerTask* task, FileChunkMap* file_chunks,
      std::unordered_set<ContentIdProto>* manifest_content_ids);

  // Queue of pending assets waiting for completion.
  PendingAssetsQueue queue_;

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

  // The builder used for updating the manifest.
  std::unique_ptr<ManifestBuilder> manifest_builder_;

  // Holds the assets that should be prioritized while updating the manifest.
  std::vector<PriorityAsset> priority_assets_ ABSL_GUARDED_BY(priority_mutex_);
  absl::Mutex priority_mutex_;

  // Deadline by which the manifest should be flushed again.
  absl::Time flush_deadline_ = absl::InfiniteFuture();

  // The time when the manifest was flushed last.
  absl::Time last_manifest_flush_;

  // How much time we allow at least for processing a prioritized asset. The
  // manifest won't be flushed for that time, to allow more assets to be
  // finalized before the manifest is sent to the client.
  static constexpr absl::Duration kMinAssetProcessingTime =
      absl::Milliseconds(200);

  // How often we allow an intermediate manifest to be flushed and pushed.
  static constexpr absl::Duration kMinDelayBetweenFlush =
      absl::Milliseconds(500);
};

};  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_UPDATER_H_
