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

#include "manifest/manifest_updater.h"

#include <future>
#include <thread>

#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "common/log.h"
#include "common/path.h"
#include "common/stopwatch.h"
#include "common/threadpool.h"
#include "common/util.h"
#include "data_store/data_store_writer.h"
#include "fastcdc/fastcdc.h"
#include "manifest/asset_builder.h"
#include "manifest/file_chunk_map.h"
#include "manifest/manifest_builder.h"
#include "manifest/manifest_iterator.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {
namespace {

// Returns AssetInfos for all files and dirs in |src_dir| + |rel_path|. Does not
// recurse into sub-directories.
absl::Status GetAllSrcAssets(const std::string& src_dir,
                             const std::string& rel_path,
                             std::vector<AssetInfo>* src_assets) {
  std::string full_src_dir = path::Join(src_dir, rel_path);

  path::EnsureEndsWithPathSeparator(&full_src_dir);
  auto handler = [src_assets, &src_dir = full_src_dir,
                  rel_path = path::ToUnix(rel_path)](
                     const std::string& dir, const std::string& filename,
                     int64_t mtime, uint64_t size, bool is_dir) {
    AssetInfo ai;
    ai.path = path::JoinUnix(rel_path, filename);
    ai.type = is_dir ? AssetProto::DIRECTORY : AssetProto::FILE;
    ai.mtime = mtime;
    ai.size = is_dir ? 0 : size;
    src_assets->push_back(std::move(ai));
    return absl::OkStatus();
  };
#if PLATFORM_WINDOWS
  // Windows expects a globbing pattern to search a path.
  std::string src_pattern = path::Join(full_src_dir, "*");
#else
  std::string src_pattern = src_dir;
#endif
  absl::Status status =
      path::SearchFiles(src_pattern, /*recursive=*/false, handler);
  std::sort(src_assets->begin(), src_assets->end());
  return status;
}

// Creates a fastcdc::Config struct from a CdcParamsProto.
fastcdc::Config CdcConfigFromProto(const CdcParamsProto& cfg_pb) {
  return fastcdc::Config(cfg_pb.min_chunk_size(), cfg_pb.avg_chunk_size(),
                         cfg_pb.max_chunk_size());
}

// Checks if a given CdcParamsProto is sane and can be used for FastCDC.
bool ValidateCdcParams(const CdcParamsProto& params) {
  return params.min_chunk_size() <= params.avg_chunk_size() &&
         params.avg_chunk_size() <= params.max_chunk_size() &&
         params.max_chunk_size() > 0;
}

// Returns the max. number of tasks that should be enqueued in the given thread
// pool.
size_t MaxQueuedTasks(const Threadpool& pool) { return pool.NumThreads() << 1; }

}  // namespace

void AssetInfo::AppendCopyChunks(const RepeatedChunkRefProto& list,
                                 uint64_t list_offset) {
  chunks.reserve(chunks.size() + list.size());
  for (const ChunkRefProto& ch : list)
    chunks.emplace_back(ch.chunk_id(), ch.offset() + list_offset);
}

void AssetInfo::AppendMoveChunks(RepeatedChunkRefProto* list,
                                 uint64_t list_offset) {
  chunks.reserve(chunks.size() + list->size());
  for (ChunkRefProto& ch : *list)
    chunks.emplace_back(std::move(*ch.mutable_chunk_id()),
                        ch.offset() + list_offset);
}

// Common fields for tasks that fill in manifest data.
class ManifestTask : public Task {
 public:
  ManifestTask(std::string src_dir, PendingAsset asset)
      : src_dir_(std::move(src_dir)), asset_(std::move(asset)) {}

  // Relative unix path of the directory containing the file or directory for
  // this task.
  const std::string& RelativeUnixPath() const { return asset_.relative_path; }

  // Relative unix path of the file or directory for this task.
  std::string RelativeUnixFilePath() const {
    return path::JoinUnix(RelativeUnixPath(), Filename());
  }

  // Name of the file or directory to process with this task.
  const std::string& Filename() const { return asset_.filename; }

  // Full path of the file or directory to process with this task.
  std::string FilePath() const {
    return path::Join(src_dir_, path::ToNative(RelativeUnixPath()),
                      asset_.filename);
  }

  // Returns the final status of the task.
  // Should not be accessed before the task is finished.
  const absl::Status& Status() const { return status_; }

  // Returns whether or not this asset is explicitly prioritized.
  bool Prioritized() const { return asset_.prioritized; }

  // Returns the pending asset's deadline.
  absl::Time Deadline() const { return asset_.deadline; }

 protected:
  const std::string src_dir_;
  const PendingAsset asset_;
  absl::Status status_;
};

// ThreadPool task that runs the CDC chunker on a given file.
class FileChunkerTask : public ManifestTask {
 public:
  FileChunkerTask(std::string src_dir, PendingAsset asset,
                  const fastcdc::Config* cfg, Buffer buffer)
      : ManifestTask(std::move(src_dir), std::move(asset)),
        cfg_(cfg),
        buffer_(std::move(buffer)) {
    assert(cfg_->max_size > 0);
  }

  // Returns the number of bytes processed. Should match file size unless some
  // error occurred.
  // Should not be accessed before the task is finished.
  uint64_t ProcessedBytes() const { return processed_bytes_; }

  // True if the file looks like a Linux executable based on elf/shebang magic
  // headers.
  // Should not be accessed before the task is finished.
  bool IsExecutable() const { return is_executable_; }

  // Returns the chunk hashes and offsets.
  // Should not be accessed before the task is finished.
  google::protobuf::RepeatedPtrField<ChunkRefProto>* Chunks() {
    return &chunks_;
  }

  // Releases the allocated buffer and returns it to the caller.
  Buffer&& ReleaseBuffer() { return std::move(buffer_); }

  // Task:
  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    // TODO: Retry with backoff if this fails in practice, e.g. if the file is
    // changed repeatedly.
    std::string file_path = FilePath();
    absl::StatusOr<FILE*> file = path::OpenFile(file_path, "rb");
    if (!file.ok()) {
      status_ =
          WrapStatus(file.status(), "Failed to open file '%s'", file_path);
      return;
    }
    path::FileCloser closer(*file);

    auto chunk_handler = [chunks = &chunks_, offset = &processed_bytes_](
                             const void* data, size_t size) {
      ChunkRefProto* chunk = chunks->Add();
      *chunk->mutable_chunk_id() = ContentId::FromArray(data, size);
      chunk->set_offset(*offset);
      *offset += size;
    };
    fastcdc::Chunker chunker(*cfg_, chunk_handler);

    bool first_chunk = true;
    auto stream_handler = [&chunker, &is_cancelled, &first_chunk,
                           is_executable = &is_executable_,
                           &file_path](const void* data, size_t size) {
      chunker.Process(static_cast<const uint8_t*>(data), size);
      if (first_chunk) {
        first_chunk = false;
        *is_executable = Util::IsExecutable(data, size);
      }
      return is_cancelled() ? absl::CancelledError(absl::StrFormat(
                                  "chunking file '%s' cancelled", file_path))
                            : absl::OkStatus();
    };

    status_ = path::StreamReadFileContents(*file, &buffer_, stream_handler);
    chunker.Finalize();
  }

 private:
  const fastcdc::Config* const cfg_;

  google::protobuf::RepeatedPtrField<ChunkRefProto> chunks_;
  uint64_t processed_bytes_ = 0;
  bool is_executable_ = false;
  Buffer buffer_;
};

// ThreadPool task that creates assets for the contents of a directory.
class DirScannerTask : public ManifestTask {
 public:
  DirScannerTask(std::string src_dir, PendingAsset asset, AssetBuilder dir,
                 DataStoreReader* data_store)
      : ManifestTask(std::move(src_dir), std::move(asset)),
        dir_(dir),
        data_store_(data_store) {}

  // Task:
  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    std::vector<AssetInfo> src_assets, manifest_assets;
    // Collect all files from the given directory.
    status_ = GetAllSrcAssets(src_dir_, path::ToNative(RelativeUnixFilePath()),
                              &src_assets);
    if (!status_.ok()) return;
    // Collect all assets from the manifest.
    status_ = GetAllAssetsFromDirAsset(&manifest_assets, is_cancelled);
    if (!status_.ok()) return;
    CompareAssets(src_assets, manifest_assets);
    if (is_cancelled()) status_ = absl::CancelledError();
  }

  // Returns the IDs of indirect lists that were fetched when executing this
  // task.
  std::vector<ContentIdProto>* ManifestContentIds() {
    return &manifest_content_ids_;
  }

  // Returns the AssetBuilder representing the directory this task is scanning.
  AssetBuilder* Dir() { return &dir_; }

  // Returns the list of assets that need to be added or updated in the
  // directory that this task was scanning.
  ManifestUpdater::OperationList* Operations() { return &operations_; }

 private:
  using Operator = ManifestUpdater::Operator;

  // Stores AssetInfo structs for all assets found in |assets| in the
  // target param |asset_infos|.
  void GetAssetInfosFromList(const std::string& rel_path,
                             const RepeatedAssetProto& assets,
                             std::vector<AssetInfo>* asset_infos) {
    asset_infos->reserve(asset_infos->size() + assets.size());

    for (const AssetProto& asset : assets) {
      AssetInfo ai;
      ai.path = path::JoinUnix(rel_path, asset.name());
      ai.type = asset.type();
      ai.mtime = asset.mtime_seconds();
      ai.size = asset.type() == AssetProto::DIRECTORY ? 0 : asset.file_size();

      if (asset.type() == AssetProto::FILE) {
        // Copy chunks from the direct chunk list.
        ai.AppendCopyChunks(asset.file_chunks(), 0);

        // Append all chunk IDs from indirect chunk lists.
        for (const IndirectChunkListProto& icl : asset.file_indirect_chunks()) {
          ChunkListProto chunk_list;
          absl::Status status =
              data_store_->GetProto(icl.chunk_list_id(), &chunk_list);
          if (!status.ok()) {
            // Pretend the file is empty.
            ai.chunks.clear();
            // Log a warning and continue so that the file is re-added and
            // corrected.
            LOG_WARNING(
                "Can't read indirect chunk list for file '%s': %s. The "
                "affected asset will be updated from disk.",
                ai.path, status.ToString());
            break;
          }
          ai.AppendMoveChunks(chunk_list.mutable_chunks(), icl.offset());
          // Collect the content IDs of all indirect chunk lists.
          manifest_content_ids_.push_back(icl.chunk_list_id());
        }
      }

      asset_infos->emplace_back(std::move(ai));
    }
  }

  // Collects all assets from the manifest directory at RelativeUnixFilePath()
  // and adds corresponding AssetInfo structs to |asset_infos|.
  absl::Status GetAllAssetsFromDirAsset(std::vector<AssetInfo>* asset_infos,
                                        IsCancelledPredicate is_cancelled) {
    // Collect all direct assets from the manifest.
    std::string rel_path = dir_.RelativeFilePath();
    GetAssetInfosFromList(rel_path, dir_.Proto()->dir_assets(), asset_infos);
    // Load all indirect asset lists, if there are any.
    if (dir_.Proto()->dir_indirect_assets_size() > 0) {
      auto it = dir_.Proto()->mutable_dir_indirect_assets()->begin();
      while (it != dir_.Proto()->mutable_dir_indirect_assets()->end()) {
        if (is_cancelled()) return absl::CancelledError();

        AssetListProto list;
        absl::Status status = data_store_->GetProto(*it, &list);
        if (status.ok()) {
          GetAssetInfosFromList(rel_path, list.assets(), asset_infos);
          // Collect the content IDs of all indirect asset lists.
          manifest_content_ids_.push_back(*it);
          ++it;
        } else {
          // In case of an error, log a warning and continue.
          LOG_WARNING(
              "Can't read indirect asset list for directory '%s': %s. The "
              "affected assets will be updated from disk.",
              rel_path, status.ToString());
          it = dir_.Proto()->mutable_dir_indirect_assets()->erase(it);
        }
      }
    }
    std::sort(asset_infos->begin(), asset_infos->end());
    return is_cancelled() ? absl::CancelledError() : absl::OkStatus();
  }

  // Both |srcs_assets| and |manifest_assets| must be sorted.
  void CompareAssets(const std::vector<AssetInfo>& src_assets,
                     const std::vector<AssetInfo>& manifest_assets) {
    // Compare the arrays, sorting the assets into the right buckets.
    auto src_iter = src_assets.begin();
    auto manifest_iter = manifest_assets.begin();

    while (src_iter != src_assets.end() ||
           manifest_iter != manifest_assets.end()) {
      const int order = src_iter == src_assets.end()
                            ? 1  // Extraneous manifest asset.
                        : manifest_iter == manifest_assets.end()
                            ? -1  // Missing/outdated manifest asset.
                            : src_iter->path.compare(manifest_iter->path);

      if (order < 0) {
        // Missing manifest file -> add to manifest.
        operations_.emplace_back(Operator::kAdd, std::move(*src_iter));
        ++src_iter;
      } else if (order > 0) {
        // Extraneous manifest asset -> delete.
        operations_.emplace_back(Operator::kDelete, std::move(*manifest_iter));
        ++manifest_iter;
      } else if (src_iter->mtime == manifest_iter->mtime &&
                 src_iter->type == manifest_iter->type &&
                 // For files, compare the size.
                 (src_iter->type != AssetProto::FILE ||
                  src_iter->size == manifest_iter->size) &&
                 // Directories always need to be updated recursively.
                 src_iter->type != AssetProto::DIRECTORY) {
        // Assets match, keep content IDs from the manifest asset for populating
        // the FileChunkMap.
        operations_.emplace_back(Operator::kKeep, std::move(*manifest_iter));
        ++src_iter;
        ++manifest_iter;
      } else {
        // Source asset changed -> update manifest asset.
        operations_.emplace_back(Operator::kUpdate, std::move(*src_iter));
        ++src_iter;
        ++manifest_iter;
      }
    }
  }

  DataStoreReader* data_store_;
  AssetBuilder dir_;
  std::vector<ContentIdProto> manifest_content_ids_;
  ManifestUpdater::OperationList operations_;
};

// static
ContentIdProto ManifestUpdater::GetManifestStoreId() {
  ContentIdProto manifest_store_id;
  ContentId::FromHexString("0000000000000000000000000000000000000000",
                           &manifest_store_id);
  return manifest_store_id;
}

// static
absl::Status ManifestUpdater::IsValidDir(std::string dir) {
  path::EnsureDoesNotEndWithPathSeparator(&dir);

  if (!path::IsAbsolute(dir)) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Directory '%s' must be an absolute path.", dir));
  }

  if (!path::Exists(dir)) {
    return absl::NotFoundError(
        absl::StrFormat("Failed to find directory '%s'.", dir));
  }

  if (!path::DirExists(dir)) {
    return absl::FailedPreconditionError(
        absl::StrFormat("Path '%s' should be a directory.", dir));
  }
  return absl::OkStatus();
}

ManifestUpdater::ManifestUpdater(DataStoreWriter* data_store, UpdaterConfig cfg)
    : data_store_(data_store),
      cfg_(std::move(cfg)),
      queue_(kMinAssetProcessingTime) {
  path::EnsureEndsWithPathSeparator(&cfg_.src_dir);
}

ManifestUpdater::~ManifestUpdater() = default;

absl::Status ManifestUpdater::UpdateAll(FileChunkMap* file_chunks,
                                        PushManifestHandler push_handler) {
  RETURN_IF_ERROR(ManifestUpdater::IsValidDir(cfg_.src_dir));

  // Don't use the Windows localized time from path::GetStats.
  time_t mtime;
  RETURN_IF_ERROR(path::GetFileTime(cfg_.src_dir, &mtime));

  // Create the info for the root directory to start the recursive search.
  AssetInfo ri;
  ri.type = AssetProto::DIRECTORY;
  ri.mtime = mtime;

  std::vector<Operation> operations{{Operator::kAdd, std::move(ri)}};

  absl::Status status = Update(&operations, file_chunks, push_handler,
                               /*recursive=*/true);

  if (status.ok() || !absl::IsUnavailable(status)) return status;

  // In case we receive an absl::UnavailableError, it means that not all
  // manifest chunks could be located. In that case, we wipe all data and
  // rebuild the manifest from scratch.
  LOG_WARNING("Failed to load manifest, building from scratch: %s",
              status.ToString());

  RETURN_IF_ERROR(data_store_->Wipe());
  file_chunks->Clear();

  RETURN_IF_ERROR(Update(&operations, file_chunks, push_handler,
                         /*recursive=*/true),
                  "Failed to build manifest from scratch");

  return absl::OkStatus();
}

ContentIdProto ManifestUpdater::DefaultManifestId() {
  CdcParamsProto params;
  params.set_min_chunk_size(cfg_.min_chunk_size);
  params.set_avg_chunk_size(cfg_.avg_chunk_size);
  params.set_max_chunk_size(cfg_.max_chunk_size);
  ManifestBuilder manifest_builder(params, data_store_);

  // Load the manifest id from the store. It's necessary to extract the CDC
  // parameters used last time.
  ContentIdProto manifest_id;
  if ((data_store_->GetProto(GetManifestStoreId(), &manifest_id).ok()) &&
      manifest_builder.LoadManifest(manifest_id).ok() &&
      ValidateCdcParams(manifest_builder.CdcParameters())) {
    params = manifest_builder.CdcParameters();
  }

  // Create an empty manifest with correct CDC parameters.
  ManifestBuilder new_manifest_builder(params, data_store_);
  absl::StatusOr<ContentIdProto> result = new_manifest_builder.Flush();
  assert(result.ok());
  manifest_id_ = *result;
  std::string id_str = manifest_id_.SerializeAsString();

  absl::Status status =
      data_store_->Put(GetManifestStoreId(), id_str.data(), id_str.size());
  if (!status.ok()) {
    LOG_ERROR("Failed to store default manifest ID in data store: %s",
              status.ToString());
  }
  return manifest_id_;
}

absl::Status ManifestUpdater::FlushAndPushManifest(
    FileChunkMap* file_chunks,
    std::unordered_set<ContentIdProto>* manifest_content_ids,
    PushManifestHandler push_manifest_handler) {
  file_chunks->FlushUpdates();
  ASSIGN_OR_RETURN(manifest_id_, manifest_builder_->Flush(),
                   "Failed to flush intermediate manifest");
  // Add all content IDs that were just written back.
  manifest_content_ids->insert(manifest_builder_->FlushedContentIds().begin(),
                               manifest_builder_->FlushedContentIds().end());
  if (push_manifest_handler) push_manifest_handler(manifest_id_);
  last_manifest_flush_ = absl::Now();
  return absl::OkStatus();
}

bool ManifestUpdater::WantManifestFlushed(
    PushManifestHandler push_manifest_handler) const {
  return push_manifest_handler && flush_deadline_ < absl::Now() &&
         last_manifest_flush_ + kMinDelayBetweenFlush < absl::Now();
}

absl::Status ManifestUpdater::MaybeFlushAndPushManifest(
    size_t dir_scanner_tasks_queued, FileChunkMap* file_chunks,
    std::unordered_set<ContentIdProto>* manifest_content_ids,
    PushManifestHandler push_manifest) {
  // Flush only if there are no DirScannerTask active.
  if (dir_scanner_tasks_queued == 0 && WantManifestFlushed(push_manifest)) {
    flush_deadline_ = absl::InfiniteFuture();
    return FlushAndPushManifest(file_chunks, manifest_content_ids,
                                push_manifest);
  }
  return absl::OkStatus();
}

void ManifestUpdater::AddPriorityAssets(std::vector<std::string> rel_paths) {
  absl::MutexLock lock(&priority_mutex_);
  absl::Time now = absl::Now();
  for (std::string& rel_path : rel_paths) {
    priority_assets_.push_back(PriorityAsset{std::move(rel_path), now});
  }
}

void ManifestUpdater::PrioritizeQueuedAssets() {
  std::vector<PriorityAsset> prio_assets;
  {
    absl::MutexLock lock(&priority_mutex_);
    if (priority_assets_.empty()) return;
    std::swap(prio_assets, priority_assets_);
  }

  absl::Time deadline = queue_.Prioritize(prio_assets, manifest_builder_.get());
  if (deadline < flush_deadline_) flush_deadline_ = deadline;
}

ManifestUpdater::QueueTasksResult ManifestUpdater::QueueTasks(
    bool drain_dir_scanner_tasks, Threadpool* pool,
    const fastcdc::Config* cdc_cfg) {
  // Prioritize requested assets before queuing new tasks.
  PrioritizeQueuedAssets();
  const size_t max_tasks_queued = MaxQueuedTasks(*pool);
  size_t file_chunker_tasks = 0, dir_scanner_tasks = 0;

  // Skip DIRECTORY assets if we should drain DirScannerTasks.
  PendingAssetsQueue::AcceptFunc accept = nullptr;
  if (drain_dir_scanner_tasks) {
    accept = [](const PendingAsset& p) {
      return p.type != AssetProto::DIRECTORY;
    };
  }

  absl::StatusOr<AssetBuilder> dir;
  PendingAsset asset;

  while (pool->NumQueuedTasks() < max_tasks_queued && !buffers_.empty() &&
         queue_.Dequeue(&asset, accept)) {
    switch (asset.type) {
      case AssetProto::FILE:
        pool->QueueTask(std::make_unique<FileChunkerTask>(
            cfg_.src_dir, std::move(asset), cdc_cfg,
            std::move(buffers_.back())));
        buffers_.pop_back();
        ++file_chunker_tasks;
        break;

      case AssetProto::DIRECTORY:
        // Flushing the manifest may invalidate the pointers to the directory
        // proto returned from GetOrCreateAsset(), so the manifest cannot be
        // flushed as long as DirScannerTask are in the queue.
        dir = manifest_builder_->GetOrCreateAsset(
            path::JoinUnix(asset.relative_path, asset.filename),
            AssetProto::DIRECTORY, /*force_create=*/true);
        if (!dir.ok()) {
          LOG_ERROR(
              "Failed to locate directory '%s' in the manifest, skipping it: "
              "%s",
              asset.relative_path, dir.status().ToString());
          continue;
        }
        pool->QueueTask(std::make_unique<DirScannerTask>(
            cfg_.src_dir, std::move(asset), std::move(dir.value()),
            data_store_));
        ++dir_scanner_tasks;
        break;

      default:
        LOG_ERROR("Unexpected type '%s' for asset '%s'",
                  AssetProto::Type_Name(asset.type), asset.relative_path);
        continue;
    }
  }
  return QueueTasksResult{dir_scanner_tasks, file_chunker_tasks};
}

absl::Status ManifestUpdater::ApplyOperations(
    std::vector<Operation>* operations, FileChunkMap* file_chunks,
    AssetBuilder* parent, absl::Time deadline, bool recursive) {
  if (operations->empty()) return absl::OkStatus();

  // First, handle all deletions to make the outcome independent of the order of
  // operations (e.g., when the same file is added and deleted again).
  const std::string* last_deleted = nullptr;
  for (const Operation& op : *operations) {
    if (op.op != Operator::kDelete) continue;
    const AssetInfo& ai = op.info;

    ++stats_.total_assets_deleted;
    file_chunks->Remove(ai.path);
    if (last_deleted && absl::StartsWith(ai.path, *last_deleted) &&
        ai.path[last_deleted->size()] == '/') {
      // Optimization: |path| is part of a deleted dir, so it can be
      // skipped.
      continue;
    }
    RETURN_IF_ERROR(manifest_builder_->DeleteAsset(ai.path),
                    "Failed to delete asset '%s' from manifest", ai.path);
    last_deleted = &ai.path;
  }

  // Second, handle additions and updates.
  AssetBuilder asset_builder;
  for (Operation& op : *operations) {
    AssetInfo& ai = op.info;
    bool created = true;

    switch (op.op) {
      case Operator::kDelete:
        continue;

      case Operator::kKeep:
        file_chunks->Init(ai.path, ai.size, &ai.chunks);
        continue;

      case Operator::kAdd:
        // If a parent was given, assets are added as direct children of that
        // parent directory.
        if (parent) {
          asset_builder = parent->AppendAsset(path::BaseName(ai.path), ai.type);
          break;
        }
        [[fallthrough]];

      case Operator::kUpdate:
        ASSIGN_OR_RETURN(asset_builder,
                         manifest_builder_->GetOrCreateAsset(ai.path, ai.type,
                                                             true, &created),
                         "Failed to add '%s' to the manifest", ai.path);
        break;
    }

    if (created) ++stats_.total_assets_added_or_updated;
    asset_builder.SetMtimeSeconds(ai.mtime);

    if (ai.type == AssetProto::FILE) {
      // Assume everything is executable for the intermediate manifest.
      // The executable bit is derived from the file data, which is not
      // available at this point.
      asset_builder.SetPermissions(kExecutablePerms);
      asset_builder.TruncateChunks();
      asset_builder.SetFileSize(ai.size);
      // Queue chunker tasks for files.
      asset_builder.SetInProgress(true);
    } else if (ai.type == AssetProto::DIRECTORY) {
      asset_builder.SetPermissions(ManifestBuilder::kDefaultDirPerms);
      // We are recursing into all sub-directories, so we queue up the child
      // directory for scanning.
      if (recursive) asset_builder.SetInProgress(true);
    }

    // If the asset is marked as in-progress, we need to queue it up.
    if (asset_builder.InProgress()) {
      PendingAsset pending(ai.type, asset_builder.RelativePath(),
                           asset_builder.Name(), deadline);
      queue_.Add(std::move(pending));
    }
  }
  return absl::OkStatus();
}

absl::Status ManifestUpdater::HandleFileChunkerResult(
    FileChunkerTask* task, FileChunkMap* file_chunks) {
  const std::string rel_file_path = task->RelativeUnixFilePath();
  buffers_.emplace_back(task->ReleaseBuffer());

  AssetBuilder asset_builder;
  ASSIGN_OR_RETURN(asset_builder, manifest_builder_->GetOrCreateAsset(
                                      rel_file_path, AssetProto::FILE));
  asset_builder.SetInProgress(false);
  if (!task->Status().ok()) {
    // In case of an error, pretend the file is empty.
    asset_builder.SetFileSize(0);
    file_chunks->Init(rel_file_path, 0);

    ++stats_.total_files_failed;
    return task->Status();
  }

  // Update the asset and the stats.
  uint64_t file_size = task->ProcessedBytes();
  stats_.total_chunks += task->Chunks()->size();
  stats_.total_processed_bytes += file_size;
  ++stats_.total_files_added_or_updated;

  asset_builder.SwapChunks(task->Chunks(), file_size);
  asset_builder.SetPermissions(task->IsExecutable()
                                   ? kExecutablePerms
                                   : ManifestBuilder::kDefaultFilePerms);

  file_chunks->Init(rel_file_path, file_size);
  file_chunks->AppendCopy(rel_file_path, asset_builder.Proto()->file_chunks(),
                          0);

  return absl::OkStatus();
}

absl::Status ManifestUpdater::HandleDirScannerResult(
    DirScannerTask* task, FileChunkMap* file_chunks,
    std::unordered_set<ContentIdProto>* manifest_content_ids) {
  // Include the error in the stats, but we can still try to process the
  // (partial) results.
  if (!task->Status().ok()) {
    ++stats_.total_dirs_failed;
  }

  // If there's a chance we can do more work within the parent's deadline, we
  // propagate the deadline to the children.
  // TODO(chrschn) Use SteadyClock instead of the system clock.
  absl::Time deadline = task->Deadline() > absl::Now() ? task->Deadline()
                                                       : absl::InfiniteFuture();

  // DirScannerTasks are inherently recursive.
  RETURN_IF_ERROR(ApplyOperations(task->Operations(), file_chunks, task->Dir(),
                                  deadline, /*recursive=*/true));
  task->Dir()->SetInProgress(false);
  // Union all manifest chunk content IDs.
  manifest_content_ids->insert(task->ManifestContentIds()->begin(),
                               task->ManifestContentIds()->end());
  return task->Status();
}

absl::Status ManifestUpdater::Update(OperationList* operations,
                                     FileChunkMap* file_chunks,
                                     PushManifestHandler push_handler,
                                     bool recursive) {
  Stopwatch sw;
  LOG_INFO(
      "Updating manifest for '%s': applying %u changes, "
      "%srecursive",
      cfg_.src_dir, operations->size(), recursive ? "" : "non-");

  stats_ = UpdaterStats();

  // Collects the content IDs that make up the manifest when recursing. They are
  // used to prune the manifest cache directory at the end of the Update()
  // process.
  std::unordered_set<ContentIdProto> manifest_content_ids;

  CdcParamsProto cdc_params;
  cdc_params.set_min_chunk_size(cfg_.min_chunk_size);
  cdc_params.set_avg_chunk_size(cfg_.avg_chunk_size);
  cdc_params.set_max_chunk_size(cfg_.max_chunk_size);
  manifest_builder_ =
      std::make_unique<ManifestBuilder>(cdc_params, data_store_);

  // Load the manifest id from the store.
  ContentIdProto manifest_id;
  absl::Status status =
      data_store_->GetProto(GetManifestStoreId(), &manifest_id);
  if (!status.ok()) {
    if (!absl::IsNotFound(status))
      return WrapStatus(status, "Failed to load manifest id");

    // A non-existing manifest is not an issue, just build it from scratch.
    LOG_INFO("No cached manifest found. Building from scratch.");
  } else {
    RETURN_IF_ERROR(manifest_builder_->LoadManifest(manifest_id),
                    "Failed to load manifest with id '%s'",
                    ContentId::ToHexString(manifest_id));
    // The CDC params might have changed when loading the manifest.
    if (ValidateCdcParams(manifest_builder_->Manifest()->cdc_params())) {
      cdc_params = manifest_builder_->Manifest()->cdc_params();
    }
  }

  RETURN_IF_ERROR(ApplyOperations(operations, file_chunks, nullptr,
                                  absl::InfiniteFuture(), recursive));

  Threadpool pool(cfg_.num_threads > 0 ? cfg_.num_threads
                                       : std::thread::hardware_concurrency());
  // Pre-allocate one buffer per queueable task with 2 * max_chunk_size.
  const size_t max_queued_tasks = MaxQueuedTasks(pool);
  buffers_.reserve(max_queued_tasks);
  while (buffers_.size() < max_queued_tasks)
    buffers_.emplace_back(cfg_.max_chunk_size << 1);
  size_t total_tasks_queued = 0, scanner_tasks_queued = 0;

  // Push intermediate manifest if there are queued tasks.
  if (push_handler && !queue_.Empty()) {
    RETURN_IF_ERROR(
        FlushAndPushManifest(file_chunks, &manifest_content_ids, push_handler));
  }

  fastcdc::Config cdc_cfg = CdcConfigFromProto(cdc_params);

  // Wait for the chunker and scanner tasks.
  while (!queue_.Empty() || total_tasks_queued > 0) {
    RETURN_IF_ERROR(MaybeFlushAndPushManifest(scanner_tasks_queued, file_chunks,
                                              &manifest_content_ids,
                                              push_handler));
    // Flushing the manifest may invalidate the AssetProto pointers held by the
    // queued DirScannerTask. If the manifest should be flushed, we drain the
    // queue from those tasks so that the push is safe.
    bool drain_dir_scanners = WantManifestFlushed(push_handler);

    QueueTasksResult queued = QueueTasks(drain_dir_scanners, &pool, &cdc_cfg);
    total_tasks_queued += queued.dir_scanners + queued.file_chunkers;
    scanner_tasks_queued += queued.dir_scanners;

    std::unique_ptr<Task> task = pool.GetCompletedTask();
    assert(total_tasks_queued > 0);
    --total_tasks_queued;

    FileChunkerTask* chunker_task = dynamic_cast<FileChunkerTask*>(task.get());
    if (chunker_task) {
      status = HandleFileChunkerResult(chunker_task, file_chunks);

      if (!status.ok()) {
        LOG_ERROR("Failed to process file '%s': %s", chunker_task->FilePath(),
                  status.ToString());
      }
      continue;
    }

    DirScannerTask* scanner_task = dynamic_cast<DirScannerTask*>(task.get());
    if (scanner_task) {
      assert(scanner_tasks_queued > 0);
      --scanner_tasks_queued;
      status = HandleDirScannerResult(scanner_task, file_chunks,
                                      &manifest_content_ids);
      if (!status.ok()) {
        LOG_ERROR("Failed to process directory '%s': %s",
                  scanner_task->FilePath(), status.ToString());
      }
      continue;
    }
  }

  // Don't pass in the push_handler here. We first want to write back the new
  // manifest ID to the data store before we call the handler.
  RETURN_IF_ERROR(
      FlushAndPushManifest(file_chunks, &manifest_content_ids, nullptr));
  // Save the manifest id to the store.
  std::string id_str = manifest_id_.SerializeAsString();
  RETURN_IF_ERROR(
      data_store_->Put(GetManifestStoreId(), id_str.data(), id_str.size()),
      "Failed to store manifest id");
  if (push_handler) push_handler(manifest_id_);

  // Remove manifest chunks that are no longer referenced when recursing through
  // all sub-directories. This also makes sure that all referenced manifest
  // chunks are present.
  if (status.ok() && recursive) {
    // Retain the chunk that stores the manifest ID.
    manifest_content_ids.insert(GetManifestStoreId());
    status = data_store_->Prune(std::move(manifest_content_ids));
    if (!status.ok()) {
      // Signal to the caller that the manifest needs to be rebuilt from
      // scratch.
      return absl::UnavailableError(status.ToString());
    }
  }

  LOG_INFO("Manifest for '%s' successfully updated in %0.3f seconds",
           cfg_.src_dir, sw.ElapsedSeconds());

  return absl::OkStatus();
}

}  // namespace cdc_ft
