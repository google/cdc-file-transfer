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

#include "manifest/file_chunk_map.h"

#include "absl/strings/str_format.h"
#include "manifest/stats_printer.h"

namespace cdc_ft {

FileChunkMap::FileChunkMap(bool enable_stats) {
  if (enable_stats) stats_ = std::make_unique<StatsPrinter>();
}

FileChunkMap::~FileChunkMap() = default;

void FileChunkMap::Init(std::string path, uint64_t file_size,
                        std::vector<FileChunk>* chunks) {
  FileUpdate update(FileUpdateType::kInit, std::move(path));
  update.file_size = file_size;
  if (chunks) update.chunks = std::move(*chunks);
  file_updates_.push_back(std::move(update));
}

void FileChunkMap::AppendCopy(std::string path,
                              const RepeatedChunkRefProto& list,
                              uint64_t list_offset) {
  FileUpdate update(FileUpdateType::kAppend, std::move(path));
  update.chunks.reserve(list.size());
  for (const ChunkRefProto& ch : list)
    update.chunks.emplace_back(ch.chunk_id(), ch.offset() + list_offset);
  file_updates_.push_back(std::move(update));
}

void FileChunkMap::AppendMove(std::string path, RepeatedChunkRefProto* list,
                              uint64_t list_offset) {
  FileUpdate update(FileUpdateType::kAppend, std::move(path));
  update.chunks.reserve(list->size());
  for (ChunkRefProto& ch : *list) {
    update.chunks.emplace_back(std::move(*ch.mutable_chunk_id()),
                               ch.offset() + list_offset);
  }
  file_updates_.push_back(std::move(update));
}

void FileChunkMap::Remove(std::string path) {
  FileUpdate update(FileUpdateType::kRemove, std::move(path));
  file_updates_.push_back(std::move(update));
}

void FileChunkMap::Clear() {
  FileUpdate update(FileUpdateType::kClear, std::string());
  file_updates_.push_back(std::move(update));
}

void FileChunkMap::FlushUpdates() {
  if (file_updates_.empty()) return;

  absl::MutexLock lock(&mutex_);

  for (FileUpdate& update : file_updates_) {
    switch (update.type) {
      case FileUpdateType::kInit: {
        File& file = path_to_file_[update.path];
        file.size = update.file_size;
        assert(total_chunks_ >= file.chunks.size());
        total_chunks_ -= file.chunks.size();
        total_chunks_ += update.chunks.size();
        file.chunks = std::move(update.chunks);
        break;
      }

      case FileUpdateType::kAppend: {
        File& file = path_to_file_[update.path];
        total_chunks_ += update.chunks.size();
        if (file.chunks.empty()) {
          file.chunks = std::move(update.chunks);
        } else {
          file.chunks.reserve(file.chunks.size() + update.chunks.size());
          std::move(std::begin(update.chunks), std::end(update.chunks),
                    std::back_inserter(file.chunks));
        }
        break;
      }

      case FileUpdateType::kRemove: {
        const auto iter = path_to_file_.find(update.path);
        if (iter == path_to_file_.end()) break;
        assert(total_chunks_ >= iter->second.chunks.size());
        total_chunks_ -= iter->second.chunks.size();
        path_to_file_.erase(iter);
        break;
      }

      case FileUpdateType::kClear: {
        path_to_file_.clear();
        total_chunks_ = 0;
        break;
      }
    }
  }

  file_updates_.clear();

  UpdateIdToChunkMap();
}

bool FileChunkMap::Lookup(const ContentIdProto& content_id, std::string* path,
                          uint64_t* offset, uint32_t* size) {
  assert(path && offset && size);

  absl::MutexLock lock(&mutex_);

  return FindChunk(content_id, path, offset, size, nullptr);
}

void FileChunkMap::RecordStreamedChunk(const ContentIdProto& content_id,
                                       size_t thread_id) {
  absl::MutexLock lock(&mutex_);

  if (!stats_) return;

  if (streamed_chunks_to_thread_.find(content_id) !=
      streamed_chunks_to_thread_.end()) {
    return;
  }

  std::string path;
  uint32_t size;
  size_t index;
  if (FindChunk(content_id, &path, nullptr, &size, &index))
    stats_->RecordStreamedChunk(path, index, size, thread_id);
  streamed_chunks_to_thread_[content_id] = thread_id;
}

void FileChunkMap::RecordCachedChunk(const ContentIdProto& content_id) {
  absl::MutexLock lock(&mutex_);

  if (!stats_) return;

  if (cached_chunks_.find(content_id) != cached_chunks_.end()) return;

  // Restarting FUSE might report cached chunks that have been originally
  // streamed. Ignore those.
  if (streamed_chunks_to_thread_.find(content_id) !=
      streamed_chunks_to_thread_.end()) {
    return;
  }

  std::string path;
  uint32_t size;
  size_t index;
  if (FindChunk(content_id, &path, nullptr, &size, &index))
    stats_->RecordCachedChunk(path, index, size);
  cached_chunks_.insert(content_id);
}

void FileChunkMap::PrintStats() {
  absl::MutexLock lock(&mutex_);

  if (!stats_) return;

  stats_->Print();
}

bool FileChunkMap::HasStats() const {
  absl::ReaderMutexLock lock(&mutex_);
  return stats_ != nullptr;
}

void FileChunkMap::UpdateIdToChunkMap() {
  assert((mutex_.AssertHeld(), true));

  // Put all chunks into the map.
  id_to_chunk_.clear();
  id_to_chunk_.reserve(total_chunks_);
  for (const auto& [path, file] : path_to_file_) {
    for (uint32_t n = 0; n < static_cast<uint32_t>(file.chunks.size()); ++n)
      id_to_chunk_[ContentIdRef(file.chunks[n].content_id)] = {&path, n};
  }

  // Might be "<" if multiple files contain the same chunk.
  assert(id_to_chunk_.size() <= total_chunks_);

  // Rebuild stats if present.
  if (stats_) {
    stats_->Clear();
    for (const auto& [path, file] : path_to_file_)
      stats_->InitFile(path, file.chunks.size());

    // Fill in the streamed chunks.
    std::string path;
    uint32_t size;
    size_t index;
    for (const auto& [id, thread_id] : streamed_chunks_to_thread_) {
      if (FindChunk(id, &path, nullptr, &size, &index))
        stats_->RecordStreamedChunk(path, index, size, thread_id);
    }

    // Fill in the cached chunks.
    for (const ContentIdProto& id : cached_chunks_) {
      if (FindChunk(id, &path, nullptr, &size, &index))
        stats_->RecordCachedChunk(path, index, size);
    }

    // Make sure the above RecordStreamedChunk() calls don't count towards
    // bandwidth stats.
    stats_->ResetBandwidthStats();
  }
}

bool FileChunkMap::FindChunk(const ContentIdProto& content_id,
                             std::string* path, uint64_t* offset,
                             uint32_t* size, size_t* index) {
  assert((mutex_.AssertHeld(), true));

  // Find the |id_to_chunk_| entry by |content_id|. It might not exist if
  // changes to the manifest have not propagated to gamelets yet.
  IdToChunkMap::iterator i2c_iter = id_to_chunk_.find(ContentIdRef(content_id));
  if (i2c_iter == id_to_chunk_.end()) return false;

  // Find the chunk location by path. This lookup should not fail because
  // |path_to_file_| and |id_to_chunk_| should always be in sync here.
  const ChunkLocation& loc = i2c_iter->second;
  PathToFileMap::iterator p2f_iter = path_to_file_.find(*loc.path);
  assert(p2f_iter != path_to_file_.end());

  // Compute path, chunk offset and chunk size.
  const File& file = p2f_iter->second;
  assert(loc.index < file.chunks.size());
  uint64_t this_offset = file.chunks[loc.index].offset;
  uint64_t next_offset = loc.index + 1 == file.chunks.size()
                             ? file.size
                             : file.chunks[loc.index + 1].offset;
  if (path) *path = *loc.path;
  if (offset) *offset = this_offset;
  if (size) *size = static_cast<uint32_t>(next_offset - this_offset);
  if (index) *index = loc.index;
  return true;
}

}  // namespace cdc_ft
