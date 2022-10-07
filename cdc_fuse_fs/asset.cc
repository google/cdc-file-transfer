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

#include "cdc_fuse_fs/asset.h"

#include "common/buffer.h"
#include "common/status.h"
#include "data_store/data_store_reader.h"

namespace cdc_ft {

Asset::Asset() = default;

Asset::~Asset() = default;

void Asset::Initialize(ino_t parent_ino, DataStoreReader* data_store_reader,
                       const AssetProto* proto) {
  parent_ino_ = parent_ino;

  assert(!data_store_reader_ && data_store_reader);
  data_store_reader_ = data_store_reader;

  assert(!proto_ && proto);
  proto_ = proto;

  // Create a lookup for the direct assets, if any.
  // Lock the mutex for convenience, it's not strictly necessary here as no
  // other thread has access to this object.
  absl::WriterMutexLock lock(&mutex_);
  UpdateProtoLookup(proto_->dir_assets());
}

absl::StatusOr<std::vector<const AssetProto*>> Asset::GetAllChildProtos() {
  mutex_.AssertNotHeld();
  assert(proto_);

  if (proto_->type() != AssetProto::DIRECTORY) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Asset '%s' is not a directory asset", proto_->name()));
  }

  // Fetch all indirect dir asset lists.
  for (;;) {
    bool list_was_fetched;
    ASSIGN_OR_RETURN(list_was_fetched, FetchNextDirAssetList(),
                     "Failed to fetch directory assets");
    if (!list_was_fetched) break;
  }
  return GetLoadedChildProtos();
}

std::vector<const AssetProto*> Asset::GetLoadedChildProtos() const {
  absl::ReaderMutexLock read_lock(&mutex_);

  // Push all directory asset protos to a vector.
  std::vector<const AssetProto*> protos;
  protos.reserve(proto_lookup_.size());
  for (const std::pair<const absl::string_view, const AssetProto*>& kv :
       proto_lookup_) {
    protos.push_back(kv.second);
  }
  return protos;
}

absl::StatusOr<const AssetProto*> Asset::Lookup(const char* name) {
  mutex_.AssertNotHeld();
  assert(proto_);
  if (proto_->type() != AssetProto::DIRECTORY) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Asset '%s' is not a directory asset", proto_->name()));
  }

  for (;;) {
    {
      absl::ReaderMutexLock read_lock(&mutex_);

      // Check if we already have the asset.
      std::unordered_map<absl::string_view, const AssetProto*>::iterator it =
          proto_lookup_.find(name);
      if (it != proto_lookup_.end()) {
        return it->second;
      }
    }

    // Fetch one more indirect asset list.
    bool list_was_fetched;
    ASSIGN_OR_RETURN(list_was_fetched, FetchNextDirAssetList(),
                     "Failed to fetch directory assets");
    if (!list_was_fetched) {
      // All lists were fetched, but asset still wasn't found.
      return nullptr;
    }
  }
}

absl::StatusOr<uint64_t> Asset::Read(uint64_t offset, void* data,
                                     uint64_t size) {
  mutex_.AssertNotHeld();
  assert(proto_);
  if (proto_->type() != AssetProto::FILE)
    return absl::InvalidArgumentError("Not a file asset");

  if (size == 0) return 0;

  // Find a chunk list such that list offset <= offset < next list offset.
  int list_idx = FindChunkList(offset);
  const RepeatedChunkRefProto* chunk_refs;
  ASSIGN_OR_RETURN(chunk_refs, GetChunkRefList(list_idx),
                   "Failed to fetch indirect chunk list %i", list_idx);
  uint64_t chunk_list_offset = ChunkListOffset(list_idx);
  if (!chunk_refs) return 0;  // Out of bounds.

  // Find a chunk such that chunk offset <= offset < next chunk offset.
  int chunk_idx = FindChunk(*chunk_refs, chunk_list_offset, offset);
  if (chunk_idx < 0 || chunk_idx >= chunk_refs->size()) {
    // Data is malformed, e.g. empty chunk list with non-zero file size.
    return MakeStatus(
        "Invalid chunk ref list %i. Found chunk index %i not in [0, %u).",
        list_idx, chunk_idx, chunk_refs->size());
  }

  uint64_t data_bytes_left = size;
  uint64_t prefetch_bytes_left = data_store_reader_->PrefetchSize(size);
  // Collect the chunk IDs required to satisfy the read request.
  ChunkTransferList chunks;
  while (chunk_refs) {
    const ChunkRefProto& chunk_ref = chunk_refs->at(chunk_idx);

    // Figure out how much data we have to read from the current chunk.
    uint64_t chunk_absolute_offset = chunk_list_offset + chunk_ref.offset();
    uint64_t chunk_offset =
        offset > chunk_absolute_offset ? offset - chunk_absolute_offset : 0;
    uint64_t chunk_size = ChunkSize(list_idx, chunk_idx, chunk_refs);
    assert(chunk_size >= chunk_offset);
    uint64_t bytes_to_read =
        std::min<uint64_t>(chunk_size - chunk_offset, data_bytes_left);
    uint64_t bytes_to_prefetch =
        std::min<uint64_t>(chunk_size - chunk_offset, prefetch_bytes_left);

    // Enqueue a chunk transfer task.
    chunks.emplace_back(chunk_ref.chunk_id(), chunk_offset,
                        bytes_to_read ? data : nullptr, bytes_to_read);
    data = static_cast<char*>(data) + bytes_to_read;
    data_bytes_left =
        data_bytes_left > bytes_to_read ? data_bytes_left - bytes_to_read : 0;
    prefetch_bytes_left -= bytes_to_prefetch;
    offset += bytes_to_prefetch;

    // If we request enough data, we are done.
    if (!prefetch_bytes_left) break;

    // Otherwise find next chunk.
    ++chunk_idx;
    while (chunk_idx >= chunk_refs->size()) {
      // Go to next list.
      chunk_idx = 0;
      ++list_idx;
      ASSIGN_OR_RETURN(chunk_refs, GetChunkRefList(list_idx),
                       "Failed to fetch indirect chunk list %i", list_idx);
      chunk_list_offset = ChunkListOffset(list_idx);
      if (!chunk_refs) {
        // Out of bounds. If we're not at the file size now, it's an error.
        if (offset != proto_->file_size()) {
          return MakeStatus(
              "Read error at position %u. Expected to be at file size %u.",
              offset, proto_->file_size());
        }
        break;
      }
    }

    if (chunk_refs) {
      // We should be exactly at a chunk boundary now.
      uint64_t chunk_rel_offset = chunk_refs->at(chunk_idx).offset();
      if (offset != chunk_list_offset + chunk_rel_offset) {
        return MakeStatus("Unexpected chunk offset %u, expected %u + %u = %u",
                          offset, chunk_list_offset, chunk_rel_offset,
                          chunk_list_offset + chunk_rel_offset);
      }
    }
  }

  // Read all data.
  absl::Status status = data_store_reader_->Get(&chunks);
  if (!status.ok() || !chunks.ReadDone()) {
    std::string msg = absl::StrFormat(
        "Failed to fetch chunk(s) [%s] for file '%s', offset %u, size %u",
        chunks.ToHexString(
            [](auto const& chunk) { return chunk.size && !chunk.done; }),
        proto_->name(), offset, size);
    return status.ok() ? absl::DataLossError(msg)
                       : WrapStatus(status, "%s", msg);
  }
  return size - data_bytes_left;
}

size_t Asset::GetNumFetchedFileChunkListsForTesting() {
  mutex_.AssertNotHeld();
  absl::ReaderMutexLock read_lock(&mutex_);

  // In contrast to |dir_asset_lists_|, |file_chunk_lists_| might be fetched
  // out-of-order, e.g. if someone tried to read the end of the file.
  // Unfetched lists are nullptrs.
  int num_fetched = 0;
  for (const std::unique_ptr<ChunkListProto>& list : file_chunk_lists_) {
    if (list) {
      ++num_fetched;
    }
  }
  return num_fetched;
}

size_t Asset::GetNumFetchedDirAssetsListsForTesting() {
  mutex_.AssertNotHeld();
  absl::ReaderMutexLock read_lock(&mutex_);

  return dir_asset_lists_.size();
}

void Asset::UpdateProto(const AssetProto* proto) {
  absl::WriterMutexLock write_lock(&mutex_);
  proto_lookup_.clear();
  file_chunk_lists_.clear();
  dir_asset_lists_.clear();
  proto_ = proto;
  if (proto_) {
    UpdateProtoLookup(proto_->dir_assets());
  }
}

bool Asset::IsConsistent(std::string* warning) const {
  assert(proto_ && warning);
  absl::ReaderMutexLock read_lock(&mutex_);
  switch (proto_->type()) {
    case AssetProto::FILE:
      if (!proto_lookup_.empty() || !proto_->dir_assets().empty() ||
          !proto_->dir_indirect_assets().empty()) {
        *warning = "File asset contains sub-assets";
        return false;
      }
      if (!proto_->symlink_target().empty()) {
        *warning = "File asset contains a symlink";
        return false;
      }
      break;
    case AssetProto::DIRECTORY:
      if (!proto_->file_chunks().empty() || !file_chunk_lists_.empty() ||
          !proto_->file_indirect_chunks().empty()) {
        *warning = "Directory asset contains file chunks";
        return false;
      }
      if (!proto_->symlink_target().empty()) {
        *warning = "Directory asset contains a symlink";
        return false;
      }
      if (proto_->file_size() > 0) {
        *warning = "File size is defined for a directory asset";
        return false;
      }
      break;
    case AssetProto::SYMLINK:
      if (!proto_lookup_.empty() || !proto_->dir_assets().empty() ||
          !proto_->dir_indirect_assets().empty()) {
        *warning = "Symlink asset contains sub-assets";
        return false;
      }
      if (!proto_->file_chunks().empty() || !file_chunk_lists_.empty() ||
          !proto_->file_indirect_chunks().empty()) {
        *warning = "Symlink asset contains file chunks";
        return false;
      }
      if (proto_->file_size() > 0) {
        *warning = "File size is defined for a symlink asset";
        return false;
      }
      break;
    default:
      *warning = "Undefined asset type";
      return false;
  }

  // Directory assets should not have any file chunks.
  // Absolute file chunk offsets for all loaded direct and indirect chunks
  // should be monotonically increasing.
  if (proto_->type() == AssetProto::FILE) {
    // Check direct chunks.
    size_t total_offset = 0;
    for (int idx = 0; idx < proto_->file_chunks_size(); ++idx) {
      if (proto_->file_chunks(idx).offset() < total_offset) {
        *warning = absl::StrFormat(
            "Disordered direct chunks: idx=%u, total_offset=%u, "
            "chunk_offset=%u",
            idx, total_offset, proto_->file_chunks(idx).offset());
        return false;
      }
      total_offset = proto_->file_chunks(idx).offset();
    }

    // Check indirect lists.
    size_t prev_list_offset = total_offset;
    for (int list_idx = 0; list_idx < proto_->file_indirect_chunks_size();
         ++list_idx) {
      size_t list_offset = ChunkListOffset(list_idx);
      if (list_idx == 0 && proto_->file_chunks_size() == 0 &&
          list_offset != 0) {
        *warning = absl::StrFormat(
            "Disordered indirect chunk list: the list offset should be 0, as "
            "there are no direct file chunks: "
            "list_offset=%u, previous list_offset=%u",
            list_offset, prev_list_offset);
        return false;
      } else if (list_idx > 0 && (prev_list_offset >= list_offset ||
                                  total_offset >= list_offset)) {
        *warning = absl::StrFormat(
            "Disordered indirect chunk list: the list offset should increase: "
            "list_offset=%u, previous list_offset=%u, total_offset=%u",
            list_offset, prev_list_offset, total_offset);
        return false;
      }
      if (file_chunk_lists_.size() <= list_idx ||
          !file_chunk_lists_[list_idx]) {
        total_offset = list_offset;
        continue;
      }
      // If the list is fetched, check its chunks' order.
      for (int chunk_idx = 0;
           chunk_idx < file_chunk_lists_[list_idx]->chunks_size();
           ++chunk_idx) {
        const ChunkRefProto& chunk =
            file_chunk_lists_[list_idx]->chunks(chunk_idx);
        if (chunk_idx == 0 && chunk.offset() != 0) {
          *warning = absl::StrFormat(
              "The offset of the first chunk in the list should be 0: "
              "list_idx=%u, list_offset=%u, chunk_offset=%u",
              list_idx, list_offset, chunk.offset());
          return false;
        }
        if (chunk.offset() + list_offset < total_offset) {
          *warning = absl::StrFormat(
              "Disordered indirect chunk list: list_idx=%u, list_offset=%u, "
              "offset=%u, chunk_offset=%u",
              list_idx, list_offset, total_offset, chunk.offset());
          return false;
        }
        total_offset = list_offset + chunk.offset();
      }
    }
    if (total_offset == 0 && proto_->file_size() == 0) {
      return true;
    }
    // The last absolute offset should be less than the file size.
    if (total_offset >= proto_->file_size()) {
      *warning = absl::StrFormat(
          "The last absolute file offset exceeds the file size: %u >= %u",
          total_offset, proto_->file_size());
      return false;
    }
  }
  return true;
}

absl::StatusOr<bool> Asset::FetchNextDirAssetList() {
  mutex_.AssertNotHeld();
  assert(proto_);

  {
    absl::ReaderMutexLock read_lock(&mutex_);

    // Shortcut to prevent acquiring a write lock if everything has been loaded.
    if (dir_asset_lists_.size() >=
        static_cast<size_t>(proto_->dir_indirect_assets_size())) {
      return false;
    }
  }

  absl::WriterMutexLock write_lock(&mutex_);

  // Check again in case some other thread has run this in the meantime.
  if (dir_asset_lists_.size() >=
      static_cast<size_t>(proto_->dir_indirect_assets_size())) {
    return false;
  }

  // Read next indirect asset list.
  const ContentIdProto& id =
      proto_->dir_indirect_assets(static_cast<int>(dir_asset_lists_.size()));
  auto list = std::make_unique<AssetListProto>();
  RETURN_IF_ERROR(data_store_reader_->GetProto(id, list.get()),
                  "Failed to fetch AssetList proto with id %s",
                  ContentId::ToHexString(id));
  dir_asset_lists_.push_back(std::move(list));
  UpdateProtoLookup(dir_asset_lists_.back()->assets());

  return true;
}

void Asset::UpdateProtoLookup(const RepeatedAssetProto& list) {
  assert((mutex_.AssertHeld(), true));

  for (const AssetProto& asset : list) {
    proto_lookup_[asset.name().c_str()] = &asset;
  }
}

int Asset::FindChunkList(uint64_t offset) {
  assert(proto_);
  const RepeatedIndirectChunkListProto& lists = proto_->file_indirect_chunks();
  if (offset >= proto_->file_size()) {
    // |offset| is not inside the file.
    return proto_->file_indirect_chunks_size();
  }

  // TODO: Optimize search by using average chunk size.
  auto it =
      std::upper_bound(lists.begin(), lists.end(), offset,
                       [](uint64_t value, const IndirectChunkListProto& list) {
                         return value < list.offset();
                       });
  return it - lists.begin() - 1;
}

int Asset::FindChunk(const RepeatedChunkRefProto& chunks,
                     uint64_t chunk_list_offset, uint64_t chunk_offset) {
  assert(chunk_list_offset <= chunk_offset);
  uint64_t rel_offset = chunk_offset - chunk_list_offset;
  // TODO: Optimize search by using average chunk size.
  auto it = std::upper_bound(chunks.begin(), chunks.end(), rel_offset,
                             [](uint64_t value, const ChunkRefProto& ch) {
                               return value < ch.offset();
                             });
  return it - chunks.begin() - 1;
}

uint64_t Asset::ChunkListOffset(int list_idx) const {
  assert(list_idx >= -1 && proto_ &&
         list_idx <= proto_->file_indirect_chunks_size());

  if (list_idx == -1) return 0;
  if (list_idx < proto_->file_indirect_chunks_size())
    return proto_->file_indirect_chunks(list_idx).offset();
  return proto_->file_size();
}

uint64_t Asset::ChunkSize(int list_idx, int chunk_idx,
                          const RepeatedChunkRefProto* chunk_refs) {
  assert(chunk_idx >= 0 && chunk_idx < chunk_refs->size());
  assert(list_idx >= -1 && proto_ &&
         list_idx <= proto_->file_indirect_chunks_size());

  // If the next chunk is in the same chunk_refs list, just return offset diff.
  if (chunk_idx + 1 < chunk_refs->size()) {
    return chunk_refs->at(chunk_idx + 1).offset() -
           chunk_refs->at(chunk_idx).offset();
  }

  // If the next chunk is on another list, use the next list's offset.
  // Note that this also works for the last list, where
  // GetChunkListOffset(list_idx + 1) returns the file size.
  uint64_t chunk_absolute_offset =
      chunk_refs->at(chunk_idx).offset() + ChunkListOffset(list_idx);
  return ChunkListOffset(list_idx + 1) - chunk_absolute_offset;
}

absl::StatusOr<const RepeatedChunkRefProto*> Asset::GetChunkRefList(
    int list_idx) {
  mutex_.AssertNotHeld();
  assert(list_idx >= -1 && proto_ &&
         list_idx <= proto_->file_indirect_chunks_size());

  if (list_idx == -1) {
    // Direct chunk list.
    return &proto_->file_chunks();
  }

  if (list_idx == proto_->file_indirect_chunks_size()) {
    // Indicates EOF.
    return nullptr;
  }

  {
    absl::ReaderMutexLock read_lock(&mutex_);

    // Do a quick check first if the list is already loaded.
    // This only requires a read lock.
    if (static_cast<size_t>(list_idx) < file_chunk_lists_.size() &&
        file_chunk_lists_[list_idx]) {
      return &file_chunk_lists_[list_idx]->chunks();
    }
  }

  absl::WriterMutexLock write_lock(&mutex_);

  // Indirect chunk list. Check if it has to be fetched.
  if (file_chunk_lists_.size() < static_cast<size_t>(list_idx) + 1) {
    file_chunk_lists_.resize(list_idx + 1);
  }
  if (!file_chunk_lists_[list_idx]) {
    auto list = std::make_unique<ChunkListProto>();
    const ContentIdProto& list_id =
        proto_->file_indirect_chunks(list_idx).chunk_list_id();
    RETURN_IF_ERROR(data_store_reader_->GetProto(list_id, list.get()),
                    "Failed to fetch ChunkListProto with id %s",
                    ContentId::ToHexString(list_id));
    file_chunk_lists_[list_idx] = std::move(list);
  }
  return &file_chunk_lists_[list_idx]->chunks();
}

}  // namespace cdc_ft
