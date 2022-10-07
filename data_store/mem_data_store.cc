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

#include "data_store/mem_data_store.h"

#include "common/status.h"

namespace cdc_ft {

MemDataStore::MemDataStore() = default;

MemDataStore::~MemDataStore() = default;

ContentIdProto MemDataStore::AddData(std::vector<char> data) {
  ContentIdProto id = ContentId::FromArray(data.data(), data.size());
  data_lookup_[id] = std::move(data);
  return id;
}

ContentIdProto MemDataStore::AddProto(
    const google::protobuf::MessageLite& message) {
  std::vector<char> data;
  data.resize(message.ByteSizeLong());
  message.SerializeToArray(data.data(), static_cast<int>(data.size()));
  return AddData(std::move(data));
}

absl::StatusOr<size_t> MemDataStore::Get(const ContentIdProto& id, void* data,
                                         size_t offset, size_t size) {
  auto it = data_lookup_.find(id);
  if (it == data_lookup_.end()) {
    return absl::NotFoundError(absl::StrFormat("Failed to find data id '%s'",
                                               ContentId::ToHexString(id)));
  }

  const std::vector<char>& data_vec = it->second;
  if (offset >= data_vec.size()) {
    return 0;
  }

  uint64_t bytes_to_copy = std::min<uint64_t>(data_vec.size() - offset, size);
  memcpy(data, data_vec.data() + offset, bytes_to_copy);
  return bytes_to_copy;
}

absl::Status MemDataStore::Get(const ContentIdProto& id, Buffer* data) {
  auto it = data_lookup_.find(id);
  if (it == data_lookup_.end()) {
    return absl::NotFoundError(absl::StrFormat("Failed to find data id '%s'",
                                               ContentId::ToHexString(id)));
  }

  const std::vector<char>& data_vec = it->second;
  data->clear();
  data->append(data_vec.data(), data_vec.size());
  return absl::OkStatus();
}

absl::Status MemDataStore::Get(ChunkTransferList* chunks) {
  for (ChunkTransferTask& chunk : *chunks) {
    if (chunk.done) continue;
    auto it = data_lookup_.find(chunk.id);
    if (it == data_lookup_.end()) continue;
    // Copy the potentially prefetched string for caching.
    chunk.chunk_data = std::string(it->second.data(), it->second.size());
    if (!chunk.size) {
      chunk.done = true;
      continue;
    }

    if (chunk.offset >= chunk.chunk_data.size()) {
      return absl::OutOfRangeError(absl::StrFormat(
          "Chunk '%s': requested offset %u is larger or equal than size %u",
          ContentId::ToHexString(chunk.id), chunk.offset,
          chunk.chunk_data.size()));
    }
    uint64_t bytes_to_copy =
        std::min<uint64_t>(chunk.chunk_data.size() - chunk.offset, chunk.size);
    if (bytes_to_copy < chunk.size) {
      return absl::DataLossError(
          absl::StrFormat("Chunk '%s': requested size %u at offset %u is "
                          "larger than chunk size %u",
                          ContentId::ToHexString(chunk.id), chunk.size,
                          chunk.offset, chunk.chunk_data.size()));
    }
    memcpy(chunk.data, chunk.chunk_data.data() + chunk.offset, bytes_to_copy);
    chunk.done = true;
  }

  return absl::OkStatus();
}

bool MemDataStore::Contains(const ContentIdProto& content_id) {
  return data_lookup_.find(content_id) != data_lookup_.end();
}

absl::Status MemDataStore::Put(const ContentIdProto& content_id,
                               const void* data, size_t size) {
  data_lookup_[content_id] =
      std::vector<char>(reinterpret_cast<const char*>(data),
                        reinterpret_cast<const char*>(data) + size);
  return absl::OkStatus();
}

absl::Status MemDataStore::Remove(const ContentIdProto& content_id) {
  data_lookup_.erase(content_id);
  return absl::OkStatus();
}

absl::Status MemDataStore::Wipe() {
  data_lookup_.clear();
  return absl::OkStatus();
}

absl::Status MemDataStore::Prune(
    std::unordered_set<ContentIdProto> ids_to_keep) {
  // Find the set of chunks not in |ids_to_keep|.
  std::vector<ContentIdProto> to_delete;
  for (const auto& [id, _] : data_lookup_) {
    if (ids_to_keep.find(id) == ids_to_keep.end())
      to_delete.push_back(id);
    else
      ids_to_keep.erase(id);
  }

  // Delete chunks not in |ids_to_keep|.
  for (const ContentIdProto& id : to_delete) {
    data_lookup_.erase(id);
  }

  // Verify that all chunks in |ids_to_keep| are present in the store.
  if (!ids_to_keep.empty()) {
    return absl::NotFoundError(absl::StrFormat(
        "%u chunks, e.g. '%s', not found in the store", ids_to_keep.size(),
        ContentId::ToHexString(*ids_to_keep.begin())));
  }
  return absl::OkStatus();
}

}  // namespace cdc_ft
