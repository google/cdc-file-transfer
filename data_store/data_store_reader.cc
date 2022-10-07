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

#include "data_store/data_store_reader.h"

#include "absl/strings/str_format.h"
#include "common/status_macros.h"
#include "manifest/content_id.h"

namespace cdc_ft {

bool ChunkTransferList::ReadDone() const {
  for (auto it = begin(); it != end(); ++it) {
    if (it->size && !it->done) return false;
  }
  return true;
}

bool ChunkTransferList::PrefetchDone() const {
  for (auto it = begin(); it != end(); ++it) {
    if (!it->done) return false;
  }
  return true;
}

std::string ChunkTransferList::ToHexString(
    std::function<bool(const ChunkTransferTask&)> filter) const {
  std::string ids;
  for (auto it = begin(); it != end(); ++it) {
    if (filter && !filter(*it)) continue;
    if (!ids.empty()) ids += ", ";
    ids += ContentId::ToHexString(it->id);
  }
  return ids;
}

std::string ChunkTransferList::UndoneToHexString() const {
  return ToHexString(
      [](const ChunkTransferTask& chunk) { return !chunk.done; });
}

size_t DataStoreReader::PrefetchSize(size_t read_size) const {
  return read_size;
}

absl::Status DataStoreReader::Get(ChunkTransferList* chunks) {
  absl::StatusOr<uint64_t> bytes_read;
  for (ChunkTransferTask& chunk : *chunks) {
    // This default implementation skips prefetching tasks (chunk.size == 0).
    if (chunk.done || !chunk.size) continue;
    bytes_read = Get(chunk.id, chunk.data, chunk.offset, chunk.size);
    if (bytes_read.ok()) {
      if (*bytes_read != chunk.size) {
        return MakeStatus(
            "Corrupted chunk %s detected, expected to read %u bytes, got %u",
            ContentId::ToHexString(chunk.id), chunk.size, *bytes_read);
      }
      chunk.done = true;
    } else {
      // Return any unexpected error.
      if (!absl::IsNotFound(bytes_read.status())) return bytes_read.status();
    }
  }
  return absl::OkStatus();
}

absl::Status DataStoreReader::GetProto(const ContentIdProto& content_id,
                                       google::protobuf::Message* proto) {
  Buffer chunk;
  return GetProto(content_id, &chunk, proto);
}

absl::Status DataStoreReader::GetProto(const ContentIdProto& content_id,
                                       Buffer* buf,
                                       google::protobuf::Message* proto) {
  // Fetch the referenced chunk.
  RETURN_IF_ERROR(Get(content_id, buf));
  // Parse the manifest proto from the chunk.
  if (!proto->ParseFromArray(buf->data(), static_cast<int>(buf->size()))) {
    return absl::InternalError(absl::StrFormat(
        "Failed to parse %s from chunk '%s'", proto->GetTypeName(),
        ContentId::ToHexString(content_id)));
  }
  return absl::OkStatus();
}

}  // namespace cdc_ft
