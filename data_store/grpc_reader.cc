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

#include "data_store/grpc_reader.h"

#include <algorithm>

#include "cdc_fuse_fs/asset_stream_client.h"
#include "common/status.h"
#include "common/status_macros.h"

namespace cdc_ft {

GrpcReader::GrpcReader(std::shared_ptr<grpc::Channel> channel,
                       bool enable_stats)
    : client_(std::make_unique<AssetStreamClient>(std::move(channel),
                                                  enable_stats)) {}

GrpcReader::~GrpcReader() = default;

absl::Status GrpcReader::SendCachedContentIds(
    std::vector<ContentIdProto> content_ids) {
  return client_->SendCachedContentIds(std::move(content_ids));
}

absl::StatusOr<size_t> GrpcReader::Get(const ContentIdProto& id, void* data,
                                       uint64_t size, uint64_t offset) {
  absl::StatusOr<std::string> result = client_->GetContent(id);
  if (!result.ok()) {
    return WrapStatus(result.status(), "Failed to stream data for id %s",
                      ContentId::ToHexString(id));
  }
  if (offset >= result->size()) {
    return 0;
  }
  uint64_t bytes_to_copy = std::min<uint64_t>(result->size() - offset, size);
  memcpy(data, result->data() + offset, bytes_to_copy);
  return bytes_to_copy;
}

absl::Status GrpcReader::Get(ChunkTransferList* chunks) {
  RepeatedContentIdProto chunk_ids;
  for (const ChunkTransferTask& chunk : *chunks) {
    if (!chunk.done) *chunk_ids.Add() = chunk.id;
  };

  const int chunk_id_count = chunk_ids.size();
  RepeatedStringProto chunk_data;
  ASSIGN_OR_RETURN(chunk_data, client_->GetContent(std::move(chunk_ids)),
                   "Failed to stream data chunks [%s]",
                   chunks->UndoneToHexString());

  if (chunk_data.size() != chunk_id_count) {
    return MakeStatus(
        "Incomplete response received for chunks [%s], expected %u, got %u",
        chunks->UndoneToHexString(), chunk_id_count, chunk_data.size());
  }

  int i = 0;
  for (ChunkTransferTask& chunk : *chunks) {
    if (chunk.done) continue;
    // Move the complete chunk data over to the chunks list.
    chunk.chunk_data = std::move(chunk_data[i++]);
    // Verify the chunk size.
    if (chunk.chunk_data.size() < chunk.offset + chunk.size) {
      return MakeStatus(
          "Truncated chunk '%s' received, expected %u + %u = %u bytes, got %u",
          ContentId::ToHexString(chunk.id), chunk.offset, chunk.size,
          chunk.offset + chunk.size, chunk.chunk_data.size());
    }
    // Copy the part of the chunk data to the target buffer.
    if (chunk.data) {
      memcpy(chunk.data, chunk.chunk_data.data() + chunk.offset, chunk.size);
    }
    chunk.done = true;
  }

  return absl::OkStatus();
}

absl::Status GrpcReader::Get(const ContentIdProto& id, Buffer* data) {
  absl::StatusOr<std::string> result = client_->GetContent(id);
  if (!result.ok()) {
    return WrapStatus(result.status(), "Failed to stream data for id %s",
                      ContentId::ToHexString(id));
  }
  data->clear();
  data->append((*result).data(), (*result).size());
  return absl::OkStatus();
}

}  // namespace cdc_ft
