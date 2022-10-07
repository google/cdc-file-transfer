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

#include "cdc_fuse_fs/asset_stream_client.h"

#include <thread>

#include "common/log.h"
#include "common/stopwatch.h"

namespace cdc_ft {

using GetContentRequest = proto::GetContentRequest;
using GetContentResponse = proto::GetContentResponse;
using SendCachedContentIdsRequest = proto::SendCachedContentIdsRequest;
using SendCachedContentIdsResponse = proto::SendCachedContentIdsResponse;

AssetStreamClient::AssetStreamClient(std::shared_ptr<grpc::Channel> channel,
                                     bool enable_stats)
    : enable_stats_(enable_stats) {
  stub_ = AssetStreamService::NewStub(std::move(channel));
}

AssetStreamClient::~AssetStreamClient() = default;

size_t TotalDataSize(const RepeatedStringProto& data) {
  size_t total_size = 0;
  for (const std::string& s : data) {
    total_size += s.size();
  }
  return total_size;
}

absl::StatusOr<std::string> AssetStreamClient::GetContent(
    const ContentIdProto& id) {
  GetContentRequest request;
  *request.add_id() = id;
  if (enable_stats_)
    request.set_thread_id(thread_id_hash_(std::this_thread::get_id()));

  grpc::ClientContext context;
  GetContentResponse response;

  Stopwatch sw;
  grpc::Status status = stub_->GetContent(&context, request, &response);
  LOG_DEBUG("GRPC TIME %0.3f sec for %u chunks with %u bytes",
            sw.ElapsedSeconds(), response.data().size(),
            TotalDataSize(response.data()));

  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
  }
  assert(response.data_size() == 1);
  return std::move(*response.mutable_data(0));
}

absl::StatusOr<RepeatedStringProto> AssetStreamClient::GetContent(
    RepeatedContentIdProto chunk_ids) {
  if (chunk_ids.empty()) return RepeatedStringProto();

  GetContentRequest request;
  *request.mutable_id() = std::move(chunk_ids);
  if (enable_stats_)
    request.set_thread_id(thread_id_hash_(std::this_thread::get_id()));

  grpc::ClientContext context;
  GetContentResponse response;

  Stopwatch sw;
  grpc::Status status = stub_->GetContent(&context, request, &response);

  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
  }
  LOG_DEBUG("GRPC TIME %0.3f sec for %zu bytes", sw.ElapsedSeconds(),
            TotalDataSize(response.data()));

  return std::move(*response.mutable_data());
}

absl::Status AssetStreamClient::SendCachedContentIds(
    std::vector<ContentIdProto> content_ids) {
  SendCachedContentIdsRequest request;
  for (ContentIdProto& id : content_ids) *request.add_id() = std::move(id);

  grpc::ClientContext context;
  SendCachedContentIdsResponse response;

  grpc::Status status =
      stub_->SendCachedContentIds(&context, request, &response);
  if (!status.ok()) {
    return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                        status.error_message());
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
