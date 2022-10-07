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

#ifndef CDC_FUSE_FS_ASSET_STREAM_CLIENT_H_
#define CDC_FUSE_FS_ASSET_STREAM_CLIENT_H_

#include <memory>
#include <string>
#include <thread>

#include "absl/status/statusor.h"
#include "grpcpp/channel.h"
#include "manifest/manifest_proto_defs.h"
#include "proto/asset_stream_service.grpc.pb.h"

namespace grpc_impl {
class Channel;
}

namespace cdc_ft {

// gRpc client for streaming assets to a gamelets. The client runs inside the
// CDC Fuse filesystem and requests chunks from the workstation.
class AssetStreamClient {
 public:
  // |channel| is a grpc channel to use.
  // |enable_stats| determines whether additional statistics are sent.
  AssetStreamClient(std::shared_ptr<grpc::Channel> channel, bool enable_stats);
  ~AssetStreamClient();

  // Gets the content of the chunk with given |id|.
  absl::StatusOr<std::string> GetContent(const ContentIdProto& id);
  absl::StatusOr<RepeatedStringProto> GetContent(
      RepeatedContentIdProto chunk_ids);

  // Sends the IDs of all cached chunks to the workstation for statistical
  // purposes.
  absl::Status SendCachedContentIds(std::vector<ContentIdProto> content_ids);

 private:
  using AssetStreamService = proto::AssetStreamService;
  std::unique_ptr<AssetStreamService::Stub> stub_;
  bool enable_stats_;
  std::hash<std::thread::id> thread_id_hash_;
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_ASSET_STREAM_CLIENT_H_
