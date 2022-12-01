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

#ifndef CDC_STREAM_GRPC_ASSET_STREAM_SERVER_H_
#define CDC_STREAM_GRPC_ASSET_STREAM_SERVER_H_

#include <memory>
#include <string>

#include "cdc_stream/asset_stream_server.h"
#include "common/thread_safe_map.h"

namespace grpc {
class Server;
}

namespace cdc_ft {

using InstanceIdMap = ThreadSafeMap<std::string, std::string>;

class AssetStreamServiceImpl;
class ConfigStreamServiceImpl;

// gRpc server for streaming assets to one or more gamelets.
class GrpcAssetStreamServer : public AssetStreamServer {
 public:
  // Creates a new asset streaming gRpc server.
  GrpcAssetStreamServer(std::string src_dir, DataStoreReader* data_store_reader,
                        FileChunkMap* file_chunks,
                        ContentSentHandler content_sent,
                        PrioritizeAssetsHandler prio_assets);

  ~GrpcAssetStreamServer();

  // AssetStreamServer:

  absl::Status Start(int port) override;

  void SetManifestId(const ContentIdProto& manifest_id) override;

  absl::Status WaitForManifestAck(const std::string& instance,
                                  absl::Duration timeout) override;

  void Shutdown() override;

  ContentIdProto GetManifestId() const override;

 private:
  InstanceIdMap instance_ids_;
  const std::unique_ptr<AssetStreamServiceImpl> asset_stream_service_;
  const std::unique_ptr<ConfigStreamServiceImpl> config_stream_service_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_GRPC_ASSET_STREAM_SERVER_H_
