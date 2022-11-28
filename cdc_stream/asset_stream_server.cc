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

#include "cdc_stream/asset_stream_server.h"

#include "cdc_stream/grpc_asset_stream_server.h"
#include "cdc_stream/testing_asset_stream_server.h"

namespace cdc_ft {

AssetStreamServer::AssetStreamServer(std::string src_dir,
                                     DataStoreReader* data_store_reader,
                                     FileChunkMap* file_chunks) {}

std::unique_ptr<AssetStreamServer> AssetStreamServer::Create(
    AssetStreamServerType type, std::string src_dir,
    DataStoreReader* data_store_reader, FileChunkMap* file_chunks,
    ContentSentHandler content_sent, PrioritizeAssetsHandler prio_assets) {
  switch (type) {
    case AssetStreamServerType::kGrpc:
      return std::make_unique<GrpcAssetStreamServer>(
          src_dir, data_store_reader, file_chunks, content_sent, prio_assets);
    case AssetStreamServerType::kTest:
      return std::make_unique<TestingAssetStreamServer>(
          src_dir, data_store_reader, file_chunks);
  }
  assert(false);
  return nullptr;
}

}  // namespace cdc_ft
