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

#include "cdc_stream/testing_asset_stream_server.h"

#include "data_store/data_store_reader.h"
#include "manifest/file_chunk_map.h"

namespace cdc_ft {

TestingAssetStreamServer::TestingAssetStreamServer(
    std::string src_dir, DataStoreReader* data_store_reader,
    FileChunkMap* file_chunks)
    : AssetStreamServer(src_dir, data_store_reader, file_chunks) {}

TestingAssetStreamServer::~TestingAssetStreamServer() = default;

absl::Status TestingAssetStreamServer::Start(int port) {
  return absl::OkStatus();
}

void TestingAssetStreamServer::SetManifestId(
    const ContentIdProto& manifest_id) {
  absl::MutexLock lock(&mutex_);
  manifest_id_ = manifest_id;
}

absl::Status TestingAssetStreamServer::WaitForManifestAck(
    const std::string& instance, absl::Duration timeout) {
  return absl::OkStatus();
}

void TestingAssetStreamServer::Shutdown() {}

ContentIdProto TestingAssetStreamServer::GetManifestId() const {
  absl::MutexLock lock(&mutex_);
  return manifest_id_;
}
}  // namespace cdc_ft
