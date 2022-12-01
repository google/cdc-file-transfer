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

#ifndef CDC_STREAM_TESTING_ASSET_STREAM_SERVER_H_
#define CDC_STREAM_TESTING_ASSET_STREAM_SERVER_H_

#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "cdc_stream/grpc_asset_stream_server.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// Not thread-safe testing server for streaming assets.
class TestingAssetStreamServer : public AssetStreamServer {
 public:
  TestingAssetStreamServer(std::string src_dir,
                           DataStoreReader* data_store_reader,
                           FileChunkMap* file_chunks);

  ~TestingAssetStreamServer();

  // AssetStreamServer:

  absl::Status Start(int port) override;

  void SetManifestId(const ContentIdProto& manifest_id)
      ABSL_LOCKS_EXCLUDED(mutex_) override;

  absl::Status WaitForManifestAck(const std::string& instance,
                                  absl::Duration timeout) override;
  void Shutdown() override;

  ContentIdProto GetManifestId() const ABSL_LOCKS_EXCLUDED(mutex_) override;

 private:
  mutable absl::Mutex mutex_;
  ContentIdProto manifest_id_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_TESTING_ASSET_STREAM_SERVER_H_
