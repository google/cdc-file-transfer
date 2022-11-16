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

#ifndef CDC_FUSE_FS_CONFIG_STREAM_CLIENT_H_
#define CDC_FUSE_FS_CONFIG_STREAM_CLIENT_H_

#include <memory>

#include "absl/status/status.h"
#include "grpcpp/grpcpp.h"
#include "manifest/manifest_proto_defs.h"
#include "proto/asset_stream_service.grpc.pb.h"

namespace grpc_impl {
class Channel;
}

namespace cdc_ft {

class ManifestIdReader;

// Interface class for the config stream client.
class ConfigStreamClient {
 public:
  ConfigStreamClient() = default;
  virtual ~ConfigStreamClient() = default;

  // Sends a request to get a stream of manifest id updates. |callback| is
  // called from a background thread for every manifest id received.
  // Returns immediately without waiting for the first manifest id.
  virtual absl::Status StartListeningToManifestUpdates(
      std::function<absl::Status(const ContentIdProto&)> callback) = 0;

  // Sends a message to indicate that the |manifest_id| was received and FUSE
  // has been updated to use the new manifest.
  virtual absl::Status SendManifestAck(ContentIdProto manifest_id) = 0;

  // Sends a message to prioritize processing of the pending assets in |assets|.
  // All assets are given as full relative Unix paths to the file or directory.
  virtual absl::Status ProcessAssets(std::vector<std::string> assets) = 0;

  // Stops listening for manifest updates.
  virtual void Shutdown() = 0;
};

class ConfigStreamGrpcClient : public ConfigStreamClient {
 public:
  // |instance| is the id of the gamelet.
  // |channel| is a gRPC channel to use.
  ConfigStreamGrpcClient(std::string instance,
                         std::shared_ptr<grpc::Channel> channel);
  ~ConfigStreamGrpcClient();

  // ConfigStreamClient

  absl::Status StartListeningToManifestUpdates(
      std::function<absl::Status(const ContentIdProto&)> callback) override;
  absl::Status SendManifestAck(ContentIdProto manifest_id) override;
  absl::Status ProcessAssets(std::vector<std::string> assets) override;
  void Shutdown() override;

 private:
  using ConfigStreamService = proto::ConfigStreamService;

  const std::string instance_;
  const std::unique_ptr<ConfigStreamService::Stub> stub_;

  std::unique_ptr<ManifestIdReader> read_client_;
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_CONFIG_STREAM_CLIENT_H_
