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

#ifndef CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_
#define CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_

#include "cdc_fuse_fs/config_stream_client.h"

namespace cdc_ft {

// Mock ConfigStreamClient implementation, used for testing only.
class MockConfigStreamClient : public ConfigStreamClient {
 public:
  // Returns the list of relative file paths to assets that have been
  // prioritized via ProcessAssets() and clears the list.
  std::vector<std::string> ReleasePrioritizedAssets();

  // ConfigStreamClient

  absl::Status StartListeningToManifestUpdates(
      std::function<absl::Status(const ContentIdProto&)> callback) override;
  absl::Status SendManifestAck(ContentIdProto manifest_id) override;
  absl::Status ProcessAssets(std::vector<std::string> assets) override;
  void Shutdown() override;

 private:
  std::vector<std::string> prioritized_assets_;
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_
