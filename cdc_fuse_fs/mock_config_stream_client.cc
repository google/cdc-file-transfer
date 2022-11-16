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

#include "cdc_fuse_fs/mock_config_stream_client.h"

namespace cdc_ft {

std::vector<std::string> MockConfigStreamClient::ReleasePrioritizedAssets() {
  return std::move(prioritized_assets_);
}

absl::Status MockConfigStreamClient::StartListeningToManifestUpdates(
    std::function<absl::Status(const ContentIdProto&)> callback) {
  return absl::OkStatus();
}

absl::Status MockConfigStreamClient::SendManifestAck(
    ContentIdProto manifest_id) {
  return absl::OkStatus();
}

absl::Status MockConfigStreamClient::ProcessAssets(
    std::vector<std::string> assets) {
  prioritized_assets_.insert(prioritized_assets_.end(), assets.begin(),
                             assets.end());
  return absl::OkStatus();
}

void MockConfigStreamClient::Shutdown() {
  // Do nothing.
}

}  // namespace cdc_ft
