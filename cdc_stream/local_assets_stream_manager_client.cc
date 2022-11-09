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

#include "cdc_stream/local_assets_stream_manager_client.h"

#include "absl/status/status.h"

namespace cdc_ft {

LocalAssetsStreamManagerClient::LocalAssetsStreamManagerClient(
    std::shared_ptr<grpc::Channel> channel) {
  stub_ = LocalAssetsStreamManager::NewStub(std::move(channel));
}

LocalAssetsStreamManagerClient::~LocalAssetsStreamManagerClient() = default;

absl::Status LocalAssetsStreamManagerClient::StartSession() {
  return absl::OkStatus();
}

absl::Status LocalAssetsStreamManagerClient::StopSession() {
  return absl::OkStatus();
}

}  // namespace cdc_ft
