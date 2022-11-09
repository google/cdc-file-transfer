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

#ifndef ASSET_STREAM_MANAGER_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_
#define ASSET_STREAM_MANAGER_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_

#include <memory>

#include "absl/status/status.h"
#include "grpcpp/channel.h"
#include "proto/local_assets_stream_manager.grpc.pb.h"

namespace grpc_impl {
class Channel;
}

namespace cdc_ft {

// gRpc client for starting/stopping asset streaming sessions.
class LocalAssetsStreamManagerClient {
 public:
  // |channel| is a grpc channel to use.
  explicit LocalAssetsStreamManagerClient(
      std::shared_ptr<grpc::Channel> channel);
  ~LocalAssetsStreamManagerClient();

  absl::Status StartSession();

  absl::Status StopSession();

 private:
  using LocalAssetsStreamManager =
      localassetsstreammanager::LocalAssetsStreamManager;
  std::unique_ptr<LocalAssetsStreamManager::Stub> stub_;
};

}  // namespace cdc_ft

#endif  // ASSET_STREAM_MANAGER_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_
