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

#ifndef CDC_STREAM_BACKGROUND_SERVICE_CLIENT_H_
#define CDC_STREAM_BACKGROUND_SERVICE_CLIENT_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/background_service.grpc.pb.h"

namespace grpc_impl {
class Channel;
}

namespace cdc_ft {

// gRpc client for managing the asset streaming service.
class BackgroundServiceClient {
 public:
  // |channel| is a grpc channel to use.
  explicit BackgroundServiceClient(std::shared_ptr<grpc::Channel> channel);

  ~BackgroundServiceClient();

  // Initialize service shutdown.
  absl::Status Exit();

  // Returns the PID of the service process.
  absl::StatusOr<int> GetPid();

  // Verifies that the service is running and able to take requests.
  absl::Status IsHealthy();

 private:
  using BackgroundService = backgroundservice::BackgroundService;
  std::unique_ptr<BackgroundService::Stub> stub_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_BACKGROUND_SERVICE_CLIENT_H_
