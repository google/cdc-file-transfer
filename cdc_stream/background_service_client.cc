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

#include "cdc_stream/background_service_client.h"

#include "absl/status/status.h"
#include "common/grpc_status.h"
#include "common/status_macros.h"
#include "grpcpp/channel.h"

namespace cdc_ft {

using GetPidResponse = backgroundservice::GetPidResponse;
using EmptyProto = google::protobuf::Empty;

BackgroundServiceClient::BackgroundServiceClient(
    std::shared_ptr<grpc::Channel> channel) {
  stub_ = BackgroundService::NewStub(std::move(channel));
}

BackgroundServiceClient::~BackgroundServiceClient() = default;

absl::Status BackgroundServiceClient::Exit() {
  EmptyProto request;
  EmptyProto response;
  grpc::ClientContext context;
  return ToAbslStatus(stub_->Exit(&context, request, &response));
}

absl::StatusOr<int> BackgroundServiceClient::GetPid() {
  EmptyProto request;
  GetPidResponse response;
  grpc::ClientContext context;
  RETURN_IF_ERROR(ToAbslStatus(stub_->GetPid(&context, request, &response)));
  return response.pid();
}

absl::Status BackgroundServiceClient::IsHealthy() {
  EmptyProto request;
  EmptyProto response;
  grpc::ClientContext context;
  return ToAbslStatus(stub_->HealthCheck(&context, request, &response));
}

}  // namespace cdc_ft
