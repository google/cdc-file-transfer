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

#include "cdc_stream/background_service_impl.h"

#include "common/grpc_status.h"
#include "common/log.h"
#include "common/util.h"
#include "grpcpp/grpcpp.h"

namespace cdc_ft {

BackgroundServiceImpl::BackgroundServiceImpl() {}

BackgroundServiceImpl::~BackgroundServiceImpl() {
  if (exit_thread_) {
    exit_thread_->join();
    exit_thread_.reset();
  }
}

void BackgroundServiceImpl::SetExitCallback(ExitCallback exit_callback) {
  exit_callback_ = std::move(exit_callback);
}

grpc::Status BackgroundServiceImpl::Exit(grpc::ServerContext* context,
                                         const EmptyProto* request,
                                         EmptyProto* response) {
  LOG_INFO("RPC:Exit");
  if (exit_callback_ && !exit_thread_) {
    // Fire up a thread so call the callback, since shutting down a server
    // won't finish until all RPCs are done.
    exit_thread_ =
        std::make_unique<std::thread>([cb = &exit_callback_]() { (*cb)(); });
  }
  return grpc::Status::OK;
}

grpc::Status BackgroundServiceImpl::GetPid(grpc::ServerContext* context,
                                           const EmptyProto* request,
                                           GetPidResponse* response) {
  LOG_INFO("RPC:GetPid");
  response->set_pid(static_cast<int32_t>(Util::GetPid()));
  return grpc::Status::OK;
}

grpc::Status BackgroundServiceImpl::HealthCheck(grpc::ServerContext* context,
                                                const EmptyProto* request,
                                                EmptyProto* response) {
  return grpc::Status::OK;
}

}  // namespace cdc_ft
