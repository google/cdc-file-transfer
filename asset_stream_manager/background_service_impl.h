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

#ifndef ASSET_STREAM_MANAGER_BACKGROUND_SERVICE_IMPL_H_
#define ASSET_STREAM_MANAGER_BACKGROUND_SERVICE_IMPL_H_

#include "absl/status/status.h"
#include "asset_stream_manager/background_service_impl.h"
#include "asset_stream_manager/session_management_server.h"
#include "grpcpp/grpcpp.h"
#include "proto/background_service.grpc.pb.h"

namespace cdc_ft {

// Implements a service to manage a background process as a server.
// The corresponding client is implemented by ProcessManager. The background
// process in this case is asset_stream_manager. ProcessManager starts the
// process on demand (e.g. when `ggp instance mount --local-dir` is invoked) and
// manages its lifetime: It calls GetPid() initially, HealthCheck() periodically
// to monitor the process, and Exit() on shutdown.
// This service is owned by SessionManagementServer.
class BackgroundServiceImpl final
    : public backgroundservice::BackgroundService::Service {
 public:
  using ExitRequest = backgroundservice::ExitRequest;
  using ExitResponse = backgroundservice::ExitResponse;
  using GetPidRequest = backgroundservice::GetPidRequest;
  using GetPidResponse = backgroundservice::GetPidResponse;
  using EmptyProto = google::protobuf::Empty;

  BackgroundServiceImpl();
  ~BackgroundServiceImpl();

  // Exit callback gets called from the Exit() RPC.
  using ExitCallback = std::function<absl::Status()>;
  void SetExitCallback(ExitCallback exit_callback);

  grpc::Status Exit(grpc::ServerContext* context, const ExitRequest* request,
                    ExitResponse* response) override;

  grpc::Status GetPid(grpc::ServerContext* context,
                      const GetPidRequest* request,
                      GetPidResponse* response) override;

  grpc::Status HealthCheck(grpc::ServerContext* context,
                           const EmptyProto* request,
                           EmptyProto* response) override;

 private:
  ExitCallback exit_callback_;
};

}  // namespace cdc_ft

#endif  // ASSET_STREAM_MANAGER_BACKGROUND_SERVICE_IMPL_H_
