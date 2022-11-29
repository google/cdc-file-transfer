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

#ifndef CDC_STREAM_BACKGROUND_SERVICE_IMPL_H_
#define CDC_STREAM_BACKGROUND_SERVICE_IMPL_H_

#include <memory>
#include <thread>

#include "absl/status/status.h"
#include "cdc_stream/background_service_impl.h"
#include "cdc_stream/session_management_server.h"
#include "grpcpp/grpcpp.h"
#include "proto/background_service.grpc.pb.h"

namespace cdc_ft {

// Implements a service to manage a background process as a server.
// This service is owned by SessionManagementServer.
class BackgroundServiceImpl final
    : public backgroundservice::BackgroundService::Service {
 public:
  using GetPidResponse = backgroundservice::GetPidResponse;
  using EmptyProto = google::protobuf::Empty;

  BackgroundServiceImpl();
  ~BackgroundServiceImpl();

  // Exit callback gets called from the Exit() RPC.
  using ExitCallback = std::function<absl::Status()>;
  void SetExitCallback(ExitCallback exit_callback);

  grpc::Status Exit(grpc::ServerContext* context, const EmptyProto* request,
                    EmptyProto* response) override;

  grpc::Status GetPid(grpc::ServerContext* context, const EmptyProto* request,
                      GetPidResponse* response) override;

  grpc::Status HealthCheck(grpc::ServerContext* context,
                           const EmptyProto* request,
                           EmptyProto* response) override;

 private:
  ExitCallback exit_callback_;
  std::unique_ptr<std::thread> exit_thread_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_BACKGROUND_SERVICE_IMPL_H_
