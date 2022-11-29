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

#ifndef CDC_STREAM_SESSION_MANAGEMENT_SERVER_H_
#define CDC_STREAM_SESSION_MANAGEMENT_SERVER_H_

#include <memory>

#include "absl/status/status.h"

namespace grpc {
class Server;
class Service;
}  // namespace grpc

namespace cdc_ft {

class SessionManager;
class ProcessFactory;

// gRPC server for managing streaming sessions. Contains these services:
// - LocalAssetsStreamManager
// - Background
class SessionManagementServer {
 public:
  static constexpr uint16_t kDefaultServicePort = 44432;

  SessionManagementServer(grpc::Service* session_service,
                          grpc::Service* background_service,
                          SessionManager* session_manager);
  ~SessionManagementServer();

  // Starts the server on the local port |port|.
  absl::Status Start(int port);

  // Waits until ProcessManager issues an Exit() request to the background
  // service.
  void RunUntilShutdown();

  // Shuts down the session manager and the server.
  absl::Status Shutdown();

 private:
  grpc::Service* session_service_;
  grpc::Service* background_service_;
  SessionManager* const session_manager_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_SESSION_MANAGEMENT_SERVER_H_
