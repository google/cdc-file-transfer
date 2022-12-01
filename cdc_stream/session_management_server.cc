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

#include "cdc_stream/session_management_server.h"

#include "absl/strings/str_format.h"
#include "cdc_stream/background_service_impl.h"
#include "cdc_stream/local_assets_stream_manager_service_impl.h"
#include "cdc_stream/session_manager.h"
#include "common/log.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "grpcpp/grpcpp.h"

namespace cdc_ft {

SessionManagementServer::SessionManagementServer(
    grpc::Service* session_service, grpc::Service* background_service,
    SessionManager* session_manager)
    : session_service_(session_service),
      background_service_(background_service),
      session_manager_(session_manager) {}

SessionManagementServer::~SessionManagementServer() = default;

absl::Status SessionManagementServer::Start(int port) {
  assert(!server_);

  // Use 127.0.0.1 here to enforce IPv4. Otherwise, if only IPv4 is blocked on
  // |port|, the server is started with IPv6 only, but clients are connecting
  // with IPv4.
  int selected_port = 0;
  std::string server_address = absl::StrFormat("127.0.0.1:%i", port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(session_service_);
  builder.RegisterService(background_service_);
  server_ = builder.BuildAndStart();
  if (selected_port != port) {
    return MakeStatus(
        "Failed to start session management server: Could not listen on port "
        "%i. Is the port in use?",
        port);
  }
  if (!server_) {
    return MakeStatus(
        "Failed to start session management server. Check cdc_stream logs.");
  }
  LOG_INFO("Session management server listening on '%s'", server_address);
  return absl::OkStatus();
}

void SessionManagementServer::RunUntilShutdown() { server_->Wait(); }

absl::Status SessionManagementServer::Shutdown() {
  assert(server_);
  RETURN_IF_ERROR(session_manager_->Shutdown(),
                  "Failed to shut down session manager");
  server_->Shutdown();
  server_->Wait();
}

}  // namespace cdc_ft
