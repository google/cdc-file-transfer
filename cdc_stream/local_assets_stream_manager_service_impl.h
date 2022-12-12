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

#ifndef CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_SERVICE_IMPL_H_
#define CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_SERVICE_IMPL_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cdc_stream/session.h"
#include "cdc_stream/session_config.h"
#include "metrics/metrics.h"
#include "proto/local_assets_stream_manager.grpc.pb.h"

namespace cdc_ft {

class MultiSession;
class ProcessFactory;
class SessionManager;

// Implements a service to start and stop streaming sessions as a server.
// The corresponding clients are implemented by the ggp CLI and SDK Proxy.
// The CLI triggers StartSession() from `ggp instance mount --local-dir` and
// StopSession() from `ggp instance unmount`. SDK Proxy invokes StartSession()
// when a user starts a new game from the partner portal and sets an `Asset
// streaming directory` in the `Advanced settings` in the `Play settings`
// dialog.
// This service is owned by SessionManagementServer.
class LocalAssetsStreamManagerServiceImpl final
    : public localassetsstreammanager::LocalAssetsStreamManager::Service {
 public:
  using StartSessionRequest = localassetsstreammanager::StartSessionRequest;
  using StartSessionRequestOrigin =
      localassetsstreammanager::StartSessionRequest_Origin;
  using StartSessionResponse = localassetsstreammanager::StartSessionResponse;
  using StopSessionRequest = localassetsstreammanager::StopSessionRequest;
  using StopSessionResponse = localassetsstreammanager::StopSessionResponse;

  LocalAssetsStreamManagerServiceImpl(
      SessionManager* session_manager, ProcessFactory* process_factory,
      metrics::MetricsService* const metrics_service);
  ~LocalAssetsStreamManagerServiceImpl();

  // Starts a streaming session from path |request->workstation_directory()| to
  // the instance with id |request->gamelet_id()|. Stops an existing session
  // if it exists.
  grpc::Status StartSession(grpc::ServerContext* context,
                            const StartSessionRequest* request,
                            StartSessionResponse* response) override;

  // Stops the streaming session to the instance with id
  // |request->gamelet_id()|. Returns a NotFound error if no session exists.
  grpc::Status StopSession(grpc::ServerContext* context,
                           const StopSessionRequest* request,
                           StopSessionResponse* response) override;

 private:
  // Internal implementation of StartSession(). Returns the unique session
  // identifier |instance_id|, the created or retrieved MultiSession |ms| as
  // well as the filled metrics event |evt|.
  absl::Status LocalAssetsStreamManagerServiceImpl::StartSessionInternal(
      const StartSessionRequest* request, std::string* instance_id,
      MultiSession** ms, metrics::DeveloperLogEvent* evt);

  // Stadia-specific: Returns a SessionTarget from a gamelet name and fills in
  // the gamelet's |instance_id|, |project_id| and |organization_id|.
  // Used if request.gamelet_name() is set.
  // Fails if the gamelet name fails to parse or if ggp ssh init fails.
  absl::StatusOr<SessionTarget> GetTargetForStadia(
      const StartSessionRequest& request, std::string* instance_id,
      std::string* project_id, std::string* organization_id);

  // Returns a SessionTarget from the corresponding fields in |request|.
  // |instance_id| is set to [user@]host:mount_dir.
  // Used if request.gamelet_name() is not set.
  SessionTarget GetTarget(const StartSessionRequest& request,
                          std::string* instance_id);

  // Convert StartSessionRequest enum to metrics enum.
  metrics::RequestOrigin ConvertOrigin(StartSessionRequestOrigin origin) const;

  // Initializes an ssh connection to a gamelet by calling 'ggp ssh init'.
  // |instance_id| must be set, |project_id|, |organization_id| are optional.
  // Returns the instance's IP address.
  absl::StatusOr<std::string> InitSsh(const std::string& instance_id,
                                      const std::string& project_id,
                                      const std::string& organization_id);

  const SessionConfig cfg_;
  SessionManager* const session_manager_;
  ProcessFactory* const process_factory_;
  metrics::MetricsService* const metrics_service_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_SERVICE_IMPL_H_
