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

#ifndef ASSET_STREAM_MANAGER_SESSION_MANAGER_H_
#define ASSET_STREAM_MANAGER_SESSION_MANAGER_H_

#include <memory>
#include <unordered_map>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "asset_stream_manager/session_config.h"
#include "metrics/metrics.h"

namespace cdc_ft {

class MultiSession;
class ProcessFactory;

// Implements a service to start and stop streaming sessions as a server.
// The corresponding clients are implemented by the ggp CLI and SDK Proxy.
// The CLI triggers StartSession() from `ggp instance mount --local-dir` and
// StopSession() from `ggp instance unmount`. SDK Proxy invokes StartSession()
// when a user starts a new game from the partner portal and sets an `Asset
// streaming directory` in the `Advanced settings` in the `Play settings`
// dialog.
// This service is owned by SessionManagementServer.
class SessionManager {
 public:
  SessionManager(SessionConfig cfg, ProcessFactory* process_factory,
                 metrics::MetricsService* metrics_service);
  ~SessionManager();

  // Starts a session and populates |multi_session| and |metrics_status|.
  absl::Status StartSession(const std::string& instance_id,
                            const std::string& project_id,
                            const std::string& organization_id,
                            const std::string& instance_ip,
                            uint16_t instance_port, const std::string& src_dir,
                            MultiSession** multi_session,
                            metrics::SessionStartStatus* metrics_status)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Stops the session for the given |instance|. Returns a NotFound error if no
  // session exists.
  absl::Status StopSession(const std::string& instance)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Shuts down all existing MultiSessions.
  absl::Status Shutdown() ABSL_LOCKS_EXCLUDED(sessions_mutex_);

 private:
  // Stops the session for the given |instance|. Returns a NotFound error if no
  // session exists.
  absl::Status StopSessionInternal(const std::string& instance)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(sessions_mutex_);

  // Returns the MultiSession for the given workstation directory |src_dir| or
  // nullptr if it does not exist.
  MultiSession* GetMultiSession(const std::string& src_dir)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(sessions_mutex_);

  // Gets an existing MultiSession or creates a new one for the given
  // workstation directory |src_dir|.
  absl::StatusOr<MultiSession*> GetOrCreateMultiSession(
      const std::string& src_dir)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(sessions_mutex_);

  // Sets session start status for a metrics event.
  void SetSessionStartStatus(metrics::DeveloperLogEvent* evt,
                             absl::Status absl_status,
                             metrics::SessionStartStatus status) const;

  const SessionConfig cfg_;
  ProcessFactory* const process_factory_;
  metrics::MetricsService* const metrics_service_;

  absl::Mutex sessions_mutex_;
  using SessionMap =
      std::unordered_map<std::string, std::unique_ptr<MultiSession>>;
  SessionMap sessions_ ABSL_GUARDED_BY(sessions_mutex_);
};

}  // namespace cdc_ft

#endif  // ASSET_STREAM_MANAGER_SESSION_MANAGER_H_
