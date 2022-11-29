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

#include "cdc_stream/session_manager.h"

#include "absl/strings/str_split.h"
#include "cdc_stream/multi_session.h"
#include "common/log.h"
#include "common/process.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "manifest/manifest_updater.h"

namespace cdc_ft {
namespace {

// Returns a key to uniquely map a streaming directory |src_dir| to a
// MultiSession instance.
std::string GetMultiSessionKey(const std::string src_dir) {
  // Use the cache dir as a key to identify MultiSessions. That way, different
  // representations of the same dir (e.g. dir and dir\) map to the same
  // MultiSession.
  return MultiSession::GetCacheDir(src_dir);
}

}  // namespace

SessionManager::SessionManager(SessionConfig cfg,
                               ProcessFactory* process_factory,
                               metrics::MetricsService* metrics_service)
    : cfg_(cfg),
      process_factory_(process_factory),
      metrics_service_(metrics_service) {}

SessionManager::~SessionManager() = default;

absl::Status SessionManager::Shutdown() {
  absl::MutexLock lock(&sessions_mutex_);

  for (const auto& [key, ms] : sessions_) {
    LOG_INFO("Shutting down MultiSession for path '%s'", ms->src_dir());
    RETURN_IF_ERROR(ms->Shutdown(),
                    "Failed to shut down MultiSession for path '%s'",
                    ms->src_dir());
  }
  sessions_.clear();
  return absl::OkStatus();
}

absl::Status SessionManager::StartSession(
    const std::string& instance_id, const std::string& src_dir,
    const SessionTarget& target, const std::string& project_id,
    const std::string& organization_id, MultiSession** multi_session,
    metrics::SessionStartStatus* metrics_status) {
  *multi_session = nullptr;
  *metrics_status = metrics::SessionStartStatus::kOk;

  absl::MutexLock lock(&sessions_mutex_);

  // Check if the directory is correct as early as possible.
  absl::Status status = ManifestUpdater::IsValidDir(src_dir);
  if (!status.ok()) {
    absl::Status stop_status = StopSessionInternal(instance_id);
    if (!stop_status.ok() && !absl::IsNotFound(stop_status)) {
      LOG_ERROR("Failed to stop previous session for instance '%s': '%s'",
                instance_id, stop_status.ToString());
    }
    *metrics_status = metrics::SessionStartStatus::kInvalidDirError;
    return WrapStatus(status, "Failed to start session for path '%s'", src_dir);
  }

  // Early out if we are streaming the workstation dir to the given gamelet.
  MultiSession* ms = GetMultiSession(src_dir);
  *multi_session = ms;
  if (ms && ms->HasSession(instance_id)) {
    if (ms->IsSessionHealthy(instance_id)) {
      LOG_INFO("Reusing existing session");
      return absl::OkStatus();
    }

    LOG_INFO("Existing session for instance '%s' is not healthy. Restarting.",
             instance_id);

    // We could also fall through, but this might restart the MultiSession.
    status = ms->StopSession(instance_id);
    if (status.ok()) {
      status =
          ms->StartSession(instance_id, target, project_id, organization_id);
    }
    if (!status.ok()) {
      *metrics_status = metrics::SessionStartStatus::kRestartSessionError;
    }
    return WrapStatus(status, "Failed to restart session for instance '%s'",
                      instance_id);
  }

  // If we are already streaming to the given gamelet, but from another
  // workstation directory, stop that session.
  // Note that NotFoundError is OK and expected (it means no session exists).
  status = StopSessionInternal(instance_id);
  if (!status.ok() && !absl::IsNotFound(status)) {
    *metrics_status = metrics::SessionStartStatus::kStopSessionError;
    return WrapStatus(status,
                      "Failed to stop previous session for instance '%s'",
                      instance_id);
  }

  // Get or create the MultiSession for the given workstation directory.
  absl::StatusOr<MultiSession*> ms_res = GetOrCreateMultiSession(src_dir);
  if (!ms_res.ok()) {
    *metrics_status = metrics::SessionStartStatus::kCreateMultiSessionError;
    return WrapStatus(ms_res.status(),
                      "Failed to create MultiSession for path '%s'", src_dir);
  }
  ms = ms_res.value();
  *multi_session = ms;

  // Start the session.
  LOG_INFO("Starting streaming session from path '%s' to instance '%s'",
           src_dir, instance_id);
  status = ms->StartSession(instance_id, target, project_id, organization_id);
  if (!status.ok()) {
    *metrics_status = metrics::SessionStartStatus::kStartSessionError;
  }
  return status;
}

absl::Status SessionManager::StopSession(
    const std::string& instance_id_filter) {
  absl::MutexLock lock(&sessions_mutex_);

  std::vector<std::string> instance_ids;
  for (const auto& [key, ms] : sessions_) {
    auto ids = ms->MatchSessions(instance_id_filter);
    instance_ids.insert(instance_ids.end(), ids.begin(), ids.end());
  }
  if (instance_ids.empty()) {
    return absl::NotFoundError(
        absl::StrFormat("No session found matching '%s'", instance_id_filter));
  }

  for (const std::string& instance_id : instance_ids) {
    RETURN_IF_ERROR(StopSessionInternal(instance_id));
  }
  return absl::OkStatus();
}

MultiSession* SessionManager::GetMultiSession(const std::string& src_dir) {
  const std::string key = GetMultiSessionKey(src_dir);
  SessionMap::iterator iter = sessions_.find(key);
  return iter != sessions_.end() ? iter->second.get() : nullptr;
}

absl::StatusOr<MultiSession*> SessionManager::GetOrCreateMultiSession(
    const std::string& src_dir) {
  const std::string key = GetMultiSessionKey(src_dir);
  SessionMap::iterator iter = sessions_.find(key);
  if (iter == sessions_.end()) {
    LOG_INFO("Creating new MultiSession for path '%s'", src_dir);
    auto ms = std::make_unique<MultiSession>(
        src_dir, cfg_, process_factory_,
        new MultiSessionMetricsRecorder(metrics_service_));
    RETURN_IF_ERROR(ms->Initialize(), "Failed to initialize MultiSession");
    iter = sessions_.insert({key, std::move(ms)}).first;
  }

  return iter->second.get();
}

absl::Status SessionManager::StopSessionInternal(
    const std::string& instance_id) {
  absl::Status status;
  for (const auto& [key, ms] : sessions_) {
    if (!ms->HasSession(instance_id)) continue;

    LOG_INFO("Stopping session streaming from '%s' to instance '%s'",
             ms->src_dir(), instance_id);
    RETURN_IF_ERROR(ms->StopSession(instance_id),
                    "Failed to stop session for instance '%s'", instance_id);

    // Session was stopped. If the MultiSession is empty now, delete it.
    if (ms->Empty()) {
      LOG_INFO("Shutting down MultiSession for path '%s'", ms->src_dir());
      RETURN_IF_ERROR(ms->Shutdown(),
                      "Failed to shut down MultiSession for path '%s'",
                      ms->src_dir());
      sessions_.erase(key);
    }

    return absl::OkStatus();
  }

  return absl::NotFoundError(
      absl::StrFormat("No session for instance '%s' found", instance_id));
}

}  // namespace cdc_ft
