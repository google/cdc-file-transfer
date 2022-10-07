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

#ifndef METRICS_MESSAGES_H_
#define METRICS_MESSAGES_H_

#include "absl/status/status.h"
#include "metrics/enums.h"

namespace cdc_ft {
namespace metrics {

// Holds manifest update stats, status and trigger.
struct ManifestUpdateData {
  // Manifest update stats.
  uint64_t total_assets_added_or_updated;
  uint64_t total_files_added_or_updated;
  uint64_t total_files_failed;
  uint64_t total_assets_deleted;
  uint64_t total_chunks;
  uint64_t total_processed_bytes;
  // Duration of local manifest update.
  // This doesn't include any workstation-gamelet communication time.
  uint64_t local_duration_ms;
  UpdateTrigger trigger;
  absl::StatusCode status;
};

bool operator==(const ManifestUpdateData& lhs, const ManifestUpdateData& rhs);

bool operator!=(const ManifestUpdateData& lhs, const ManifestUpdateData& rhs);

// Contains data which is recorded on asset streaming session start.
struct SessionStartData {
  // Current number of simultaneous sessions in multisession
  // including the newly started one.
  uint32_t concurrent_session_count;
  // Session abseil start status.
  absl::StatusCode absl_status;
  // Session start status, which describes at which point
  // of execution it failed.
  SessionStartStatus status;
  // Where the StartSession request originated from.
  RequestOrigin origin;
};

bool operator==(const SessionStartData& lhs, const SessionStartData& rhs);

bool operator!=(const SessionStartData& lhs, const SessionStartData& rhs);

// Contains data which is recorded on asset streaming multisession start.
struct MultiSessionStartData {
  // Initial number of files in the streamed directory.
  uint32_t file_count;
  // Initial number of bytes in the streamed directory.
  uint64_t byte_count;
  // Initial total chunk count.
  uint64_t chunk_count;
  // Minimum chunk size CDC parameter.
  uint64_t min_chunk_size;
  // Average chunk size CDC parameter.
  uint64_t avg_chunk_size;
  // Maximum chunk size CDC parameter.
  uint64_t max_chunk_size;
};

bool operator==(const MultiSessionStartData& lhs,
                const MultiSessionStartData& rhs);

bool operator!=(const MultiSessionStartData& lhs,
                const MultiSessionStartData& rhs);

// Session Heartbeat data.
// It is sent every 5 minutes in case data has been changed
// and at the end of a session. The counters are cumulative, so in order
// to calculate total size of data sent during a particular session
// the value of the last event should be queried.
struct SessionData {
  // Number of chunks sent during the session so far.
  uint64_t chunk_count;
  // Number of bytes sent during the session so far.
  uint64_t byte_count;
};

bool operator==(const SessionData& lhs, const SessionData& rhs);

bool operator!=(const SessionData& lhs, const SessionData& rhs);

struct AssetStreamingManagerData {
  // Session Heartbeat data.
  std::unique_ptr<SessionData> session_data;
  // Holds manifest update stats, status and trigger.
  std::unique_ptr<ManifestUpdateData> manifest_update_data;
  // Contains data which is recorded on asset streaming session start.
  std::unique_ptr<SessionStartData> session_start_data;
  // Contains data which is recorded on asset streaming multisession start.
  std::unique_ptr<MultiSessionStartData> multi_session_start_data;
  // Randomly-generated whenever asset streaming multisession is created.
  std::string multisession_id;
  // Randomly-generated whenever asset streaming session is created.
  std::string session_id;
};

bool operator==(const AssetStreamingManagerData& lhs,
                const AssetStreamingManagerData& rhs);

bool operator!=(const AssetStreamingManagerData& lhs,
                const AssetStreamingManagerData& rhs);

// Contains developer log events, sent by GGP tools running on developer
// workstations.
struct DeveloperLogEvent {
  DeveloperLogEvent() = default;

  DeveloperLogEvent(DeveloperLogEvent&& other)
      : project_id(std::move(other.project_id)),
        organization_id(std::move(other.organization_id)),
        as_manager_data(std::move(other.as_manager_data)) {}

  DeveloperLogEvent& operator=(DeveloperLogEvent&& other) {
    project_id = std::move(other.project_id);
    organization_id = std::move(other.organization_id);
    as_manager_data = std::move(other.as_manager_data);
    return *this;
  }

  // GGP Project ID that is selected for the current session.
  std::string project_id;

  // GGP Publisher/Organization ID that is selected for the current session.
  std::string organization_id;

  // Describes asset streaming manager data.
  std::unique_ptr<AssetStreamingManagerData> as_manager_data;
};

bool operator==(const DeveloperLogEvent& lhs, const DeveloperLogEvent& rhs);

bool operator!=(const DeveloperLogEvent& lhs, const DeveloperLogEvent& rhs);

}  // namespace metrics
}  // namespace cdc_ft

#endif  // METRICS_MESSAGES_H_
