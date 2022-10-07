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

#include "metrics/messages.h"

namespace cdc_ft {
namespace metrics {

namespace {
template <typename T>
inline bool Eq(const std::unique_ptr<T>& lhs, const std::unique_ptr<T>& rhs) {
  return (!lhs && !rhs) || (lhs && rhs && *lhs == *rhs);
}
}  // namespace

bool operator==(const ManifestUpdateData& lhs, const ManifestUpdateData& rhs) {
  return lhs.total_assets_added_or_updated ==
             rhs.total_assets_added_or_updated &&
         lhs.total_files_added_or_updated == rhs.total_files_added_or_updated &&
         lhs.total_files_failed == rhs.total_files_failed &&
         lhs.total_assets_deleted == rhs.total_assets_deleted &&
         lhs.total_chunks == rhs.total_chunks &&
         lhs.total_processed_bytes == rhs.total_processed_bytes &&
         lhs.local_duration_ms == rhs.local_duration_ms &&
         lhs.trigger == rhs.trigger && lhs.status == rhs.status;
}

bool operator!=(const ManifestUpdateData& lhs, const ManifestUpdateData& rhs) {
  return !(lhs == rhs);
}

bool operator==(const SessionStartData& lhs, const SessionStartData& rhs) {
  return lhs.concurrent_session_count == rhs.concurrent_session_count &&
         lhs.absl_status == rhs.absl_status && lhs.status == rhs.status &&
         lhs.origin == rhs.origin;
}

bool operator!=(const SessionStartData& lhs, const SessionStartData& rhs) {
  return !(lhs == rhs);
}

bool operator==(const MultiSessionStartData& lhs,
                const MultiSessionStartData& rhs) {
  return lhs.file_count == rhs.file_count && lhs.byte_count == rhs.byte_count &&
         lhs.chunk_count == rhs.chunk_count &&
         lhs.min_chunk_size == rhs.min_chunk_size &&
         lhs.avg_chunk_size == rhs.avg_chunk_size &&
         lhs.max_chunk_size == rhs.max_chunk_size;
}

bool operator!=(const MultiSessionStartData& lhs,
                const MultiSessionStartData& rhs) {
  return !(lhs == rhs);
}

bool operator==(const SessionData& lhs, const SessionData& rhs) {
  return lhs.chunk_count == rhs.chunk_count && lhs.byte_count == rhs.byte_count;
}

bool operator!=(const SessionData& lhs, const SessionData& rhs) {
  return !(lhs == rhs);
}

bool operator==(const AssetStreamingManagerData& lhs,
                const AssetStreamingManagerData& rhs) {
  return Eq(lhs.session_data, rhs.session_data) &&
         Eq(lhs.manifest_update_data, rhs.manifest_update_data) &&
         Eq(lhs.session_start_data, rhs.session_start_data) &&
         Eq(lhs.multi_session_start_data, rhs.multi_session_start_data) &&
         lhs.multisession_id == rhs.multisession_id &&
         lhs.session_id == rhs.session_id;
}

bool operator!=(const AssetStreamingManagerData& lhs,
                const AssetStreamingManagerData& rhs) {
  return !(lhs == rhs);
}

bool operator==(const DeveloperLogEvent& lhs, const DeveloperLogEvent& rhs) {
  return lhs.project_id == rhs.project_id &&
         lhs.organization_id == rhs.organization_id &&
         Eq(lhs.as_manager_data, rhs.as_manager_data);
}

bool operator!=(const DeveloperLogEvent& lhs, const DeveloperLogEvent& rhs) {
  return !(lhs == rhs);
}

}  // namespace metrics
}  // namespace cdc_ft
