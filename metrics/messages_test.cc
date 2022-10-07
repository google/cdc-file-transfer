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

#include "absl/status/status.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace metrics {
namespace {

class MessagesTest : public ::testing::Test {
 protected:
  ManifestUpdateData GetManifestUpdateData() {
    ManifestUpdateData data;
    data.total_assets_added_or_updated = 1;
    data.total_files_added_or_updated = 2;
    data.total_files_failed = 3;
    data.total_assets_deleted = 4;
    data.total_chunks = 5;
    data.total_processed_bytes = 6;
    data.local_duration_ms = 7;
    data.trigger = UpdateTrigger::kRegularUpdate;
    data.status = absl::StatusCode::kResourceExhausted;
    return data;
  }

  SessionStartData GetSessionStartData() {
    SessionStartData data;
    data.absl_status = absl::StatusCode::kInvalidArgument;
    data.status = SessionStartStatus::kRestartSessionError;
    data.concurrent_session_count = 1;
    data.origin = RequestOrigin::kPartnerPortal;
    return data;
  }

  MultiSessionStartData GetMultiSessionStartData() {
    MultiSessionStartData data;
    data.byte_count = 1;
    data.chunk_count = 2;
    data.file_count = 3;
    data.min_chunk_size = 4;
    data.avg_chunk_size = 5;
    data.max_chunk_size = 6;
    return data;
  }

  SessionData GetSessionData() {
    SessionData data;
    data.byte_count = 1;
    data.chunk_count = 2;
    return data;
  }

  AssetStreamingManagerData GetAssetStreamingManagerData() {
    AssetStreamingManagerData data;
    data.session_id = "id1";
    data.multisession_id = "id2";
    data.manifest_update_data =
        std::make_unique<ManifestUpdateData>(GetManifestUpdateData());
    data.multi_session_start_data =
        std::make_unique<MultiSessionStartData>(GetMultiSessionStartData());
    data.session_data = std::make_unique<SessionData>(GetSessionData());
    data.session_start_data =
        std::make_unique<SessionStartData>(GetSessionStartData());
    return data;
  }

  DeveloperLogEvent GetDeveloperLogEvent() {
    DeveloperLogEvent data;
    data.project_id = "id1";
    data.organization_id = "id2";
    data.as_manager_data = std::make_unique<AssetStreamingManagerData>(
        GetAssetStreamingManagerData());
    return data;
  }
};

TEST_F(MessagesTest, ManifestUpdateDataEqual) {
  EXPECT_EQ(GetManifestUpdateData(), GetManifestUpdateData());
  ManifestUpdateData data = GetManifestUpdateData();
  data.total_assets_added_or_updated = 11;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.total_files_added_or_updated = 12;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.total_files_failed = 13;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.total_assets_deleted = 14;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.total_chunks = 15;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.total_processed_bytes = 16;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.local_duration_ms = 17;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.trigger = UpdateTrigger::kRunningUpdateAll;
  EXPECT_NE(data, GetManifestUpdateData());
  data = GetManifestUpdateData();
  data.status = absl::StatusCode::kFailedPrecondition;
  EXPECT_NE(data, GetManifestUpdateData());
}

TEST_F(MessagesTest, SessionStartDataEqual) {
  EXPECT_EQ(GetSessionStartData(), GetSessionStartData());
  SessionStartData data = GetSessionStartData();
  data.absl_status = absl::StatusCode::kUnauthenticated;
  EXPECT_NE(data, GetSessionStartData());
  data = GetSessionStartData();
  data.status = SessionStartStatus::kStopSessionError;
  EXPECT_NE(data, GetSessionStartData());
  data = GetSessionStartData();
  data.concurrent_session_count = 11;
  EXPECT_NE(data, GetSessionStartData());
  data = GetSessionStartData();
  data.origin = RequestOrigin::kUnknown;
  EXPECT_NE(data, GetSessionStartData());
}

TEST_F(MessagesTest, MultiSessionStartDataEqual) {
  EXPECT_EQ(GetMultiSessionStartData(), GetMultiSessionStartData());
  MultiSessionStartData data = GetMultiSessionStartData();
  data.byte_count = 11;
  EXPECT_NE(data, GetMultiSessionStartData());
  data = GetMultiSessionStartData();
  data.chunk_count = 12;
  EXPECT_NE(data, GetMultiSessionStartData());
  data = GetMultiSessionStartData();
  data.file_count = 13;
  EXPECT_NE(data, GetMultiSessionStartData());
  data = GetMultiSessionStartData();
  data.min_chunk_size = 14;
  EXPECT_NE(data, GetMultiSessionStartData());
  data = GetMultiSessionStartData();
  data.avg_chunk_size = 15;
  EXPECT_NE(data, GetMultiSessionStartData());
  data = GetMultiSessionStartData();
  data.max_chunk_size = 16;
  EXPECT_NE(data, GetMultiSessionStartData());
}

TEST_F(MessagesTest, SessionDataEqual) {
  EXPECT_EQ(GetSessionData(), GetSessionData());
  SessionData data = GetSessionData();
  data.byte_count = 11;
  EXPECT_NE(data, GetSessionData());
  data = GetSessionData();
  data.chunk_count = 12;
  EXPECT_NE(data, GetSessionData());
}

TEST_F(MessagesTest, AssetStreamingManagerDataEqual) {
  EXPECT_EQ(GetAssetStreamingManagerData(), GetAssetStreamingManagerData());
  AssetStreamingManagerData data = GetAssetStreamingManagerData();
  data.session_id = "id-11";
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.multisession_id = "id-12";
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.manifest_update_data = NULL;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.manifest_update_data->total_assets_added_or_updated = 11;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.multi_session_start_data = NULL;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.multi_session_start_data->byte_count = 11;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.session_data = NULL;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.session_data->byte_count = 11;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.session_start_data = NULL;
  EXPECT_NE(data, GetAssetStreamingManagerData());
  data = GetAssetStreamingManagerData();
  data.session_start_data->absl_status = absl::StatusCode::kOutOfRange;
  EXPECT_NE(data, GetAssetStreamingManagerData());
}

TEST_F(MessagesTest, DeveloperLogEventEqual) {
  EXPECT_EQ(GetDeveloperLogEvent(), GetDeveloperLogEvent());
  DeveloperLogEvent data = GetDeveloperLogEvent();
  data.project_id = "id-11";
  EXPECT_NE(data, GetDeveloperLogEvent());
  data = GetDeveloperLogEvent();
  data.organization_id = "id-12";
  EXPECT_NE(data, GetDeveloperLogEvent());
  data = GetDeveloperLogEvent();
  data.as_manager_data = NULL;
  EXPECT_NE(data, GetDeveloperLogEvent());
  data = GetDeveloperLogEvent();
  data.as_manager_data->session_data = NULL;
  EXPECT_NE(data, GetDeveloperLogEvent());
}

}  // namespace
}  // namespace metrics
}  // namespace cdc_ft
