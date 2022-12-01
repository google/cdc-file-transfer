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

#include "cdc_stream/metrics_recorder.h"

#include "common/status_test_macros.h"
#include "gtest/gtest.h"
#include "metrics/metrics.h"

namespace cdc_ft {
namespace {

struct MetricsRecord {
  MetricsRecord(metrics::DeveloperLogEvent dev_log_event,
                metrics::EventType code)
      : dev_log_event(std::move(dev_log_event)), code(code) {}
  metrics::DeveloperLogEvent dev_log_event;
  metrics::EventType code;
};

class MetricsServiceForTesting : public metrics::MetricsService {
 public:
  MetricsServiceForTesting() {
    metrics_records_ = new std::vector<MetricsRecord>();
  }

  ~MetricsServiceForTesting() { delete metrics_records_; }

  void RecordEvent(metrics::DeveloperLogEvent event,
                   metrics::EventType code) const override {
    metrics_records_->push_back(MetricsRecord(std::move(event), code));
  }

  int NumberOfRecordRequests() { return (int)metrics_records_->size(); }

  std::vector<MetricsRecord> GetEventsAndClear() {
    return std::move(*metrics_records_);
  }

 private:
  std::vector<MetricsRecord>* metrics_records_;
};

class MetricsRecorderTest : public ::testing::Test {
 public:
  void SetUp() override { metrics_service_ = new MetricsServiceForTesting(); }

  void TearDown() override { delete metrics_service_; }

 protected:
  MetricsServiceForTesting* metrics_service_;
};

TEST_F(MetricsRecorderTest, SendEventWithMultisessionId) {
  MultiSessionMetricsRecorder target(metrics_service_);
  metrics::DeveloperLogEvent q_evt;
  q_evt.project_id = "proj/id";
  q_evt.organization_id = "org/id";

  target.RecordEvent(std::move(q_evt), metrics::EventType::kMultiSessionStart);
  EXPECT_EQ(metrics_service_->NumberOfRecordRequests(), 1);
  std::vector<MetricsRecord> requests = metrics_service_->GetEventsAndClear();
  EXPECT_EQ(requests[0].code, metrics::EventType::kMultiSessionStart);
  metrics::DeveloperLogEvent expected_evt;
  expected_evt.project_id = "proj/id";
  expected_evt.organization_id = "org/id";
  expected_evt.as_manager_data =
      std::make_unique<metrics::AssetStreamingManagerData>();
  expected_evt.as_manager_data->multisession_id = target.MultiSessionId();
  EXPECT_EQ(requests[0].dev_log_event, expected_evt);
  EXPECT_FALSE(target.MultiSessionId().empty());

  q_evt = metrics::DeveloperLogEvent();
  q_evt.project_id = "proj/id";
  q_evt.organization_id = "org/id";
  target.RecordEvent(std::move(q_evt), metrics::EventType::kMultiSessionStart);
  EXPECT_EQ(metrics_service_->NumberOfRecordRequests(), 1);
  std::vector<MetricsRecord> requests2 = metrics_service_->GetEventsAndClear();
  EXPECT_EQ(requests2[0].code, metrics::EventType::kMultiSessionStart);
  EXPECT_EQ(requests2[0].dev_log_event, requests[0].dev_log_event);

  MultiSessionMetricsRecorder target2(metrics_service_);
  EXPECT_NE(target2.MultiSessionId(), target.MultiSessionId());
}

TEST_F(MetricsRecorderTest, SendEventWithSessionId) {
  SessionMetricsRecorder target(metrics_service_, "id1", "m_proj", "m_org");
  metrics::DeveloperLogEvent q_evt;
  q_evt.project_id = "proj/id";
  q_evt.organization_id = "org/id";

  target.RecordEvent(std::move(q_evt), metrics::EventType::kSessionStart);
  EXPECT_EQ(metrics_service_->NumberOfRecordRequests(), 1);
  std::vector<MetricsRecord> requests = metrics_service_->GetEventsAndClear();
  EXPECT_EQ(requests[0].code, metrics::EventType::kSessionStart);
  metrics::DeveloperLogEvent expected_evt;
  expected_evt.project_id = "m_proj";
  expected_evt.organization_id = "m_org";
  expected_evt.as_manager_data =
      std::make_unique<metrics::AssetStreamingManagerData>();
  expected_evt.as_manager_data->multisession_id = "id1";
  expected_evt.as_manager_data->session_id = target.SessionId();
  EXPECT_EQ(requests[0].dev_log_event, expected_evt);
  EXPECT_FALSE(target.SessionId().empty());

  q_evt = metrics::DeveloperLogEvent();
  q_evt.project_id = "proj/id";
  q_evt.organization_id = "org/id";
  target.RecordEvent(std::move(q_evt), metrics::EventType::kSessionStart);
  EXPECT_EQ(metrics_service_->NumberOfRecordRequests(), 1);
  std::vector<MetricsRecord> requests2 = metrics_service_->GetEventsAndClear();
  EXPECT_EQ(requests2[0].code, metrics::EventType::kSessionStart);
  EXPECT_EQ(requests2[0].dev_log_event, requests[0].dev_log_event);

  SessionMetricsRecorder target2(metrics_service_, "id2", "m_proj", "m_org");
  EXPECT_NE(target2.SessionId(), target.SessionId());
}

}  // namespace
}  // namespace cdc_ft
