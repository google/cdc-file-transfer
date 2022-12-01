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

#include "common/log.h"

namespace cdc_ft {

MetricsRecorder::MetricsRecorder(metrics::MetricsService* const metrics_service)
    : metrics_service_(metrics_service) {}

metrics::MetricsService* MetricsRecorder::GetMetricsService() const {
  return metrics_service_;
}

MultiSessionMetricsRecorder::MultiSessionMetricsRecorder(
    metrics::MetricsService* const metrics_service)
    : MetricsRecorder(metrics_service),
      multisession_id_(Util::GenerateUniqueId()) {}

MultiSessionMetricsRecorder::~MultiSessionMetricsRecorder() = default;

void MultiSessionMetricsRecorder::RecordEvent(metrics::DeveloperLogEvent event,
                                              metrics::EventType code) const {
  if (!event.as_manager_data) {
    event.as_manager_data =
        std::make_unique<metrics::AssetStreamingManagerData>();
  }
  event.as_manager_data->multisession_id = multisession_id_;
  metrics_service_->RecordEvent(std::move(event), code);
}

SessionMetricsRecorder::SessionMetricsRecorder(
    metrics::MetricsService* const metrics_service,
    const std::string& multisession_id, const std::string& project_id,
    const std::string& organization_id)
    : MetricsRecorder(metrics_service),
      multisession_id_(multisession_id),
      project_id_(project_id),
      organization_id_(organization_id),
      session_id_(Util::GenerateUniqueId()) {}

SessionMetricsRecorder::~SessionMetricsRecorder() = default;

void SessionMetricsRecorder::RecordEvent(metrics::DeveloperLogEvent event,
                                         metrics::EventType code) const {
  if (!event.as_manager_data) {
    event.as_manager_data =
        std::make_unique<metrics::AssetStreamingManagerData>();
  }
  event.as_manager_data->multisession_id = multisession_id_;
  event.as_manager_data->session_id = session_id_;
  event.project_id = project_id_;
  event.organization_id = organization_id_;
  metrics_service_->RecordEvent(std::move(event), code);
}
}  // namespace cdc_ft
