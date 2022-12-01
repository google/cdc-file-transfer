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

#ifndef CDC_STREAM_METRICS_RECORDER_H_
#define CDC_STREAM_METRICS_RECORDER_H_

#include "absl/status/status.h"
#include "common/util.h"
#include "metrics/enums.h"
#include "metrics/messages.h"
#include "metrics/metrics.h"

namespace cdc_ft {

class MetricsRecorder {
 public:
  virtual void RecordEvent(metrics::DeveloperLogEvent event,
                           metrics::EventType code) const = 0;

  virtual metrics::MetricsService* GetMetricsService() const;

 protected:
  explicit MetricsRecorder(metrics::MetricsService* const metrics_service);
  metrics::MetricsService* const metrics_service_;
};

class MultiSessionMetricsRecorder : public MetricsRecorder {
 public:
  explicit MultiSessionMetricsRecorder(
      metrics::MetricsService* const metrics_service);
  ~MultiSessionMetricsRecorder();

  virtual void RecordEvent(metrics::DeveloperLogEvent event,
                           metrics::EventType code) const;

  const std::string& MultiSessionId() const { return multisession_id_; }

 private:
  std::string multisession_id_;
};

class SessionMetricsRecorder : public MetricsRecorder {
 public:
  explicit SessionMetricsRecorder(
      metrics::MetricsService* const metrics_service,
      const std::string& multisession_id, const std::string& project_id,
      const std::string& organization_id);
  ~SessionMetricsRecorder();

  virtual void RecordEvent(metrics::DeveloperLogEvent event,
                           metrics::EventType code) const;

  const std::string& SessionId() const { return session_id_; }

 private:
  std::string multisession_id_;
  std::string session_id_;
  std::string project_id_;
  std::string organization_id_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_METRICS_RECORDER_H_
