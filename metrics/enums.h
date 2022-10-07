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

#ifndef METRICS_ENUMS_H_
#define METRICS_ENUMS_H_

namespace cdc_ft {
namespace metrics {

// Event types logged by file transfer component.
enum class EventType {
  kSessionHeartBeat,
  kSessionStart,
  kMultiSessionStart,
  kSessionEnd,
  kMultiSessionEnd,
  kManifestUpdated
};

// Manifest update trigger.
enum class UpdateTrigger {
  // Initial ManifestUpdater::UpdateAll() call
  // at the start of a MultiSession.
  kInitUpdateAll,
  // ManifestUpdater::UpdateAll() call during a running MultiSession
  // if the streamed folder is recreated.
  kRunningUpdateAll,
  // ManifestUpdater::Update() call to incrementally update modified files.
  kRegularUpdate
};

// Session start status, which describes at which point
// of execution it failed.
enum class SessionStartStatus {
  kOk,
  kParseInstanceNameError,
  kInvalidDirError,
  kRestartSessionError,
  kStopSessionError,
  kCreateMultiSessionError,
  kStartSessionError
};

// Where a request originated from.
enum class RequestOrigin { kUnknown, kCli, kPartnerPortal };

}  // namespace metrics
}  // namespace cdc_ft

#endif  // METRICS_ENUMS_H_
