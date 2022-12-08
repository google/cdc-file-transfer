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

#ifndef CDC_STREAM_SESSION_CONFIG_H_
#define CDC_STREAM_SESSION_CONFIG_H_

#include <cstdint>

namespace cdc_ft {

// The values set in this config do not necessarily denote the default values.
// For the defaults, see the corresponding flag values.
struct SessionConfig {
  // General log verbosity.
  int verbosity = 0;

  // Silence logs from process execution.
  bool quiet = false;

  // Print detailed streaming stats.
  bool stats = false;

  // Whether to run FUSE in debug mode.
  bool fuse_debug = false;

  // Whether to run FUSE in single-threaded mode.
  bool fuse_singlethreaded = false;

  // Whether to run FUSE consistency check.
  bool fuse_check = false;

  // Cache capacity with a suffix.
  uint64_t fuse_cache_capacity = 0;

  // Cleanup timeout in seconds.
  uint32_t fuse_cleanup_timeout_sec = 0;

  // Access idling timeout in seconds.
  uint32_t fuse_access_idle_timeout_sec = 0;

  // Number of threads used in the manifest updater to compute chunks/hashes.
  uint32_t manifest_updater_threads = 0;

  // Time to wait until running a manifest update after detecting a file change.
  uint32_t file_change_wait_duration_ms = 0;

  // Ports used for local port forwarding.
  uint16_t forward_port_first = 0;
  uint16_t forward_port_last = 0;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_SESSION_CONFIG_H_
