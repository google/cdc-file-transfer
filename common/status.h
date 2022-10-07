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

#ifndef COMMON_STATUS_H_
#define COMMON_STATUS_H_

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "common/platform.h"

namespace cdc_ft {

//
// Convenience helper functions for status creation.
//

inline absl::Status MakeStatus(const char* message) {
  return absl::InternalError(message);
}

template <typename... Args>
absl::Status MakeStatus(const absl::FormatSpec<Args...>& format, Args... args) {
  return absl::InternalError(absl::StrFormat(format, args...));
}

// Convenience helper for cases that may or may not have a message to wrap.
template <typename... Args>
absl::Status WrapStatus(absl::Status inner_status) {
  return inner_status;
}

// Returns OK if |inner_status| is OK and a copy of |inner_status| with given
// message + |inner_status|'s message otherwise.
template <typename... Args>
absl::Status WrapStatus(absl::Status inner_status,
                        const absl::FormatSpec<Args...>& format, Args... args) {
  if (inner_status.ok()) {
    return inner_status;
  }

  std::string message = absl::StrFormat(format, args...);
  absl::Status wrapped_status(
      inner_status.code(), absl::StrCat(inner_status.message(), "; ", message));
  inner_status.ForEachPayload(
      [&wrapped_status](absl::string_view key, const absl::Cord& value) {
        wrapped_status.SetPayload(key, value);
      });
  return wrapped_status;
}

//
// Tags attached to absl::Status to allow more fine-grained error handling.
//

enum class Tag : uint8_t {
  // Sending end of the socket was closed, so receiving end can no longer read.
  kSocketEof = 0,

  // Bind failed, probably because there's another instance of cdc_rsync
  // running.
  kAddressInUse = 1,

  // The gamelet components need to be re-deployed.
  kDeployServer = 2,

  // Something asks for user input, but we're in quiet mode.
  kInstancePickerNotAvailableInQuietMode = 3,

  // Timeout while trying to connect to the gamelet component.
  kConnectionTimeout = 4,

  // MUST BE LAST.
  kCount = 5,
};

// Tags a status. No-op if |status| is OK. Overwrites existing tags.
absl::Status SetTag(absl::Status status, Tag tag);

// Returns the associated tag. Empty if none.
absl::optional<Tag> GetTag(const absl::Status& status);

// Checks whether the status has a certain tag.
bool HasTag(const absl::Status& status, Tag tag);

}  // namespace cdc_ft

#endif  // COMMON_STATUS_H_
