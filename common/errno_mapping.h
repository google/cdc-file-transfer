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

#ifndef COMMON_ERRNO_MAPPING_H_
#define COMMON_ERRNO_MAPPING_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"

namespace cdc_ft {

// Converts the errno |error_number| to an absl code.
absl::StatusCode ErrnoToCanonicalCode(int error_number);

// Creates a status by converting the errno |error_number| to an absl code.
template <typename... Args>
absl::Status ErrnoToCanonicalStatus(int error_number,
                                    const absl::FormatSpec<Args...>& format,
                                    Args... args) {
  if (error_number == 0) return absl::OkStatus();
  std::string msg = absl::StrFormat(format, args...);
  return absl::Status(ErrnoToCanonicalCode(error_number),
                      absl::StrCat(msg, ": ", strerror(error_number)));
}

template <typename... Args>
absl::Status ErrorCodeToCanonicalStatus(const std::error_code& code,
                                        const absl::FormatSpec<Args...>& format,
                                        Args... args) {
  if (!code) return absl::OkStatus();
  std::string msg = absl::StrFormat(format, args...);
  absl::StatusCode absl_code = code.category() == std::system_category()
                                   ? ErrnoToCanonicalCode(code.value())
                                   : absl::StatusCode::kUnknown;
  return absl::Status(absl_code, absl::StrCat(msg, ": ", code.message()));
}

}  // namespace cdc_ft

#endif  // COMMON_ERRNO_MAPPING_H_
