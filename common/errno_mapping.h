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

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace cdc_ft {

// Converts the errno |error_number| to an absl code.
absl::StatusCode ErrnoToCanonicalCode(int error_number);

// Creates a status by converting the errno |error_number| to an absl code.
absl::Status ErrnoToCanonicalStatus(int error_number,
                                    absl::string_view message);

// Creates a status by converting the std |code to an absl code.
absl::Status ErrorCodeToCanonicalStatus(const std::error_code& code,
                                        absl::string_view message);

}  // namespace cdc_ft

#endif  // COMMON_ERRNO_MAPPING_H_
