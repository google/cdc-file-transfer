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

#ifndef COMMON_STATUS_MACROS_H_
#define COMMON_STATUS_MACROS_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "common/status.h"

#define RETURN_IF_ERROR(expr, ...)                            \
  do {                                                        \
    const absl::Status __status__ = (expr);                   \
    if (!__status__.ok()) {                                   \
      return ::cdc_ft::WrapStatus(__status__, ##__VA_ARGS__); \
    }                                                         \
  } while (0)

#define ASSIGN_OR_RETURN(lhs, expr, ...)                               \
  do {                                                                 \
    auto __status__ = (expr);                                          \
    if (!__status__.ok()) {                                            \
      return ::cdc_ft::WrapStatus(__status__.status(), ##__VA_ARGS__); \
    }                                                                  \
    lhs = std::move(__status__.value());                               \
  } while (0)

#endif  // COMMON_STATUS_MACROS_H_
