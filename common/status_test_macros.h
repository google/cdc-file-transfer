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

#ifndef COMMON_STATUS_TEST_MACROS_H_
#define COMMON_STATUS_TEST_MACROS_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/platform.h"

#ifndef PLATFORM_WINDOWS
#define __forceinline inline __attribute__((always_inline))
#endif

namespace internal {
namespace status_test_macros {

// Helper functions to get status from both status and StatusOr.
template <class T>
const absl::Status& ToStatus(const absl::StatusOr<T>& result) {
  return result.status();
}
__forceinline const absl::Status& ToStatus(const absl::Status& status) {
  return status;
}

}  // namespace status_test_macros
}  // namespace internal

// Little convenience macros to check the return value of an absl::Status in
// gTest unit tests.
#define EXPECT_OK(x) VERIFY_OK(EXPECT, x)
#define ASSERT_OK(x) VERIFY_OK(ASSERT, x)
#define VERIFY_OK(action, x)                                          \
  do {                                                                \
    auto __status = ::internal::status_test_macros::ToStatus(x);      \
    action##_TRUE(__status.ok()) << "Error: " << __status.ToString(); \
  } while (0)

#define EXPECT_NOT_OK(x) VERIFY_NOT_OK(EXPECT, x)
#define ASSERT_NOT_OK(x) VERIFY_NOT_OK(ASSERT, x)
#define VERIFY_NOT_OK(action, x) action##_FALSE((x).ok())

// type should be one of the absl::IsXXX errors, e.g. "Canceled".
#define EXPECT_ERROR(type, x) VERIFY_ERROR(EXPECT, type, x)
#define ASSERT_ERROR(type, x) VERIFY_ERROR(ASSERT, type, x)
#define VERIFY_ERROR(action, type, x)                                     \
  do {                                                                    \
    auto __status = ::internal::status_test_macros::ToStatus(x);          \
    action##_TRUE(absl::Is##type(__status))                               \
        << "Unexpected status '" << __status.ToString() << "', expected " \
        << #type;                                                         \
  } while (0)

// type should be one of the absl::IsXXX errors, e.g. "Canceled".
#define EXPECT_ERROR_MSG(type, msg, x) VERIFY_ERROR_MSG(EXPECT, type, msg, x)
#define ASSERT_ERROR_MSG(type, msg, x) VERIFY_ERROR_MSG(ASSERT, type, msg, x)
#define VERIFY_ERROR_MSG(action, type, msg, x)                            \
  do {                                                                    \
    auto __status = ::internal::status_test_macros::ToStatus(x);          \
    action##_TRUE(absl::Is##type(__status))                               \
        << "Unexpected status '" << __status.ToString() << "', expected " \
        << #type;                                                         \
    action##_NE(__status.message().find(msg), std::string::npos)          \
        << "Unexpected message '" << __status.message() << "', expected " \
        << "'" << msg << "'";                                             \
  } while (0)

#endif  // COMMON_STATUS_TEST_MACROS_H_
