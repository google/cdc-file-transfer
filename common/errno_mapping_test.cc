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

#include "common/errno_mapping.h"

#include <errno.h>
#include <stddef.h>

#include <string>

#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Errno value that is hopefully not defined on any OS.
const int kUndefinedErrno = 999123;

TEST(ErrnoMappingTest, ErrnoToCanonicalCode) {
  EXPECT_EQ(ErrnoToCanonicalCode(0), absl::StatusCode::kOk);

  // Spot-check a few errno values.
  EXPECT_EQ(ErrnoToCanonicalCode(EINVAL), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(ErrnoToCanonicalCode(ENOENT), absl::StatusCode::kNotFound);
  EXPECT_EQ(ErrnoToCanonicalCode(kUndefinedErrno), absl::StatusCode::kUnknown);
}

TEST(ErrnoMappingTest, ErrnoToCanonicalStatus) {
  EXPECT_OK(ErrnoToCanonicalStatus(0, ""));

  // Spot-check a few errno values.
  EXPECT_ERROR_MSG(InvalidArgument, "test0",
                   ErrnoToCanonicalStatus(EINVAL, "test0"));
  EXPECT_ERROR_MSG(NotFound, "test1", ErrnoToCanonicalStatus(ENOENT, "test1"));

  // Apparently errno 999 is known not to be defined.
  EXPECT_ERROR_MSG(Unknown, "test2",
                   ErrnoToCanonicalStatus(kUndefinedErrno, "test2"));
}

}  // namespace
}  // namespace cdc_ft
