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

#include "common/stopwatch.h"

#include "common/testing_clock.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class StopwatchTest : public ::testing::Test {};

TEST_F(StopwatchTest, DefaultStopwatch) {
  // Cover DefaultClock.
  Stopwatch stopwatch;
  EXPECT_GE(stopwatch.ElapsedSeconds(), 0.0);
}

TEST_F(StopwatchTest, Stopwatch) {
  TestingSteadyClock clock;
  Stopwatch stopwatch(&clock);

  EXPECT_EQ(stopwatch.ElapsedSeconds(), 0.0);
  clock.Advance(1000);
  EXPECT_EQ(stopwatch.ElapsedSeconds(), 1.0);
  clock.Advance(500);
  EXPECT_EQ(stopwatch.ElapsedSeconds(), 1.5);
  stopwatch.Reset();
  EXPECT_EQ(stopwatch.ElapsedSeconds(), 0.0);
  clock.Advance(250);
  EXPECT_EQ(stopwatch.ElapsedSeconds(), 0.25);
}

}  // namespace
}  // namespace cdc_ft
