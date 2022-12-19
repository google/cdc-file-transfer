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

#include "common/port_range_parser.h"

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

TEST(PortRangeParserTest, SingleSuccess) {
  uint16_t first, last;
  EXPECT_TRUE(port_range::Parse("65535", &first, &last));
  EXPECT_EQ(first, 65535);
  EXPECT_EQ(last, 65535);
}

TEST(PortRangeParserTest, RangeSuccess) {
  uint16_t first, last;
  EXPECT_TRUE(port_range::Parse("1-2", &first, &last));
  EXPECT_EQ(first, 1);
  EXPECT_EQ(last, 2);
}

TEST(ParamsTest, NoValueFail) {
  uint16_t first = 1, last = 1;
  EXPECT_FALSE(port_range::Parse("", &first, &last));
  EXPECT_EQ(first, 0);
  EXPECT_EQ(last, 0);
}

TEST(ParamsTest, BadValueTooSmallFail) {
  uint16_t first, last;
  EXPECT_FALSE(port_range::Parse("0", &first, &last));
}

TEST(ParamsTest, BadValueNotIntegerFail) {
  uint16_t first, last;
  EXPECT_FALSE(port_range::Parse("port", &first, &last));
}

TEST(ParamsTest, ForwardPort_BadRangeTooBig) {
  uint16_t first, last;
  EXPECT_FALSE(port_range::Parse("50000-65536", &first, &last));
}

TEST(ParamsTest, ForwardPort_BadRangeFirstGtLast) {
  uint16_t first, last;
  EXPECT_FALSE(port_range::Parse("50001-50000", &first, &last));
}

TEST(ParamsTest, ForwardPort_BadRangeTwoMinus) {
  uint16_t first, last;
  EXPECT_FALSE(port_range::Parse("1-2-3", &first, &last));
}

}  // namespace
}  // namespace cdc_ft
