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

#include "common/thread_safe_map.h"

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class ThreadSafeMapTest : public ::testing::Test {};

TEST_F(ThreadSafeMapTest, SetGetExising) {
  ThreadSafeMap<std::string, std::string> map_str_str;
  std::string key_str = "key 1";
  std::string val_str = "val 1";
  map_str_str.Set(key_str, val_str);
  EXPECT_EQ(map_str_str.Get(key_str), val_str);

  ThreadSafeMap<int, bool> map_int_bool;
  int key_int = 345;
  bool val_bool = true;
  map_int_bool.Set(key_int, val_bool);
  EXPECT_EQ(map_int_bool.Get(key_int), val_bool);

  ThreadSafeMap<int, double> map_int_double;
  key_int = 124;
  double val_double = 987.23445432;
  map_int_double.Set(key_int, val_double);
  EXPECT_EQ(map_int_double.Get(key_int), val_double);
}

TEST_F(ThreadSafeMapTest, SetGetMissing) {
  ThreadSafeMap<std::string, std::string> map_str_str;
  map_str_str.Set("key 1", "val 1");
  EXPECT_EQ(map_str_str.Get("key 2"), "");

  ThreadSafeMap<int, bool> map_int_bool;
  map_int_bool.Set(1287, true);
  EXPECT_EQ(map_int_bool.Get(-98), false);

  ThreadSafeMap<int, double> map_int_double;
  map_int_double.Set(-82534, 876.121232);
  EXPECT_EQ(map_int_double.Get(12), 0);
}

}  // namespace
}  // namespace cdc_ft
