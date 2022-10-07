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

#include "common/buffer.h"

#include <cstring>

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

TEST(BufferTest, ConstructDefault) {
  Buffer b;
  EXPECT_EQ(b.size(), 0);
  EXPECT_EQ(b.data(), nullptr);
}

TEST(BufferTest, ConstructWithSize) {
  Buffer b(32);
  EXPECT_EQ(b.size(), 32);
  EXPECT_EQ(b.capacity(), 32);
  EXPECT_NE(b.data(), nullptr);
  // This shouldn't crash.
  memset(b.data(), 0, b.size());
}

TEST(BufferTest, ConstructWithInitializerList) {
  Buffer b({'1', '2', '3', '4', '5', '6', '7', '8', '9'});
  EXPECT_EQ(b.size(), 9);
  EXPECT_EQ(b.capacity(), 9);
  EXPECT_NE(b.data(), nullptr);
  EXPECT_EQ(memcmp(b.data(), "123456789", 9), 0);
}

TEST(BufferTest, MoveConstructor) {
  Buffer b({9});
  Buffer b2(std::move(b));

  EXPECT_EQ(b.size(), 0);
  EXPECT_EQ(b.data(), nullptr);
  EXPECT_EQ(b.capacity(), 0);

  EXPECT_EQ(b2.size(), 1);
  EXPECT_NE(b2.data(), nullptr);
  EXPECT_EQ(*b2.data(), 9);
}

TEST(BufferTest, MoveOperator) {
  Buffer b({9});
  Buffer b2({12, 13});

  b2 = std::move(b);

  EXPECT_EQ(b.size(), 0);
  EXPECT_EQ(b.data(), nullptr);
  EXPECT_EQ(b.capacity(), 0);

  EXPECT_EQ(b2.size(), 1);
  EXPECT_NE(b2.data(), nullptr);
  EXPECT_EQ(*b2.data(), 9);
}

TEST(BufferTest, EqualsOperator) {
  Buffer b({1});
  Buffer b2({1});
  Buffer b3({2});

  EXPECT_TRUE(b == b2);
  EXPECT_FALSE(b == b3);

  EXPECT_FALSE(b != b2);
  EXPECT_TRUE(b != b3);
}

TEST(BufferTest, SizeIncrease) {
  Buffer b(8);
  EXPECT_EQ(b.size(), 8);
  EXPECT_EQ(b.capacity(), 8);
  EXPECT_NE(b.data(), nullptr);
  memcpy(b.data(), "01234567", 8);

  // Should realloc the buffer, but keep the data.
  b.resize(10);
  EXPECT_EQ(b.size(), 10);
  EXPECT_EQ(b.capacity(), 15);
  EXPECT_NE(b.data(), nullptr);
  EXPECT_EQ(memcmp(b.data(), "01234567", 8), 0);
  memcpy(b.data(), "0123456789", 10);

  // Should not realloc the buffer and keep data.
  b.resize(12);
  EXPECT_EQ(b.size(), 12);
  EXPECT_EQ(b.capacity(), 15);
  EXPECT_NE(b.data(), nullptr);
  EXPECT_EQ(memcmp(b.data(), "0123456789", 10), 0);
}

TEST(BufferTest, SizeDecrease) {
  Buffer b(8);
  EXPECT_EQ(b.size(), 8);
  EXPECT_EQ(b.capacity(), 8);
  EXPECT_NE(b.data(), nullptr);
  memcpy(b.data(), "01234567", 8);

  // Should not realloc the buffer and keep data.
  b.resize(2);
  EXPECT_EQ(b.size(), 2);
  EXPECT_EQ(b.capacity(), 8);
  EXPECT_NE(b.data(), nullptr);
  EXPECT_EQ(memcmp(b.data(), "01", 2), 0);
}

TEST(BufferTest, Reserve) {
  Buffer b({1});
  Buffer b2({1});
  Buffer b3({2});

  EXPECT_TRUE(b == b2);
  EXPECT_FALSE(b == b3);

  EXPECT_FALSE(b != b2);
  EXPECT_TRUE(b != b3);
}

TEST(BufferTest, Empty) {
  Buffer b;
  EXPECT_TRUE(b.empty());
  b = {};
  EXPECT_TRUE(b.empty());
  b = {1};
  EXPECT_FALSE(b.empty());
}

TEST(BufferTest, Append) {
  Buffer b;
  b.append("9", 1);
  EXPECT_EQ(b, Buffer({'9'}));
  b.append("123", 3);
  EXPECT_EQ(b, Buffer({'9', '1', '2', '3'}));
}

TEST(BufferTest, Clear) {
  Buffer b(8);
  EXPECT_EQ(b.size(), 8);
  EXPECT_EQ(b.capacity(), 8);
  EXPECT_NE(b.data(), nullptr);

  b.clear();
  EXPECT_EQ(b.size(), 0);
  EXPECT_EQ(b.capacity(), 8);
  EXPECT_NE(b.data(), nullptr);
}

}  // namespace
}  // namespace cdc_ft
