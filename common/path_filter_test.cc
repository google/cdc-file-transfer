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

#include "common/path_filter.h"

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class PathFilterTest : public ::testing::Test {};

TEST_F(PathFilterTest, IsMatchFn_NoWildcard) {
  EXPECT_TRUE(internal::IsMatch("", ""));
  EXPECT_TRUE(internal::IsMatch("a", "a"));
  EXPECT_FALSE(internal::IsMatch("a", "b"));
  EXPECT_TRUE(internal::IsMatch("abc", "abc"));
  EXPECT_FALSE(internal::IsMatch("abc", "abd"));
}

TEST_F(PathFilterTest, IsMatchFn_Questionmark) {
  EXPECT_FALSE(internal::IsMatch("?", ""));
  EXPECT_TRUE(internal::IsMatch("?", "a"));
  EXPECT_FALSE(internal::IsMatch("?", "ab"));
  EXPECT_TRUE(internal::IsMatch("??", "ab"));
  EXPECT_FALSE(internal::IsMatch("??", "a"));
}

TEST_F(PathFilterTest, IsMatchFn_Asterisk) {
  EXPECT_TRUE(internal::IsMatch("*", ""));
  EXPECT_TRUE(internal::IsMatch("*", "a"));
  EXPECT_TRUE(internal::IsMatch("*", "ab"));
  EXPECT_TRUE(internal::IsMatch("*", "abc"));

  EXPECT_TRUE(internal::IsMatch("a**", "abc"));
  EXPECT_FALSE(internal::IsMatch("b**", "abc"));
  EXPECT_FALSE(internal::IsMatch("c**", "abc"));

  EXPECT_TRUE(internal::IsMatch("*a*", "abc"));
  EXPECT_TRUE(internal::IsMatch("*b*", "abc"));
  EXPECT_TRUE(internal::IsMatch("*c*", "abc"));

  EXPECT_FALSE(internal::IsMatch("**a", "abc"));
  EXPECT_FALSE(internal::IsMatch("**b", "abc"));
  EXPECT_TRUE(internal::IsMatch("**c", "abc"));
}

TEST_F(PathFilterTest, IsMatchFn_CommonCases) {
  constexpr char p1[] = "*.txt";
  EXPECT_TRUE(internal::IsMatch(p1, "file1.txt"));
  EXPECT_FALSE(internal::IsMatch(p1, "file2.dat"));

  constexpr char p2[] = "dir1\\dir2\\*.txt";
  EXPECT_TRUE(internal::IsMatch(p2, "dir1\\dir2\\file1.txt"));
  EXPECT_FALSE(internal::IsMatch(p2, "dir1\\dir2\\file2.dat"));
  EXPECT_FALSE(internal::IsMatch(p2, "dir1\\file3.txt"));

  constexpr char p3[] = "dir1\\dir?\\*.txt";
  EXPECT_TRUE(internal::IsMatch(p3, "dir1\\dir2\\file1.txt"));
  EXPECT_TRUE(internal::IsMatch(p3, "dir1\\dir3\\file2.txt"));
  EXPECT_FALSE(internal::IsMatch(p3, "dir1\\file3.txt"));

  constexpr char p4[] = "dir1\\*\\*.txt";
  EXPECT_FALSE(internal::IsMatch(p4, "dir1\\file1.txt"));
  EXPECT_TRUE(internal::IsMatch(p4, "dir1\\dir2\\file1.txt"));
  EXPECT_TRUE(internal::IsMatch(p4, "dir1\\dir3\\file2.txt"));
  // Note: This is different from glob behavior!
  EXPECT_TRUE(internal::IsMatch(p4, "dir1\\dir4\\dir5\\file3.txt"));
  EXPECT_TRUE(internal::IsMatch(p4, "dir1\\\\file2.txt"));

  constexpr char p5[] = "dir1\\*\\dir3\\*\\*.txt";
  EXPECT_FALSE(internal::IsMatch(p5, "dir1\\dir2\\file1.txt"));
  EXPECT_FALSE(internal::IsMatch(p5, "dir1\\dir3\\file2.txt"));
  EXPECT_TRUE(internal::IsMatch(p5, "dir1\\dir2\\dir3\\dir4\\file3.txt"));
}

TEST_F(PathFilterTest, IsMatch_NoFilter) {
  PathFilter path_filter;
  EXPECT_TRUE(path_filter.IsMatch(""));
  EXPECT_TRUE(path_filter.IsMatch("a"));
  EXPECT_TRUE(path_filter.IsMatch("b\\c"));
}

TEST_F(PathFilterTest, IsMatch_IncludeFilter) {
  PathFilter path_filter;
  path_filter.AddRule(PathFilter::Rule::Type::kInclude, "a");
  EXPECT_TRUE(path_filter.IsMatch(""));
  EXPECT_TRUE(path_filter.IsMatch("a"));
  EXPECT_TRUE(path_filter.IsMatch("b\\c"));
}

TEST_F(PathFilterTest, IsMatch_ExcludeFilter) {
  PathFilter path_filter;
  path_filter.AddRule(PathFilter::Rule::Type::kExclude, "a");
  EXPECT_TRUE(path_filter.IsMatch(""));
  EXPECT_FALSE(path_filter.IsMatch("a"));
  EXPECT_TRUE(path_filter.IsMatch("b\\c"));
}

TEST_F(PathFilterTest, IsMatch_IncludeThenExclude) {
  PathFilter path_filter;
  path_filter.AddRule(PathFilter::Rule::Type::kInclude, "file2.txt");
  path_filter.AddRule(PathFilter::Rule::Type::kExclude, "*.txt");
  EXPECT_FALSE(path_filter.IsMatch("file1.txt"));
  EXPECT_TRUE(path_filter.IsMatch("file2.txt"));
  EXPECT_TRUE(path_filter.IsMatch("file3.dat"));
}

TEST_F(PathFilterTest, IsMatch_ExcludeThenInclude) {
  PathFilter path_filter;
  path_filter.AddRule(PathFilter::Rule::Type::kExclude, "*.txt");
  path_filter.AddRule(PathFilter::Rule::Type::kInclude, "file2.txt");
  EXPECT_FALSE(path_filter.IsMatch("file1.txt"));
  EXPECT_FALSE(path_filter.IsMatch("file2.txt"));
  EXPECT_TRUE(path_filter.IsMatch("file3.dat"));
}

}  // namespace
}  // namespace cdc_ft
