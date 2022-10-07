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

#include "common/url.h"

#include "absl/status/statusor.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class UrlTest : public ::testing::Test {
 protected:
  void AssertEqual(absl::StatusOr<Url> actual, Url expected) {
    EXPECT_OK(actual);
    EXPECT_EQ(actual.value(), expected);
  }
};

TEST_F(UrlTest, ParseUrl) {
  AssertEqual(Url::Parse("https://hOSt.t.gF.d"),
              Url("https", "host.t.gf.d", "443"));
  AssertEqual(Url::Parse("http://h"), Url("http", "h", "80"));
  AssertEqual(Url::Parse("https://h:8080/path"), Url("https", "h", "8080"));
  AssertEqual(Url::Parse("https://h?t=45"), Url("https", "h", "443"));
  AssertEqual(Url::Parse("https://h:765?t=45"), Url("https", "h", "765"));
  AssertEqual(
      Url::Parse(
          "https://my-domain.ho.HO.ho:80/path/to/smth?arg1=123&dfd=[g,6,y]"),
      Url("https", "my-domain.ho.ho.ho", "80"));
  EXPECT_NOT_OK(Url::Parse("httpss://domain"));
  EXPECT_NOT_OK(Url::Parse("HOST.com:443/path"));
  EXPECT_NOT_OK(Url::Parse("https:/domain"));
  EXPECT_NOT_OK(Url::Parse("http//domain"));
  EXPECT_NOT_OK(Url::Parse("htps://dmn.ua"));
  EXPECT_NOT_OK(Url::Parse("non-url"));
}

}  // namespace
}  // namespace cdc_ft
