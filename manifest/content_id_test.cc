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

#include "manifest/content_id.h"

#include "gtest/gtest.h"

namespace cdc_ft {

namespace {

using StringList = std::vector<absl::string_view>;

static constexpr char kData[] = "Hey Google, tell me a joke.";
static constexpr size_t kHashSize = 20;
static constexpr char kHash[kHashSize + 1] =
    "\x12\xe8\x41\x41\x39\x93\x13\x82\x34\xd0\xfe\xcb\x4e\xcf\x6a\x4c\xfd\x74"
    "\x55\x27";
static constexpr char kHashHex[] = "12e841413993138234d0fecb4ecf6a4cfd745527";

TEST(ContentIdTest, StringToContentId) {
  ContentIdProto content_id = ContentId::FromDataString(std::string(kData));
  EXPECT_EQ(content_id.blake3_sum_160().size(), kHashSize);
  EXPECT_EQ(content_id.blake3_sum_160(), absl::string_view(kHash, kHashSize));
}

TEST(ContentIdTest, StringViewToContentId) {
  ContentIdProto content_id =
      ContentId::FromDataString(absl::string_view(kData));
  EXPECT_EQ(content_id.blake3_sum_160().size(), kHashSize);
  EXPECT_EQ(content_id.blake3_sum_160(), absl::string_view(kHash, kHashSize));
}

TEST(ContentIdTest, PtrToContentId) {
  absl::string_view data(kData);
  ContentIdProto content_id = ContentId::FromArray(data.data(), data.size());
  EXPECT_EQ(content_id.blake3_sum_160().size(), kHashSize);
  EXPECT_EQ(content_id.blake3_sum_160(), absl::string_view(kHash, kHashSize));
}

TEST(ContentIdTest, ToHexString) {
  ContentIdProto content_id =
      ContentId::FromDataString(absl::string_view(kData));
  std::string hash_str = ContentId::ToHexString(content_id);
  EXPECT_EQ(hash_str.size(), 2 * kHashSize);
  EXPECT_EQ(hash_str, kHashHex);
}

TEST(ContentIdTest, FromHexString) {
  ContentIdProto content_id;
  EXPECT_TRUE(ContentId::FromHexString(kHashHex, &content_id));
  EXPECT_EQ(content_id.blake3_sum_160(), kHash);
}

TEST(ContentIdTest, GetByte) {
  ContentIdProto content_id;
  EXPECT_EQ(ContentId::GetByte(content_id, 0), 0);
  EXPECT_EQ(ContentId::GetByte(content_id, 1000), 0);

  EXPECT_TRUE(ContentId::FromHexString(kHashHex, &content_id));
  EXPECT_EQ(ContentId::GetByte(content_id, 0), static_cast<uint8_t>(kHash[0]));
  EXPECT_EQ(ContentId::GetByte(content_id, 1), static_cast<uint8_t>(kHash[1]));
  EXPECT_EQ(ContentId::GetByte(content_id, 20), 0);
}

}  // namespace

}  // namespace cdc_ft
