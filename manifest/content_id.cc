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

#include "blake3.h"

namespace cdc_ft {
namespace {

// Converts |n| in the range 0..15 to its lower-case hex representation.
// Returns -1 if |n| is not in the range 0..15.
char IntToHex(uint8_t n) {
  if (n <= 9) return '0' + n;
  if (n <= 15) return 'a' + n - 10;
  return -1;
}

// Converts the lower-case hex character |c| to its integer representation.
// Returns -1 if |c| is not a valid lower-case hex character.
int HexToInt(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return -1;
}
}  // namespace

// static
ContentIdProto ContentId::FromDataString(const std::string& data) {
  return FromArray(data.c_str(), data.size());
}

// static
ContentIdProto ContentId::FromDataString(absl::string_view data) {
  return FromArray(data.data(), data.size());
}

// static
ContentIdProto ContentId::FromArray(const void* data, size_t len) {
  blake3_hasher state;
  uint8_t out[kHashSize];
  blake3_hasher_init(&state);
  blake3_hasher_update(&state, data, len);
  blake3_hasher_finalize(&state, out, kHashSize);
  ContentIdProto content_id;
  content_id.set_blake3_sum_160(out, kHashSize);
  return content_id;
}

// static
std::string ContentId::ToHexString(const ContentIdProto& content_id) {
  absl::string_view blake3_sum(content_id.blake3_sum_160());
  std::string ret;
  ret.reserve(blake3_sum.size() << 1);
  for (size_t i = 0; i < blake3_sum.size(); ++i) {
    ret.push_back(IntToHex(static_cast<uint8_t>(blake3_sum[i]) >> 4));
    ret.push_back(IntToHex(static_cast<uint8_t>(blake3_sum[i]) & 0xf));
  }
  return ret;
}

// static
bool ContentId::FromHexString(const std::string& str,
                              ContentIdProto* content_id) {
  if (str.size() != kHashSize * 2) return false;

  std::string* hash = content_id->mutable_blake3_sum_160();
  hash->clear();
  hash->reserve(kHashSize);
  for (size_t n = 0; n < str.size(); n += 2) {
    int high = HexToInt(str[n]);
    int low = HexToInt(str[n + 1]);
    if (high == -1 || low == -1) {
      hash->clear();
      return false;
    }
    hash->push_back((high << 4) + low);
  }

  return true;
}

// static
uint8_t ContentId::GetByte(const ContentIdProto& content_id, size_t pos) {
  if (pos >= content_id.blake3_sum_160().size()) return 0;
  return content_id.blake3_sum_160()[pos];
}
}  // namespace cdc_ft
