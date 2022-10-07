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

#ifndef MANIFEST_CONTENT_ID_H_
#define MANIFEST_CONTENT_ID_H_

#include <string>

#include "absl/strings/string_view.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// This helper class provides some utility functions to work with ContentIdProto
// messages.
class ContentId {
 public:
  // Hashes are 160 bit long.
  static constexpr size_t kHashSize = 20;

  // Returns content ID for the |data| passed in as a string.
  static ContentIdProto FromDataString(const std::string& data);

  // Returns the content ID for the |data| passed in as a string_view.
  static ContentIdProto FromDataString(absl::string_view data);

  // Returns the content ID for the |data| passed in as a pointer.
  static ContentIdProto FromArray(const void* data, size_t len);

  // Converts the given content ID into a hex string. The string will consist of
  // the hex digits of the hash ('0'...'9', 'a'...'f'), so a 160 bit hash
  // results in a string of length kHashSize * 2.
  static std::string ToHexString(const ContentIdProto& content_id);

  // Converts the given hex string into a content ID. The string is assumed to
  // consist of the hex digits of the hash ('0'...'9', 'a'...'f'), so a 160 bit
  // hash would have length kHashSize * 2. Returns false if |str| is malformed.
  static bool FromHexString(const std::string& str, ContentIdProto* content_id);

  // Returns the |pos| byte of |content_id|.
  // Returns 0 if |content_id| is not set or |pos| is invalid.
  static uint8_t GetByte(const ContentIdProto& content_id, size_t pos);
};

namespace proto {

inline bool operator==(const ContentId& a, const ContentId& b) {
  return a.blake3_sum_160() == b.blake3_sum_160();
}

inline bool operator!=(const ContentId& a, const ContentId& b) {
  return !(a == b);
}

inline bool operator<(const ContentId& a, const ContentId& b) {
  return a.blake3_sum_160() < b.blake3_sum_160();
}

}  // namespace proto
}  // namespace cdc_ft

namespace std {

template <>
struct hash<cdc_ft::ContentIdProto> {
  size_t operator()(const cdc_ft::ContentIdProto& id) const {
    // Pick the first 8 bytes of the hash (assuming 64 bit binary).
    if (id.blake3_sum_160().size() < sizeof(size_t)) {
      return 0;
    }
    return *reinterpret_cast<const size_t*>(id.blake3_sum_160().data());
  }
};

}  // namespace std

#endif  // MANIFEST_CONTENT_ID_H_
