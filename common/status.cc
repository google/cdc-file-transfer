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

#include "common/status.h"

#include "absl/strings/numbers.h"

namespace cdc_ft {
namespace {

constexpr char kTagKey[] = "tag";

absl::Cord TagToCord(Tag tag) {
  return absl::Cord(std::to_string(static_cast<int>(tag)));
}

absl::optional<Tag> CordToTag(absl::optional<absl::Cord> cord) {
  if (!cord.has_value()) return {};

  int value;
  absl::numbers_internal::safe_strto32_base(std::string(cord.value()), &value,
                                            10);
  return value >= 0 && value < static_cast<int>(Tag::kCount)
             ? static_cast<Tag>(value)
             : absl::optional<Tag>();
}

}  // namespace

absl::Status SetTag(absl::Status status, Tag tag) {
  status.SetPayload(kTagKey, TagToCord(tag));
  return status;
}

bool HasTag(const absl::Status& status, Tag tag) {
  return status.GetPayload(kTagKey) == TagToCord(tag);
}

absl::optional<Tag> GetTag(const absl::Status& status) {
  return CordToTag(status.GetPayload(kTagKey));
}

}  // namespace cdc_ft
