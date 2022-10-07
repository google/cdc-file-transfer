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

#include "absl_helper/jedec_size_flag.h"

namespace cdc_ft {

namespace {

JedecUnit ToJedecUnit(char c) {
  switch (c) {
    case 'b':
    case 'B':
      return JedecUnit::Byte;
    case 'k':
    case 'K':
      return JedecUnit::Kilo;
    case 'm':
    case 'M':
      return JedecUnit::Mega;
    case 'g':
    case 'G':
      return JedecUnit::Giga;
    case 't':
    case 'T':
      return JedecUnit::Tera;
    case 'p':
    case 'P':
      return JedecUnit::Peta;
    default:
      return JedecUnit::Unkown;
  }
}

int LeftShiftAmount(JedecUnit unit) {
  switch (unit) {
    case JedecUnit::Kilo:
      return 10;
    case JedecUnit::Mega:
      return 20;
    case JedecUnit::Giga:
      return 30;
    case JedecUnit::Tera:
      return 40;
    case JedecUnit::Peta:
      return 50;
    default:
      return 0;
  }
}

}  // namespace

bool AbslParseFlag(absl::string_view text, JedecSize* flag, std::string* err) {
  if (text.empty()) return false;
  JedecUnit unit = ToJedecUnit(text.back());
  if (unit != JedecUnit::Unkown) {
    text.remove_suffix(1);
  } else {
    // Are we dealing with a digit character?
    if (text.back() >= '0' && text.back() <= '9') {
      unit = JedecUnit::Byte;
    } else {
      *err =
          "Supported size units are (B)yte, (K)ilo, (M)ega, (G)iga, (T)era, "
          "(P)eta.";
      return false;
    }
  }
  // Try to parse a plain uint64_t value.
  uint64_t size;
  if (!absl::ParseFlag(text, &size, err)) {
    return false;
  }
  flag->SetSize(size << LeftShiftAmount(unit));
  return true;
}

std::string AbslUnparseFlag(const JedecSize& size) {
  return absl::UnparseFlag(size.Size());
}
};  // namespace cdc_ft
