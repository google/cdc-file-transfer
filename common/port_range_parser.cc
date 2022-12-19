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

#include <cassert>

#include "absl/strings/str_split.h"

namespace cdc_ft {
namespace port_range {

bool Parse(const char* value, uint16_t* first, uint16_t* last) {
  assert(value);
  *first = 0;
  *last = 0;
  std::vector<std::string> parts = absl::StrSplit(value, '-');
  if (parts.empty() || parts.size() > 2) return false;
  const int ifirst = atoi(parts[0].c_str());
  const int ilast = parts.size() > 1 ? atoi(parts[1].c_str()) : ifirst;
  if (ifirst <= 0 || ifirst > UINT16_MAX) return false;
  if (ilast <= 0 || ilast > UINT16_MAX || ifirst > ilast) return false;
  *first = static_cast<uint16_t>(ifirst);
  *last = static_cast<uint16_t>(ilast);
  return true;
}

}  // namespace port_range
}  // namespace cdc_ft
