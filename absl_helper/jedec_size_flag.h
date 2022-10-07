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

#ifndef ABSL_HELPER_JEDEC_SIZE_FLAG_H_
#define ABSL_HELPER_JEDEC_SIZE_FLAG_H_

#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/marshalling.h"
#include "absl/strings/string_view.h"

namespace cdc_ft {

// Supported JEDEC unit suffixes.
enum class JedecUnit : char {
  Unkown = 0,
  Byte = 'B',  // optional
  Kilo = 'K',
  Mega = 'M',
  Giga = 'G',
  Tera = 'T',
  Peta = 'P',
};

// This class parses flag arguments that represent human readable data sizes,
// such as 1024, 2K, 3M, 4G, or 5T.
//
// See https://en.wikipedia.org/wiki/JEDEC_memory_standards.
class JedecSize {
 public:
  explicit JedecSize(uint64_t size = 0) : size_(size) {}
  uint64_t Size() const { return size_; }
  void SetSize(uint64_t size) { size_ = size; }

 private:
  uint64_t size_;
};

// Abseil flags parser for JedecSize.
bool AbslParseFlag(absl::string_view text, JedecSize* flag, std::string* err);

// Abseil flags unparser for JedecSize.
std::string AbslUnparseFlag(const JedecSize& size);

};  // namespace cdc_ft

#endif  // ABSL_HELPER_JEDEC_SIZE_FLAG_H_
