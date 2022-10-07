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

namespace cdc_ft {
namespace internal {

bool IsMatch(const char* pattern, const char* path, size_t path_len) {
  for (/*empty*/; *pattern != '\0'; ++pattern) {
    switch (*pattern) {
      case '?':
        if (*path == '\0') {
          return false;
        }
        ++path;
        break;

      case '*': {
        if (pattern[1] == '\0') {
          return true;
        }
        for (size_t n = 0; n < path_len; n++) {
          if (IsMatch(pattern + 1, path + n, path_len - n)) {
            return true;
          }
        }
        return false;
      }

      default:
        if (*path != *pattern) {
          return false;
        }
        ++path;
    }
  }
  return *path == '\0';
}

bool IsMatch(const std::string& pattern, const std::string& path) {
  return IsMatch(pattern.c_str(), path.c_str(), path.size());
}

}  // namespace internal

PathFilter::PathFilter() = default;

PathFilter::~PathFilter() = default;

bool PathFilter::IsEmpty() const { return rules_.empty(); }

void PathFilter::AddRule(Rule::Type type, std::string pattern) {
  rules_.emplace_back(type, std::move(pattern));
}

bool PathFilter::IsMatch(const std::string& path) const {
  for (const Rule& rule : rules_) {
    if (internal::IsMatch(rule.pattern.c_str(), path.c_str(), path.size())) {
      return rule.type == Rule::Type::kInclude;
    }
  }
  return true;
}

PathFilter::Rule::Rule(Type type, std::string pattern)
    : type(type), pattern(std::move(pattern)) {}

}  // namespace cdc_ft
