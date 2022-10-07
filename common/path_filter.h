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

#ifndef COMMON_PATH_FILTER_H_
#define COMMON_PATH_FILTER_H_

#include <string>
#include <vector>

namespace cdc_ft {
namespace internal {

// Returns true iff |path| matches |pattern|. Exposed for testing purposes.
bool IsMatch(const std::string& pattern, const std::string& path);

}  // namespace internal

// Applies include and exclude filter rules to a path.
class PathFilter {
 public:
  struct Rule {
    enum class Type {
      kInclude,
      kExclude,
    };

    Type type;
    std::string pattern;
    Rule(Type type, std::string pattern);
  };

  PathFilter();
  ~PathFilter();

  // Returns true if no rule has been added.
  bool IsEmpty() const;

  // Adds a rule to the set of filter rules. The |type| determines whether paths
  // that match |pattern| should be filtered in or out, see IsMatch(). |pattern|
  // is a Windows style file path pattern and supports * and ? wildcards. Does
  // not support general glob-style ** or [a-z] patterns.
  void AddRule(Rule::Type type, std::string pattern);

  // Returns all rules added.
  const std::vector<Rule>& GetRules() const { return rules_; }

  // Applies filter rules in the order they have been passed to AddRule().
  // Returns true if the first matching rule is of type |kInclude| or no
  // matching rule is found. Returns false if the first matching rule is
  // of type |kExclude|.
  bool IsMatch(const std::string& path) const;

 private:
  std::vector<Rule> rules_;
};

}  // namespace cdc_ft

#endif  // COMMON_PATH_FILTER_H_
