/*
 * Copyright 2023 Google LLC
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

#ifndef COMMON_ANSI_FILTER_H_
#define COMMON_ANSI_FILTER_H_

#include <string>

namespace cdc_ft {
namespace ansi_filter {

// Removes ANSI escape sequences from a string.
// |input| is a string that can contain ANSI escape sequences.
// Returns the filtered string with ANSI escape sequences removed.
// Example: The most common escape sequence sets a color, e.g.
//            "This \x1b[1;32merror\x1b[0m is red."
//          The filtered output is
//            "This error is red."
std::string RemoveEscapeSequences(const std::string& input);

}  // namespace ansi_filter
}  // namespace cdc_ft

#endif  // COMMON_ANSI_FILTER_H_
