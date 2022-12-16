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

#ifndef COMMON_PORT_RANGE_PARSER_H_
#define COMMON_PORT_RANGE_PARSER_H_

#include <cstdint>

namespace cdc_ft {
namespace port_range {

// Parses |value| into a port range |first|-|last|.
// If |value| is a single number a, assigns |first|=|last|=a.
// If |value| is a range a-b, assigns |first|=a, |last|=b.
bool Parse(const char* value, uint16_t* first, uint16_t* last);

}  // namespace port_range
}  // namespace cdc_ft

#endif  // COMMON_PORT_RANGE_PARSER_H_
