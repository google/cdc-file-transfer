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

#ifndef COMMON_URL_H_
#define COMMON_URL_H_

#include "absl/status/statusor.h"

namespace cdc_ft {

struct Url {
  Url();
  Url(std::string protocol, std::string host, std::string port);
  ~Url();

  std::string protocol;
  std::string host;
  std::string port;

  // Parses |url_string| to Url struct from a given HTTP(S) URL.
  // Uses a very simple parser, e.g. does not unescape host (e.g. foo%20bar).
  static absl::StatusOr<Url> Parse(const std::string& url_string);
};

bool operator==(const Url& lhs, const Url& rhs);

}  // namespace cdc_ft

#endif  // COMMON_URL_H_
