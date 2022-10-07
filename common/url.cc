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

#include "common/url.h"

#include <ctype.h>

#include <regex>

#include "absl/strings/str_format.h"

namespace cdc_ft {

absl::StatusOr<Url> Url::Parse(const std::string& url_string) {
  std::string url_copy = url_string;
  std::for_each(url_copy.begin(), url_copy.end(),
                [](char& c) { c = tolower(c); });
  std::regex re("(https?)://([^:^?/]+)(?::([0-9]+))?.*");
  std::smatch match;
  if (!std::regex_match(url_copy, match, re))
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("'%s' is not a valid url.", url_string.c_str()));
  Url url;
  url.protocol = match[1].str();
  url.host = match[2].str();
  if (match.size() == 4 && match[3].length() > 0)
    url.port = match[3].str();
  else if (url.protocol.back() == 's')
    url.port = "443";
  else
    url.port = "80";
  return url;
}

Url::Url() = default;

Url::Url(std::string protocol, std::string host, std::string port)
    : protocol(protocol), host(host), port(port) {}

Url::~Url() = default;

bool operator==(const Url& lhs, const Url& rhs) {
  return lhs.host == rhs.host && lhs.port == rhs.port &&
         lhs.protocol == rhs.protocol;
}

}  // namespace cdc_ft
