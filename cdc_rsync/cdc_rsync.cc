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

#include "cdc_rsync/cdc_rsync.h"

#include <vector>

#include "cdc_rsync/cdc_rsync_client.h"
#include "cdc_rsync/error_messages.h"
#include "common/log.h"
#include "common/path_filter.h"
#include "common/status.h"

namespace cdc_ft {
namespace {

ReturnCode TagToMessage(Tag tag, const Options* options, std::string* msg) {
  msg->clear();
  switch (tag) {
    case Tag::kSocketEof:
      *msg = kMsgConnectionLost;
      return ReturnCode::kConnectionLost;

    case Tag::kAddressInUse:
      *msg = kMsgAddressInUse;
      return ReturnCode::kAddressInUse;

    case Tag::kDeployServer:
      *msg = kMsgDeployFailed;
      return ReturnCode::kDeployFailed;

    case Tag::kInstancePickerNotAvailableInQuietMode:
      *msg = kMsgInstancePickerNotAvailableInQuietMode;
      return ReturnCode::kInstancePickerNotAvailableInQuietMode;

    case Tag::kConnectionTimeout:
      *msg =
          absl::StrFormat(kMsgFmtConnectionTimeout, options->ip, options->port);
      return ReturnCode::kConnectionTimeout;

    case Tag::kCount:
      return ReturnCode::kGenericError;
  }

  // Should not happen (TM). Will fall back to status message in this case.
  return ReturnCode::kGenericError;
}

PathFilter::Rule::Type ToInternalType(FilterRule::Type type) {
  switch (type) {
    case FilterRule::Type::kInclude:
      return PathFilter::Rule::Type::kInclude;
    case FilterRule::Type::kExclude:
      return PathFilter::Rule::Type::kExclude;
  }
  assert(false);
  return PathFilter::Rule::Type::kInclude;
}

}  // namespace

ReturnCode Sync(const Options* options, const FilterRule* filter_rules,
                size_t num_filter_rules, const char* sources_dir,
                const char* const* sources, size_t num_sources,
                const char* destination, const char** error_message) {
  LogLevel log_level = Log::VerbosityToLogLevel(options->verbosity);
  Log::Initialize(std::make_unique<ConsoleLog>(log_level));

  PathFilter path_filter;
  for (size_t n = 0; n < num_filter_rules; ++n) {
    path_filter.AddRule(ToInternalType(filter_rules[n].type),
                        filter_rules[n].pattern);
  }

  std::vector<std::string> sources_vec;
  for (size_t n = 0; n < num_sources; ++n) {
    sources_vec.push_back(sources[n]);
  }

  // Run rsync.
  GgpRsyncClient client(*options, std::move(path_filter), sources_dir,
                        std::move(sources_vec), destination);
  absl::Status status = client.Run();

  if (status.ok()) {
    *error_message = nullptr;
    return ReturnCode::kOk;
  }

  std::string msg;
  ReturnCode code = ReturnCode::kGenericError;
  absl::optional<Tag> tag = GetTag(status);
  if (tag.has_value()) {
    code = TagToMessage(tag.value(), options, &msg);
  }

  // Fall back to status message.
  if (msg.empty()) {
    msg = std::string(status.message());
  } else if (options->verbosity >= 2) {
    // In verbose mode, log the status as well, so nothing gets lost.
    LOG_ERROR("%s", status.ToString().c_str());
  }

  // Store error message in static buffer (don't use std::string through DLL
  // boundary!).
  static char buf[1024] = {0};
  strncpy_s(buf, msg.c_str(), _TRUNCATE);
  *error_message = buf;

  return code;
}

}  // namespace cdc_ft
