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
#include "common/status.h"

namespace cdc_ft {
namespace {

ReturnCode TagToMessage(Tag tag, const std::string& user_host, int port,
                        std::string* msg) {
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

    case Tag::kConnectionTimeout:
      *msg = absl::StrFormat(kMsgFmtConnectionTimeout, user_host, port);
      return ReturnCode::kConnectionTimeout;

    case Tag::kCount:
      return ReturnCode::kGenericError;
  }

  // Should not happen (TM). Will fall back to status message in this case.
  return ReturnCode::kGenericError;
}

}  // namespace

ReturnCode Sync(const Options& options, const std::vector<std::string>& sources,
                const std::string& user_host, const std::string& destination,
                std::string* error_message) {
  error_message->clear();
  LogLevel log_level = Log::VerbosityToLogLevel(options.verbosity);
  Log::Initialize(std::make_unique<ConsoleLog>(log_level));

  // Run rsync.
  CdcRsyncClient client(options, sources, user_host, destination);
  absl::Status status = client.Run();

  if (status.ok()) {
    return ReturnCode::kOk;
  }

  ReturnCode code = ReturnCode::kGenericError;
  absl::optional<Tag> tag = GetTag(status);
  if (tag.has_value()) {
    code = TagToMessage(tag.value(), user_host, options.port, error_message);
  }

  // Fall back to status message.
  if (error_message->empty()) {
    *error_message = status.message();
  } else if (options.verbosity >= 2) {
    // In verbose mode, log the status as well, so nothing gets lost.
    LOG_ERROR("%s", status.ToString().c_str());
  }

  return code;
}

}  // namespace cdc_ft
