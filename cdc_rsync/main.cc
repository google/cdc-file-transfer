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

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <string>
#include <vector>

#include "cdc_rsync/cdc_rsync_client.h"
#include "cdc_rsync/params.h"
#include "common/log.h"
#include "common/status.h"
#include "common/util.h"

namespace {

enum class ReturnCode {
  // No error. Will match the tool's exit code, so OK must be 0.
  kOk = 0,

  // Generic error.
  kGenericError = 1,

  // Server connection timed out.
  kConnectionTimeout = 2,

  // Connection to the server was shut down unexpectedly.
  kConnectionLost = 3,

  // Binding to the forward port failed, probably because there's another
  // instance of cdc_rsync running.
  kAddressInUse = 4,

  // Server deployment failed. This should be rare, it means that the server
  // components were successfully copied, but the up-to-date check still fails.
  kDeployFailed = 5,
};

ReturnCode TagToMessage(cdc_ft::Tag tag,
                        const cdc_ft::params::Parameters& params,
                        std::string* msg) {
  msg->clear();
  switch (tag) {
    case cdc_ft::Tag::kSocketEof:
      // Receiving pipe end was shut down unexpectedly.
      *msg = "The connection to the instance was shut down unexpectedly.";
      return ReturnCode::kConnectionLost;

    case cdc_ft::Tag::kAddressInUse:
      *msg =
          "Failed to establish a connection to the instance. All ports are "
          "already in use. This can happen if another instance of this command "
          "is running. Currently, only 10 simultaneous connections are "
          "supported.";
      return ReturnCode::kAddressInUse;

    case cdc_ft::Tag::kDeployServer:
      *msg =
          "Failed to deploy or run the instance components for unknown "
          "reasons. Please report this issue.";
      return ReturnCode::kDeployFailed;

    case cdc_ft::Tag::kConnectionTimeout:
      // Server connection timed out. SSH probably stale.
      *msg = absl::StrFormat(
          "Server connection timed out. Verify that the host '%s' "
          "is correct, or specify a larger timeout with --contimeout.",
          params.user_host);
      return ReturnCode::kConnectionTimeout;

    case cdc_ft::Tag::kCount:
      return ReturnCode::kGenericError;
  }

  // Should not happen (TM). Will fall back to status message in this case.
  return ReturnCode::kGenericError;
}

}  // namespace

int wmain(int argc, wchar_t* argv[]) {
  // Convert args from wide to UTF8 strings.
  std::vector<std::string> utf8_str_args;
  utf8_str_args.reserve(argc);
  for (int i = 0; i < argc; i++) {
    utf8_str_args.push_back(cdc_ft::Util::WideToUtf8Str(argv[i]));
  }

  // Convert args from UTF8 strings to UTF8 c-strings.
  std::vector<const char*> utf8_args;
  utf8_args.reserve(argc);
  for (const auto& utf8_str_arg : utf8_str_args) {
    utf8_args.push_back(utf8_str_arg.c_str());
  }

  // Read parameters from the environment and the command line.
  cdc_ft::params::Parameters parameters;
  if (!cdc_ft::params::Parse(argc, utf8_args.data(), &parameters)) {
    return 1;
  }

  // Initialize logging.
  cdc_ft::LogLevel log_level =
      cdc_ft::Log::VerbosityToLogLevel(parameters.options.verbosity);
  cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(log_level));

  // Run rsync.
  cdc_ft::CdcRsyncClient client(parameters.options, parameters.sources,
                                parameters.user_host, parameters.destination);
  absl::Status status = client.Run();
  if (status.ok()) {
    return static_cast<int>(ReturnCode::kOk);
  }

  // Get an error message from the tag associated with the status.
  std::string error_message;
  ReturnCode code = ReturnCode::kGenericError;
  absl::optional<cdc_ft::Tag> tag = cdc_ft::GetTag(status);
  if (tag.has_value()) {
    code = TagToMessage(tag.value(), parameters, &error_message);
  }

  // Fall back to status message if there was no tag.
  if (error_message.empty()) {
    error_message = status.message();
  } else if (parameters.options.verbosity >= 2) {
    // In verbose mode, log the status as well, so nothing gets lost.
    LOG_ERROR("%s", status.ToString());
  }

  if (!error_message.empty()) {
    fprintf(stderr, "Error: %s\n", error_message.c_str());
  }
  return static_cast<int>(code);
}
