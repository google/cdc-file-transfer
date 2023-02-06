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

#include "cdc_rsync/base/server_exit_code.h"
#include "cdc_rsync_server/cdc_rsync_server.h"
#include "common/build_version.h"
#include "common/gamelet_component.h"
#include "common/log.h"
#include "common/status.h"

namespace {

void SendErrorMessage(const char* msg) {
  constexpr char marker = cdc_ft::kServerErrorMarker;
  fprintf(stderr, "%c%s%c", marker, msg, marker);
}

}  // namespace

namespace cdc_ft {

// Returns custom error codes based on the tag associated with |status|. This is
// used to display custom error messages on the client.
// Example: A bind failure usually means two instances are in use
//          simultaneously.
ServerExitCode GetExitCode(const absl::Status& status) {
  absl::optional<Tag> tag = GetTag(status);
  if (!tag.has_value()) {
    return kServerExitCodeGeneric;
  }

  // Some tags translate to a special error message on the client.
  switch (tag.value()) {
    case Tag::kAddressInUse:
      // Can't bind port, probably two instances in use simultaneously.
      return kServerExitCodeAddressInUse;

    case Tag::kSocketEof:
      // Usually means client disconnected and shut down already.
    case Tag::kDeployServer:
    case Tag::kConnectionTimeout:
    case Tag::kCount:
      // Should not happen in server.
      break;
  }

  return kServerExitCodeGeneric;
}

}  // namespace cdc_ft

int main(int argc, const char** argv) {
  if (argc < 5) {
    printf(R"("cdc_rsync_server - Remote component of cdc_rsync. Version: %s

Usage: cdc_rsync_server <build_version> cdc_rsync_server <size> <modified_time>
       <build_version>            build version embedded in the component
       <size>                     file size of the component
       <modified_time>            timestamp (Unix epoch) of the component)",
           BUILD_VERSION);

    return cdc_ft::kServerExitCodeGenericStartup;
  }

  // The rest is expected to be sets of gamelet component info consisting of
  // (version, filename, size, modified_time). This is used check whether the
  // components are up-to-date.
  std::vector<cdc_ft::GameletComponent> components =
      cdc_ft::GameletComponent::FromCommandLineArgs(argc - 1, argv + 1);

  cdc_ft::Log::Initialize(
      std::make_unique<cdc_ft::ConsoleLog>(cdc_ft::LogLevel::kWarning));
  cdc_ft::CdcRsyncServer server;

  if (!server.CheckComponents(components)) {
    return cdc_ft::kServerExitCodeOutOfDate;
  }

  absl::Status status = server.Run();
  if (status.ok()) {
    return 0;
  }

  cdc_ft::ServerExitCode code = cdc_ft::GetExitCode(status);

  // Print full error in verbose mode, so that it's not lost.
  if (server.GetVerbosity() >= 2) {
    fprintf(stderr, "Server error: %s\n", status.ToString().c_str());
  }

  // Send error message to the client and return code.
  SendErrorMessage(std::string(status.message()).c_str());
  return code;
}
