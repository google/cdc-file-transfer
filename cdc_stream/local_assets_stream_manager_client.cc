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

#include "cdc_stream/local_assets_stream_manager_client.h"

#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "common/grpc_status.h"

namespace cdc_ft {

using StartSessionRequest = localassetsstreammanager::StartSessionRequest;
using StartSessionResponse = localassetsstreammanager::StartSessionResponse;
using StopSessionRequest = localassetsstreammanager::StopSessionRequest;
using StopSessionResponse = localassetsstreammanager::StopSessionResponse;

LocalAssetsStreamManagerClient::LocalAssetsStreamManagerClient(
    std::shared_ptr<grpc::Channel> channel) {
  stub_ = LocalAssetsStreamManager::NewStub(std::move(channel));
}

LocalAssetsStreamManagerClient::~LocalAssetsStreamManagerClient() = default;

absl::Status LocalAssetsStreamManagerClient::StartSession(
    const std::string& src_dir, const std::string& user_host,
    const std::string& mount_dir, const std::string& ssh_command,
    const std::string& sftp_command) {
  StartSessionRequest request;
  request.set_workstation_directory(src_dir);
  request.set_user_host(user_host);
  request.set_mount_dir(mount_dir);
  request.set_ssh_command(ssh_command);
  request.set_sftp_command(sftp_command);

  grpc::ClientContext context;
  StartSessionResponse response;
  return ToAbslStatus(stub_->StartSession(&context, request, &response));
}

absl::Status LocalAssetsStreamManagerClient::StopSession(
    const std::string& user_host, const std::string& mount_dir) {
  StopSessionRequest request;
  request.set_user_host(user_host);
  request.set_mount_dir(mount_dir);

  grpc::ClientContext context;
  StopSessionResponse response;
  return ToAbslStatus(stub_->StopSession(&context, request, &response));
}

// static
absl::Status LocalAssetsStreamManagerClient::ParseUserHostDir(
    const std::string& user_host_dir, std::string* user_host,
    std::string* dir) {
  std::vector<std::string> parts =
      absl::StrSplit(user_host_dir, absl::MaxSplits(':', 1));
  if (parts.size() < 2 ||
      (parts[0].size() == 1 && toupper(parts[0][0]) >= 'A' &&
       toupper(parts[0][0]) <= 'Z')) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Failed to parse '%s'. Make sure it is of the form "
                        "[user@]host:linux_dir.",
                        user_host_dir));
  }

  *user_host = parts[0];
  *dir = parts[1];
  return absl::OkStatus();
}

}  // namespace cdc_ft
