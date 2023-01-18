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

#ifndef CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_
#define CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_

#include <memory>

#include "absl/status/status.h"
#include "proto/local_assets_stream_manager.grpc.pb.h"

namespace grpc_impl {
class Channel;
}

namespace cdc_ft {

// gRpc client for starting/stopping asset streaming sessions.
class LocalAssetsStreamManagerClient {
 public:
  // |channel| is a grpc channel to use.
  explicit LocalAssetsStreamManagerClient(
      std::shared_ptr<grpc::Channel> channel);

  ~LocalAssetsStreamManagerClient();

  // Starts streaming |src_dir| to |user_host|:|mount_dir|.
  // Starting a second session to the same target will stop the first one.
  // |src_dir| is the Windows source directory to stream.
  // |user_host| is the Linux host, formatted as [user@:host].
  // |mount_dir| is the Linux target directory to stream to.
  // |ssh_command| is the ssh command and extra arguments to use.
  // |sftp_command| is the sftp command and extra arguments to use.
  absl::Status StartSession(const std::string& src_dir,
                            const std::string& user_host,
                            const std::string& mount_dir,
                            const std::string& ssh_command,
                            const std::string& sftp_command);

  // Stops the streaming session to the Linux target |user_host|:|mount_dir|.
  // |user_host| is the Linux host, formatted as [user@:host].
  // |mount_dir| is the Linux target directory.
  absl::Status StopSession(const std::string& user_host,
                           const std::string& mount_dir);

  // Helper function that splits "user@host:dir" into "user@host" and "dir".
  // Does not think that C: is a host.
  static absl::Status ParseUserHostDir(const std::string& user_host_dir,
                                       std::string* user_host,
                                       std::string* dir);

 private:
  using LocalAssetsStreamManager =
      localassetsstreammanager::LocalAssetsStreamManager;
  std::unique_ptr<LocalAssetsStreamManager::Stub> stub_;
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_LOCAL_ASSETS_STREAM_MANAGER_CLIENT_H_
