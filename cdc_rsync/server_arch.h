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

#ifndef CDC_RSYNC_SERVER_ARCH_H_
#define CDC_RSYNC_SERVER_ARCH_H_

#include <string>

namespace cdc_ft {

// Abstracts all architecture specifics of cdc_rsync_server deployment.
class ServerArch {
 public:
  enum class Type {
    kLinux = 0,
    kWindows = 1,
  };

  // Detects the architecture type based on the destination path, e.g. path
  // starting with C: indicate Windows.
  static Type Detect(const std::string& destination);

  ServerArch(Type type);
  ~ServerArch();

  // Returns the arch-specific filename of cdc_rsync_server[.exe].
  std::string CdcServerFilename() const;

  // Returns the arch-specific directory where cdc_rsync_server is deployed.
  std::string RemoteToolsBinDir() const;

  // Returns an arch-specific SSH shell command that gets invoked in order to
  // start cdc_rsync_server. The command
  // - returns |exit_code_not_found| if cdc_rsync_server does not exist (to
  //   prevent the confusing bash output message
  //     "bash: .../cdc_rsync_server: No such file or directory"), and
  // - runs the server with the provided |args|.
  std::string GetStartServerCommand(int exit_code_not_found,
                                    const std::string& args) const;

  // Returns an arch-specific SFTP command sequence that deploys the server
  // component on the target gets invoked after
  // cdc_rsync_server has been copied to a temp location. The commands
  // - create the cdc-file-transfer/bin folder if it doesn't exist yet,
  // - make the old cdc_rsync_server writable if it exists,
  // - copy cdc_rsync_server to a temp location,
  // - make the new cdc_rsync_server executable (Linux only) and
  // - replaces the existing cdc_rsync_server by the temp one.
  std::string GetDeploySftpCommands() const;

 private:
  Type type_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_ARCH_H_
