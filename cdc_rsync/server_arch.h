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

#include "absl/status/statusor.h"
#include "common/arch_type.h"

namespace cdc_ft {

class RemoteUtil;

// Abstracts all architecture specifics of cdc_rsync_server deployment.
// Comes in two flavors, "guessed" and "detected". Guesses are used as an
// optimization. For instance, if one syncs to C:\some\path, it's clearly a
// Windows machine and we can skip detection.
class ServerArch {
 public:
  // Guesses the arch type based on the destination path, e.g. path starting
  // with C: indicate Windows. This is a guessed type. It may be wrong. For
  // instance, if destination is just a single folder like "foo", the method
  // defaults to Type::kLinux.
  static ServerArch GuessFromDestination(const std::string& destination);

  // Returns the arch type that matches the current process's type.
  // This is not a guessed type, it is reliable.
  static ServerArch DetectFromLocalDevice();

  // Creates an  by properly detecting it on the remote device.
  // This is more costly than guessing, but it is reliable.
  static absl::StatusOr<ServerArch> DetectFromRemoteDevice(
      RemoteUtil* remote_util);

  // Returns the (local!) arch specific filename of cdc_rsync[.exe].
  static std::string CdcRsyncFilename();

  ServerArch(ArchType type, bool is_guess);
  ~ServerArch();

  // Accessor for the arch type.
  ArchType GetType() const { return type_; }

  // Returns the type as a human readable string.
  const char* GetTypeStr() const;

  // Returns true if the type was guessed and not detected.
  bool IsGuess() const { return is_guess_; }

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
  ArchType type_;
  bool is_guess_ = false;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_ARCH_H_
