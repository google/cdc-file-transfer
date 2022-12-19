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

#ifndef CDC_RSYNC_SERVER_CDC_RSYNC_SERVER_H_
#define CDC_RSYNC_SERVER_CDC_RSYNC_SERVER_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "cdc_rsync/base/message_pump.h"
#include "cdc_rsync_server/file_diff_generator.h"
#include "cdc_rsync_server/file_info.h"
#include "common/gamelet_component.h"
#include "common/path_filter.h"

namespace cdc_ft {

class MessagePump;
class ServerSocket;
class SocketFinalizer;

class CdcRsyncServer {
 public:
  CdcRsyncServer();
  ~CdcRsyncServer();

  // Checks that the gamelet components (cdc_rsync_server binary etc.) are
  // up-to-date by checking their sizes and timestamps.
  bool CheckComponents(const std::vector<GameletComponent>& components);

  // Listens to |port|, accepts a connection from the client and runs the rsync
  // procedure.
  absl::Status Run(int port);

  // Returns the verbosity sent from the client. 0 by default.
  int GetVerbosity() const { return verbosity_; }

 private:
  // Runs the rsync procedure.
  absl::Status Sync();

  // Receives options from the client.
  absl::Status HandleSetOptions();

  // Finds all server-side files in the |destination_| folder.
  absl::Status FindFiles();

  // Receives all client-side files.
  absl::Status HandleSendAllFiles();

  // Diffs client- and server-side files.
  absl::Status DiffFiles();

  // Deletes files and directories present on the server, but not on the client.
  absl::Status RemoveExtraneousFilesAndDirs();

  // Creates missing directories.
  absl::Status CreateMissingDirs();

  // Sends file indices to the client. Used for missing and changed files.
  template <typename T>
  absl::Status SendFileIndices(const char* file_type,
                               const std::vector<T>& files);

  // Receives missing files from the client.
  absl::Status HandleSendMissingFileData();

  // Core rsync algorithm. Sends signatures of changed files to the client,
  // receives diffs and applies them.
  absl::Status SyncChangedFiles();

  // Waits for the shutdown message and send an ack.
  absl::Status HandleShutdown();

  // Called on |message_pump_|'s receiver thread whenever a package is received.
  // Used to toggle decompression.
  void Thread_OnPackageReceived(PacketType type);

  // The order determines the correct destruction order, so keep it!
  std::unique_ptr<SocketFinalizer> socket_finalizer_;
  std::unique_ptr<ServerSocket> socket_;
  std::unique_ptr<MessagePump> message_pump_;

  std::string destination_;

  // Options.
  bool delete_ = false;
  bool recursive_ = false;
  int verbosity_ = 0;
  bool whole_file_ = false;
  bool compress_ = false;
  bool checksum_ = false;
  bool relative_ = false;
  bool dry_run_ = false;
  bool existing_ = false;
  std::string copy_dest_;

  PathFilter path_filter_;

  std::vector<FileInfo> client_files_;
  std::vector<DirInfo> client_dirs_;
  std::vector<FileInfo> server_files_;
  std::vector<DirInfo> server_dirs_;
  file_diff::Result diff_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_CDC_RSYNC_SERVER_H_
