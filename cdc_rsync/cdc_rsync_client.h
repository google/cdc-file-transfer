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

#ifndef CDC_RSYNC_CDC_RSYNC_CLIENT_H_
#define CDC_RSYNC_CDC_RSYNC_CLIENT_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cdc_rsync/base/message_pump.h"
#include "cdc_rsync/client_socket.h"
#include "cdc_rsync/progress_tracker.h"
#include "common/path_filter.h"
#include "common/process.h"

namespace cdc_ft {

class PortManager;
class Process;
class RemoteUtil;
class ServerArch;
class ZstdStream;

class CdcRsyncClient {
 public:
  struct Options {
    bool delete_ = false;
    bool recursive = false;
    int verbosity = 0;
    bool quiet = false;
    bool whole_file = false;
    bool relative = false;
    bool compress = false;
    bool checksum = false;
    bool dry_run = false;
    bool existing = false;
    bool json = false;
    std::string copy_dest;
    int compress_level = 6;
    int connection_timeout_sec = 10;
    int forward_port_first = 44450;
    int forward_port_last = 44459;
    std::string ssh_command;
    std::string sftp_command;
    std::string sources_dir;  // Base dir for files loaded for --files-from.
    PathFilter filter;

    // Backwards compatibility for switching from scp to sftp.
    // Used internally, do not use.
    std::string deprecated_scp_command;

    // Compression level 0 is invalid.
    static constexpr int kMinCompressLevel = -5;
    static constexpr int kMaxCompressLevel = 22;
  };

  CdcRsyncClient(const Options& options, std::vector<std::string> sources,
                 std::string user_host, std::string destination);

  ~CdcRsyncClient();

  // Deploys the server if necessary, starts it and runs the rsync procedure.
  absl::Status Run();

 private:
  // Finds available local and remote ports for port forwarding.
  // May update |server_arch| by properly detecting the architecture and retry
  // if the architecture was guessed, i.e. if |server_arch|->IsGuess() is true.
  absl::StatusOr<int> FindAvailablePort(ServerArch* server_arch);

  // Starts the server process. If the method returns a status with tag
  // |kTagDeployServer|, Run() calls DeployServer() and tries again.
  absl::Status StartServer(int port, const ServerArch& arch);

  // Stops the server process.
  absl::Status StopServer();

  // Handler for stdout and stderr data emitted by the server.
  absl::Status HandleServerOutput(const char* data);

  // Runs the rsync procedure.
  absl::Status Sync();

  // Copies all gamelet components to the gamelet.
  absl::Status DeployServer(const ServerArch& arch);

  // Sends relevant options to the server.
  absl::Status SendOptions();

  // Finds all source files and sends the file infos to the server.
  absl::Status FindAndSendAllSourceFiles();

  // Receives the stats from the file diffs (e.g. number of missing, changed
  // etc. files) from the server.
  absl::Status ReceiveFileStats();

  // Receives paths of deleted files and prints them out.
  absl::Status ReceiveDeletedFiles();

  // Receives file indices from the server. Used for missing and changed files.
  absl::Status ReceiveFileIndices(const char* file_type,
                                  std::vector<uint32_t>* file_indices);

  // Copies missing files to the server.
  absl::Status SendMissingFiles();

  // Core rsync algorithm. Receives signatures of changed files from server,
  // calculates the diffs and sends them to the server.
  absl::Status ReceiveSignaturesAndSendDelta();

  // Start the zstd compression stream. Used before file copy and diff.
  absl::Status StartCompressionStream();

  // Stops the zstd compression stream.
  absl::Status StopCompressionStream();

  Options options_;
  std::vector<std::string> sources_;
  const std::string destination_;
  WinProcessFactory process_factory_;
  std::unique_ptr<RemoteUtil> remote_util_;
  std::unique_ptr<PortManager> port_manager_;
  std::unique_ptr<SocketFinalizer> socket_finalizer_;
  ClientSocket socket_;
  MessagePump message_pump_{&socket_, MessagePump::PacketReceivedDelegate()};
  ConsoleProgressPrinter printer_;
  ProgressTracker progress_;
  std::unique_ptr<ZstdStream> compression_stream_;

  std::unique_ptr<Process> server_process_;
  std::string server_output_;  // Written in a background thread. Do not access
  std::string server_error_;   // while the server process is active.
  int server_exit_code_ = 0;
  std::atomic_bool is_server_listening_{false};
  bool is_server_error_ = false;

  // All source files found on the client.
  std::vector<ClientFileInfo> files_;

  // All source dirs found on the client.
  std::vector<ClientDirInfo> dirs_;

  // Indices (into files_) of files that are missing on the server.
  std::vector<uint32_t> missing_file_indices_;

  // Indices (into files_) of files that exist, but are different on the server.
  std::vector<uint32_t> changed_file_indices_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_CDC_RSYNC_CLIENT_H_
