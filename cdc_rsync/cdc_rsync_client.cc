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

#include "cdc_rsync/cdc_rsync_client.h"

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "cdc_rsync/base/cdc_interface.h"
#include "cdc_rsync/base/message_pump.h"
#include "cdc_rsync/base/server_exit_code.h"
#include "cdc_rsync/client_file_info.h"
#include "cdc_rsync/client_socket.h"
#include "cdc_rsync/file_finder_and_sender.h"
#include "cdc_rsync/parallel_file_opener.h"
#include "cdc_rsync/progress_tracker.h"
#include "cdc_rsync/protos/messages.pb.h"
#include "cdc_rsync/server_arch.h"
#include "cdc_rsync/zstd_stream.h"
#include "common/gamelet_component.h"
#include "common/log.h"
#include "common/path.h"
#include "common/port_manager.h"
#include "common/process.h"
#include "common/remote_util.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/stopwatch.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

// Bash exit code if binary could not be run, e.g. permission denied.
constexpr int kExitCodeCouldNotExecute = 126;

// Bash exit code if binary was not found.
constexpr int kExitCodeNotFound = 127;

SetOptionsRequest::FilterRule::Type ToProtoType(PathFilter::Rule::Type type) {
  switch (type) {
    case PathFilter::Rule::Type::kInclude:
      return SetOptionsRequest::FilterRule::TYPE_INCLUDE;
    case PathFilter::Rule::Type::kExclude:
      return SetOptionsRequest::FilterRule::TYPE_EXCLUDE;
  }
  assert(false);
  return SetOptionsRequest::FilterRule::TYPE_INCLUDE;
}

// Translates a server process exit code and stderr into a status.
absl::Status GetServerExitStatus(int exit_code, const std::string& error_msg) {
  auto se_code = static_cast<ServerExitCode>(exit_code);
  switch (se_code) {
    case kServerExitCodeGenericStartup:
      if (!error_msg.empty()) {
        return MakeStatus("Server returned error during startup: %s",
                          error_msg);
      }
      return MakeStatus(
          "Server exited with an unspecified error during startup");

    case kServerExitCodeOutOfDate:
      return MakeStatus(
          "Server exited since instance components are out of date");

    case kServerExitCodeGeneric:
      if (!error_msg.empty()) {
        return MakeStatus("Server returned error: %s", error_msg);
      }
      return MakeStatus("Server exited with an unspecified error");

    case kServerExitCodeAddressInUse:
      return SetTag(MakeStatus("Server failed to connect"), Tag::kAddressInUse);
  }

  // Could potentially happen if the server exits due to another reason,
  // e.g. some ssh.exe error (remember that the server process is actually
  // an ssh process).
  return MakeStatus("Server exited with code %i", exit_code);
}

}  // namespace

CdcRsyncClient::CdcRsyncClient(const Options& options,
                               std::vector<std::string> sources,
                               std::string user_host, std::string destination)
    : options_(options),
      sources_(std::move(sources)),
      destination_(std::move(destination)),
      printer_(options.quiet, Util::IsTTY() && !options.json),
      progress_(&printer_, options.verbosity, options.json) {
  // If there is no |user_host|, we sync files locally!
  if (!user_host.empty()) {
    remote_util_ =
        std::make_unique<RemoteUtil>(std::move(user_host), options.verbosity,
                                     options.quiet, &process_factory_,
                                     /*forward_output_to_log=*/false);
    if (!options_.ssh_command.empty()) {
      remote_util_->SetSshCommand(options_.ssh_command);
    }
    if (!options_.sftp_command.empty()) {
      remote_util_->SetSftpCommand(options_.sftp_command);
    }
  }

  // Note that remote_util_.get() may be null.
  port_manager_ = std::make_unique<PortManager>(
      "cdc_rsync_ports_f77bcdfe-368c-4c45-9f01-230c5e7e2132",
      options.forward_port_first, options.forward_port_last, &process_factory_,
      remote_util_.get());
}

CdcRsyncClient::~CdcRsyncClient() {
  message_pump_.StopMessagePump();
  socket_.Disconnect();
}

absl::Status CdcRsyncClient::Run() {
  // If |remote_util_| is not set, it's a local sync. Otherwise, guess the
  // architecture of the device that runs cdc_rsync_server from the destination
  // path, e.g. "C:\path\to\dest" strongly indicates Windows.
  ServerArch server_arch = !remote_util_
                               ? ServerArch::DetectFromLocalDevice()
                               : ServerArch::GuessFromDestination(destination_);

  int port;
  ASSIGN_OR_RETURN(port, FindAvailablePort(&server_arch),
                   "Failed to find available port");

  // Start the server process.
  absl::Status status = StartServer(port, server_arch);
  if (HasTag(status, Tag::kDeployServer) && server_arch.IsGuess() &&
      server_exit_code_ != kServerExitCodeOutOfDate) {
    // Server couldn't be run, e.g. not found or failed to start.
    // Check whether we guessed the arch type wrong and try again.
    // Note that in case of a local sync, or if the server actively reported
    // that it's out-dated, there's no need to detect the arch.
    LOG_DEBUG(
        "Failed to start server, retrying after detecting remote arch: %s",
        status.ToString());
    const ArchType old_type = server_arch.GetType();
    ASSIGN_OR_RETURN(server_arch,
                     ServerArch::DetectFromRemoteDevice(remote_util_.get()));
    if (server_arch.GetType() != old_type) {
      LOG_DEBUG("Guessed server arch type wrong, guessed %s, actual %s.",
                GetArchTypeStr(old_type), server_arch.GetTypeStr());
      status = StartServer(port, server_arch);
    }
  }

  if (HasTag(status, Tag::kDeployServer)) {
    // Gamelet components are not deployed or out-dated. Deploy and retry.
    status = DeployServer(server_arch);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to deploy server");
    }

    status = StartServer(port, server_arch);
  }
  if (!status.ok()) {
    return WrapStatus(status, "Failed to start server");
  }

  // Tag::kSocketEof most likely means that the server had an error exited. In
  // that case, try to shut it down properly to get more info from the error
  // message.
  status = Sync();
  if (!status.ok() && !HasTag(status, Tag::kSocketEof)) {
    return WrapStatus(status, "Failed to sync files");
  }

  absl::Status stop_status = StopServer();
  if (!stop_status.ok()) {
    return WrapStatus(stop_status, "Failed to stop server");
  }

  // If the server doesn't send any error information, return the sync status.
  if (server_error_.empty() && HasTag(status, Tag::kSocketEof)) {
    return status;
  }

  // Check exit code and stderr.
  if (server_exit_code_ != 0) {
    status = GetServerExitStatus(server_exit_code_, server_error_);
  }

  return status;
}

absl::StatusOr<int> CdcRsyncClient::FindAvailablePort(ServerArch* server_arch) {
  // Find available local and remote ports for port forwarding.
  // If only one port is in the given range, try that without checking.
  if (options_.forward_port_first >= options_.forward_port_last) {
    return options_.forward_port_first;
  }

  assert(server_arch);
  absl::StatusOr<int> port = port_manager_->ReservePort(
      options_.connection_timeout_sec, server_arch->GetType());

  if (absl::IsDeadlineExceeded(port.status())) {
    // Server didn't respond in time.
    return SetTag(port.status(), Tag::kConnectionTimeout);
  }
  if (absl::IsResourceExhausted(port.status())) {
    // Port in use.
    return SetTag(port.status(), Tag::kAddressInUse);
  }

  // If |server_arch| was guessed, calling netstat might have failed because
  // the arch was wrong. Properly detect it and try again if it changed.
  if (!port.ok() && server_arch->IsGuess()) {
    const ArchType old_type = server_arch->GetType();
    LOG_DEBUG(
        "Failed to reserve port, retrying after detecting remote arch: %s",
        port.status().ToString());
    ASSIGN_OR_RETURN(*server_arch,
                     ServerArch::DetectFromRemoteDevice(remote_util_.get()));
    assert(!server_arch->IsGuess());
    if (server_arch->GetType() != old_type) {
      LOG_DEBUG("Guessed server arch type wrong, guessed %s, actual %s.",
                GetArchTypeStr(old_type), server_arch->GetTypeStr());
      return FindAvailablePort(server_arch);
    }
  }

  return port;
}

absl::Status CdcRsyncClient::StartServer(int port, const ServerArch& arch) {
  assert(!server_process_);

  // Components are expected to reside in the same dir as the executable.
  std::string component_dir;
  absl::Status status = path::GetExeDir(&component_dir);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to get the executable directory");
  }

  std::vector<GameletComponent> components;
  status = GameletComponent::Get(
      {path::Join(component_dir, arch.CdcServerFilename())}, &components);
  if (!status.ok()) {
    return MakeStatus(
        "Required instance component not found. Make sure the file "
        "%s resides in the same folder as %s.",
        arch.CdcServerFilename(), ServerArch::CdcRsyncFilename());
  }
  std::string component_args = GameletComponent::ToCommandLineArgs(components);

  ProcessStartInfo start_info;
  start_info.name = "cdc_rsync_server";

  if (remote_util_) {
    // Run cdc_rsync_server on the remote instance.
    std::string remote_command = arch.GetStartServerCommand(
        kExitCodeNotFound, absl::StrFormat("%i %s", port, component_args));
    start_info = remote_util_->BuildProcessStartInfoForSshPortForwardAndCommand(
        port, port, /*reverse=*/false, remote_command, arch.GetType());
  } else {
    // Run cdc_rsync_server locally.
    std::string exe_dir;
    RETURN_IF_ERROR(path::GetExeDir(&exe_dir), "Failed to get exe directory");

    std::string server_path = path::Join(exe_dir, arch.CdcServerFilename());
    start_info.command =
        absl::StrFormat("%s %i %s", server_path, port, component_args);
  }

  // Capture stdout, but forward to stdout for debugging purposes.
  start_info.stdout_handler = [this](const char* data, size_t /*data_size*/) {
    return HandleServerOutput(data);
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  status = process->Start();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to start cdc_rsync_server process");
  }

  // Wait until the server process is listening.
  Stopwatch timeout_timer;
  bool is_timeout = false;
  auto detect_listening_or_timeout = [is_listening = &is_server_listening_,
                                      timeout = options_.connection_timeout_sec,
                                      &timeout_timer, &is_timeout]() -> bool {
    is_timeout = timeout_timer.ElapsedSeconds() > timeout;
    return *is_listening || is_timeout;
  };
  status = process->RunUntil(detect_listening_or_timeout);
  if (!status.ok()) {
    // Some internal process error. Note that this does NOT mean that
    // cdc_rsync_server does not exist. In that case, the ssh process exits with
    // code kExitCodeNotFound.
    return status;
  }
  if (is_timeout) {
    return SetTag(absl::DeadlineExceededError("Timeout while starting server"),
                  Tag::kConnectionTimeout);
  }

  if (process->HasExited()) {
    // Don't re-deploy for code > kServerExitCodeOutOfDate, which means that the
    // out-of-date check already passed on the server.
    server_exit_code_ = process->ExitCode();
    if (server_exit_code_ > kServerExitCodeOutOfDate &&
        server_exit_code_ <= kServerExitCodeMax) {
      return GetServerExitStatus(server_exit_code_, server_error_);
    }

    // Don't re-deploy if we're not copying to a remote device. We can start
    // cdc_rsync_server from the original location directly.
    if (!remote_util_) {
      return GetServerExitStatus(server_exit_code_, server_error_);
    }

    // Server exited before it started listening, most likely because of
    // outdated components (code kServerExitCodeOutOfDate) or because the server
    // wasn't deployed at all yet (code kExitCodeNotFound). Instruct caller
    // to re-deploy.
    return SetTag(MakeStatus("Redeploy server"), Tag::kDeployServer);
  }

  status = Socket::Initialize();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to initialize sockets");
  }
  socket_finalizer_ = std::make_unique<SocketFinalizer>();

  assert(is_server_listening_);
  status = socket_.Connect(port);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to initialize connection");
  }

  server_process_ = std::move(process);
  message_pump_.StartMessagePump();
  return absl::OkStatus();
}

absl::Status CdcRsyncClient::StopServer() {
  assert(server_process_);

  // Close socket.
  absl::Status status = socket_.ShutdownSendingEnd();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to shut down socket sending end");
  }

  status = server_process_->RunUntilExit();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to stop cdc_rsync_server process");
  }

  server_exit_code_ = server_process_->ExitCode();
  server_process_.reset();
  return absl::OkStatus();
}

absl::Status CdcRsyncClient::HandleServerOutput(const char* data) {
  // Note: This is called from a background thread!

  // Handle server error messages. Unfortunately, if the server prints to
  // stderr, the ssh process does not write it to its stderr, but to stdout, so
  // we have to jump through hoops to read the error. We use a marker char for
  // the start of the error message:
  //   This is stdout \x1e This is stderr \x1e This is stdout again
  std::string stdout_data_storage;
  const char* stdout_data = data;
  if (is_server_error_ || strchr(data, kServerErrorMarker)) {
    // Only run this expensive code if necessary.
    std::vector<std::string> parts =
        absl::StrSplit(data, absl::ByChar(kServerErrorMarker));
    for (size_t n = 0; n < parts.size(); ++n) {
      if (is_server_error_) {
        server_error_.append(parts[n]);
      } else {
        stdout_data_storage.append(parts[n]);
      }
      if (n + 1 < parts.size()) {
        is_server_error_ = !is_server_error_;
      }
    }
    stdout_data = stdout_data_storage.c_str();
  }

  printer_.Print(stdout_data, false, Util::GetConsoleWidth());
  if (!is_server_listening_) {
    server_output_.append(stdout_data);
    is_server_listening_ =
        server_output_.find("Server is listening") != std::string::npos;
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::Sync() {
  absl::Status status = SendOptions();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send options to server");
  }

  status = FindAndSendAllSourceFiles();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to find and send all source files");
  }

  status = ReceiveFileStats();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive file stats");
  }

  if (options_.delete_) {
    status = ReceiveDeletedFiles();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive paths of deleted files");
    }
  }

  status = ReceiveFileIndices("missing", &missing_file_indices_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive missing file indices");
  }
  status = SendMissingFiles();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send missing files");
  }

  status = ReceiveFileIndices("changed", &changed_file_indices_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive changed file indices");
  }

  status = ReceiveSignaturesAndSendDelta();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive signatures and send delta");
  }

  // Set sync point for shutdown (waits for the server to finish).
  ShutdownRequest shutdown_request;
  status = message_pump_.SendMessage(PacketType::kShutdown, shutdown_request);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send shutdown request");
  }

  ShutdownResponse response;
  status = message_pump_.ReceiveMessage(PacketType::kShutdown, &response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive shutdown response");
  }

  return status;
}

absl::Status CdcRsyncClient::DeployServer(const ServerArch& arch) {
  assert(!server_process_);
  assert(remote_util_);

  std::string exe_dir;
  absl::Status status = path::GetExeDir(&exe_dir);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to get exe directory");
  }

  std::string deploy_msg;
  if (server_exit_code_ == kExitCodeNotFound) {
    deploy_msg = "Server not deployed. Deploying...";
  } else if (server_exit_code_ == kExitCodeCouldNotExecute) {
    deploy_msg = "Server failed to start. Redeploying...";
  } else if (server_exit_code_ == kServerExitCodeOutOfDate) {
    deploy_msg = "Server outdated. Redeploying...";
  } else {
    deploy_msg = "Deploying server...";
  }
  printer_.Print(deploy_msg, true, Util::GetConsoleWidth());

  // sftp cdc_rsync_server to the target.
  std::string commands = arch.GetDeploySftpCommands();
  RETURN_IF_ERROR(remote_util_->Sftp(commands, exe_dir, /*compress=*/false),
                  "Failed to deploy cdc_rsync_server");

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::SendOptions() {
  LOG_INFO("Sending options");

  SetOptionsRequest request;
  request.set_destination(destination_);
  request.set_delete_(options_.delete_);
  request.set_recursive(options_.recursive);
  request.set_verbosity(options_.verbosity);
  request.set_whole_file(options_.whole_file);
  request.set_compress(options_.compress);
  request.set_relative(options_.relative);

  for (const PathFilter::Rule& rule : options_.filter.GetRules()) {
    SetOptionsRequest::FilterRule* filter_rule = request.add_filter_rules();
    filter_rule->set_type(ToProtoType(rule.type));
    filter_rule->set_pattern(rule.pattern);
  }

  request.set_checksum(options_.checksum);
  request.set_dry_run(options_.dry_run);
  request.set_existing(options_.existing);
  if (!options_.copy_dest.empty()) {
    request.set_copy_dest(options_.copy_dest);
  }

  absl::Status status =
      message_pump_.SendMessage(PacketType::kSetOptions, request);
  if (!status.ok()) {
    return WrapStatus(status, "SendDestination() failed");
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::FindAndSendAllSourceFiles() {
  LOG_INFO("Finding and sending all sources files");

  Stopwatch stopwatch;

  FileFinderAndSender file_finder(&options_.filter, &message_pump_, &progress_,
                                  options_.sources_dir, options_.recursive,
                                  options_.relative);

  progress_.StartFindFiles();
  for (const std::string& source : sources_) {
    absl::Status status = file_finder.FindAndSendFiles(source);
    if (!status.ok()) {
      return status;
    }
  }
  progress_.Finish();

  RETURN_IF_ERROR(file_finder.Flush(), "Failed to flush file finder");
  file_finder.ReleaseFiles(&files_);

  LOG_INFO("Found and sent %u source files in %0.3f seconds", files_.size(),
           stopwatch.ElapsedSeconds());

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::ReceiveFileStats() {
  LOG_INFO("Receiving file stats");

  SendFileStatsResponse response;
  absl::Status status =
      message_pump_.ReceiveMessage(PacketType::kSendFileStats, &response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive SendFileStatsResponse");
  }

  progress_.ReportFileStats(
      response.num_missing_files(), response.num_extraneous_files(),
      response.num_matching_files(), response.num_changed_files(),
      response.total_missing_bytes(), response.total_changed_client_bytes(),
      response.total_changed_server_bytes(), response.num_missing_dirs(),
      response.num_extraneous_dirs(), response.num_matching_dirs(),
      options_.whole_file, options_.checksum, options_.delete_);
  return absl::OkStatus();
}

absl::Status CdcRsyncClient::ReceiveDeletedFiles() {
  LOG_INFO("Receiving path of deleted files");
  std::string current_directory;

  progress_.StartDeleteFiles();
  for (;;) {
    AddDeletedFilesResponse response;
    absl::Status status =
        message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive AddDeletedFilesResponse");
    }

    // An empty response indicates that all files have been sent.
    if (response.files_size() == 0 && response.dirs_size() == 0) {
      break;
    }

    // Print info. Don't use path::Join(), it would mess up slashes.
    for (const std::string& file : response.files()) {
      progress_.ReportFileDeleted(response.directory() + file);
    }
    for (const std::string& dir : response.dirs()) {
      progress_.ReportDirDeleted(response.directory() + dir);
    }
  }
  progress_.Finish();

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::ReceiveFileIndices(
    const char* file_type, std::vector<uint32_t>* file_indices) {
  LOG_INFO("Receiving indices of %s files", file_type);

  for (;;) {
    AddFileIndicesResponse response;
    absl::Status status =
        message_pump_.ReceiveMessage(PacketType::kAddFileIndices, &response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive AddFileIndicesResponse");
    }

    // An empty response indicates that all files have been sent.
    if (response.client_indices_size() == 0) {
      break;
    }

    // Record file indices.
    file_indices->insert(file_indices->end(), response.client_indices().begin(),
                         response.client_indices().end());
  }

  // Validate indices.
  for (uint32_t index : *file_indices) {
    if (index >= files_.size()) {
      return MakeStatus("Received invalid index %u", index);
    }
  }

  LOG_INFO("Received %u indices of %s files", file_indices->size(), file_type);

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::SendMissingFiles() {
  if (missing_file_indices_.empty()) {
    return absl::OkStatus();
  }

  LOG_INFO("Sending missing files");

  if (options_.dry_run) {
    for (uint32_t client_index : missing_file_indices_) {
      const ClientFileInfo& file = files_[client_index];
      progress_.StartCopy(file.path.substr(file.base_dir_len), file.size);
      progress_.Finish();
    }
    return absl::OkStatus();
  }

  // This part is (optionally) compressed.
  if (options_.compress) {
    absl::Status status = StartCompressionStream();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to start compression process");
    }
  }

  ParallelFileOpener file_opener(&files_, missing_file_indices_);

  constexpr size_t kBufferSize = 128 * 1024;
  for (uint32_t server_index = 0; server_index < missing_file_indices_.size();
       ++server_index) {
    uint32_t client_index = missing_file_indices_[server_index];
    const ClientFileInfo& file = files_[client_index];

    LOG_INFO("%s", file.path);
    progress_.StartCopy(file.path.substr(file.base_dir_len), file.size);
    SendMissingFileDataRequest request;
    request.set_server_index(server_index);
    absl::Status status =
        message_pump_.SendMessage(PacketType::kSendMissingFileData, request);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send SendMissingFileDataRequest");
    }
    ProgressTracker* progress = &progress_;
    auto handler = [message_pump = &message_pump_, progress](const void* data,
                                                             size_t size) {
      progress->ReportCopyProgress(size);
      return message_pump->SendRawData(data, size);
    };

    FILE* fp = file_opener.GetNextOpenFile();
    if (!fp) {
      return MakeStatus("Failed to open file '%s'", file.path);
    }
    status = path::StreamReadFileContents(fp, kBufferSize, handler);
    fclose(fp);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to read file %s", file.path);
    }

    progress_.Finish();
  }

  if (options_.compress) {
    absl::Status status = StopCompressionStream();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to stop compression process");
    }
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::ReceiveSignaturesAndSendDelta() {
  if (changed_file_indices_.empty()) {
    return absl::OkStatus();
  }

  if (options_.dry_run) {
    for (uint32_t client_index : changed_file_indices_) {
      const ClientFileInfo& file = files_[client_index];
      progress_.StartSync(file.path.substr(file.base_dir_len), file.size,
                          file.size);
      progress_.ReportSyncProgress(file.size, file.size);
      progress_.Finish();
    }
    return absl::OkStatus();
  }

  LOG_INFO("Receiving signatures and sending deltas of changed files");

  // This part is (optionally) compressed.
  if (options_.compress) {
    absl::Status status = StartCompressionStream();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to start compression process");
    }
  }

  CdcInterface cdc(&message_pump_);

  // Open files in parallel. Speeds up many small file case.
  ParallelFileOpener file_opener(&files_, changed_file_indices_);

  std::string signature_data;
  for (uint32_t server_index = 0; server_index < changed_file_indices_.size();
       ++server_index) {
    uint32_t client_index = changed_file_indices_[server_index];
    const ClientFileInfo& file = files_[client_index];

    SendSignatureResponse response;
    absl::Status status =
        message_pump_.ReceiveMessage(PacketType::kAddSignatures, &response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive SendSignatureResponse");
    }

    // Validate index.
    if (response.client_index() != client_index) {
      return MakeStatus("Received invalid index %u. Expected %u.",
                        response.client_index(), client_index);
    }

    LOG_INFO("%s", file.path);
    progress_.StartSync(file.path.substr(file.base_dir_len), file.size,
                        response.server_file_size());

    FILE* fp = file_opener.GetNextOpenFile();
    if (!fp) {
      return MakeStatus("Failed to open file '%s'", file.path);
    }

    status = cdc.ReceiveSignatureAndCreateAndSendDiff(fp, &progress_);
    fclose(fp);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to sync file %s", file.path);
    }

    progress_.Finish();
  }

  if (options_.compress) {
    absl::Status status = StopCompressionStream();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to stop compression process");
    }
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::StartCompressionStream() {
  assert(!compression_stream_);

  // Notify server that data is compressed from now on.
  ToggleCompressionRequest request;
  absl::Status status =
      message_pump_.SendMessage(PacketType::kToggleCompression, request);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send ToggleCompressionRequest");
  }

  // Make sure the sender thread is idle.
  message_pump_.FlushOutgoingQueue();

  // Set up compression stream.
  uint32_t num_threads = std::thread::hardware_concurrency();
  compression_stream_ = std::make_unique<ZstdStream>(
      &socket_, options_.compress_level, num_threads);

  // Redirect the |message_pump_| output to the compression stream.
  message_pump_.RedirectOutput([this](const void* data, size_t size) {
    LOG_VERBOSE("Compressing packet of size %u", size);
    return compression_stream_->Write(data, size);
  });

  // The pipes are now set up like this:
  // |message_pump_| -> |compression_stream_| -> |socket_|.

  return absl::OkStatus();
}

absl::Status CdcRsyncClient::StopCompressionStream() {
  assert(compression_stream_);

  // Finish writing to |compression_process_|'s stdin and change back to
  // writing to the actual network socket.
  message_pump_.FlushOutgoingQueue();
  message_pump_.RedirectOutput(nullptr);

  // Finish compression stream and reset.
  RETURN_IF_ERROR(compression_stream_->Finish(),
                  "Failed to finish compression stream");
  compression_stream_.reset();

  // Wait for the server ack. This must be done before sending more data.
  ToggleCompressionResponse response;
  absl::Status status =
      message_pump_.ReceiveMessage(PacketType::kToggleCompression, &response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive ToggleCompressionResponse");
  }

  return absl::OkStatus();
}

}  // namespace cdc_ft
