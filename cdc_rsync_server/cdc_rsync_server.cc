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

#include "cdc_rsync_server/cdc_rsync_server.h"

#include "absl/strings/str_format.h"
#include "cdc_rsync/base/cdc_interface.h"
#include "cdc_rsync/protos/messages.pb.h"
#include "cdc_rsync_server/file_deleter_and_sender.h"
#include "cdc_rsync_server/file_finder.h"
#include "cdc_rsync_server/unzstd_stream.h"
#include "common/log.h"
#include "common/path.h"
#include "common/server_socket.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/stopwatch.h"
#include "common/threadpool.h"
#include "common/util.h"

namespace cdc_ft {

namespace {

// Number of files for which to call fclose() and finalize files in parallel.
constexpr size_t kNumFinalizerThreads = 8;

// Max 16 files in the patcher and finalizer queues to prevent that too many
// files are open concurrently.
constexpr size_t kMaxQueueSize = 16;

// Suffix for the patched file created from the basis file and the diff.
constexpr char kIntermediatePathSuffix[] = ".__cdc_rsync_temp__";

#if PLATFORM_WINDOWS
constexpr char kServerFilename[] = "cdc_rsync_server.exe";
#elif PLATFORM_LINUX
constexpr char kServerFilename[] = "cdc_rsync_server";
#endif

uint16_t kExecutableBits =
    path::MODE_IXUSR | path::MODE_IXGRP | path::MODE_IXOTH;

// Background task that receives patch info for the base file at |base_filepath|
// and writes the patched file to |target_filepath|. If |base_filepath| and
// |target_filepath| match, writes an intermediate file and replaces
// the file at |target_filepath| with the intermediate file when all data has
// been received.
// Each PatchTask is queued twice, once to create the patched file, and once in
// a different thread pool to close and finalize the patched file. This is
// because fclose() can take a long time to finish, so it could block patching.
class PatchTask : public Task {
 public:
  PatchTask(const std::string& base_filepath,
            const std::string& target_filepath, const ChangedFileInfo& file,
            CdcInterface* cdc)
      : base_filepath_(base_filepath),
        target_filepath_(target_filepath),
        file_(file),
        cdc_(cdc),
        need_intermediate_file_(target_filepath_ == base_filepath_),
        patched_filepath_(target_filepath_ == base_filepath_
                              ? base_filepath_ + kIntermediatePathSuffix
                              : target_filepath_) {}

  virtual ~PatchTask() = default;

  PatchTask(const PatchTask& other) = delete;
  PatchTask& operator=(const PatchTask& other) = delete;

  const ChangedFileInfo& File() const { return file_; }

  const absl::Status& Status() const { return status_; }

  // Task:
  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    // Each PatchTask is queued twice, once to apply the patch and once to
    // close and finalize the patched file.
    switch (state_) {
      case State::kPatching:
        Patch();
        state_ = State::kFinalizing;
        break;
      case State::kFinalizing:
        Finalize();
        state_ = State::kDone;
        break;
      default:
        assert(!"Invalid state");
    }
  }

 private:
  void Patch() {
    absl::StatusOr<FILE*> patched_fp = path::OpenFile(patched_filepath_, "wb");
    if (!patched_fp.ok()) {
      status_ = patched_fp.status();
      return;
    }
    patched_fp_ = *patched_fp;

    // Receive diff stream from server and apply.
    status_ =
        cdc_->ReceiveDiffAndPatch(base_filepath_, patched_fp_, &is_executable_);

    // The file is closed by Finalize() in a separate thread pool since fclose()
    // takes a while on some systems.
  }

  void Finalize() {
    if (patched_fp_) {
      fclose(patched_fp_);
      patched_fp_ = nullptr;
    }

    if (!status_.ok()) {
      // Some error occurred during Patch().
      return;
    }

    // These bits are OR'ed on top of the mode bits.
    uint16_t mode_or_bits = is_executable_ ? kExecutableBits : 0;

    // Store mode from the original base path.
    path::Stats stats;
    status_ = GetStats(base_filepath_, &stats);
    if (!status_.ok()) {
      status_ =
          WrapStatus(status_, "GetStats() failed for '%s'", base_filepath_);
      return;
    }

    if (need_intermediate_file_) {
      // Replace |base_filepath_| (==|target_filepath_|) by the intermediate
      // file |patched_filepath|.
      status_ = path::ReplaceFile(target_filepath_, patched_filepath_);
      if (!status_.ok()) {
        status_ = WrapStatus(status_, "ReplaceFile() for '%s' by '%s' failed",
                             base_filepath_, patched_filepath_);
        return;
      }
    } else {
      // An intermediate file is typically not needed when the base path is
      // a file in a package. Since package files are read-only, we add the
      // write bit, so that the file can be overwritten with the next sync.
      mode_or_bits |= path::Mode::MODE_IWUSR;
    }

    // Restore mode, possibly adding executable bit and user write bit.
    status_ = path::ChangeMode(target_filepath_, stats.mode | mode_or_bits);
    if (!status_.ok()) {
      status_ =
          WrapStatus(status_, "ChangeMode() failed for '%s'", base_filepath_);
      return;
    }

    status_ = path::SetFileTime(target_filepath_, file_.client_modified_time);
  }

  const std::string base_filepath_;
  const std::string target_filepath_;
  const ChangedFileInfo file_;
  CdcInterface* const cdc_;
  const bool need_intermediate_file_ = false;
  const std::string patched_filepath_;

  FILE* patched_fp_ = nullptr;
  bool is_executable_ = false;
  absl::Status status_;

  // This task is queued twice, once to patch and once to close and finalize the
  // patched file.
  enum class State { kPatching, kFinalizing, kDone };
  State state_ = State::kPatching;
};

// Background task that closes a file and sets the mtime and perms. This is done
// in the background since closing a file might block for a long time.
class FinalizeCopiedFileTask : public Task {
 public:
  // Finalize |file| with given path |filepath|. |status| is the status from
  // writing the file. On error, the file is only closed.
  FinalizeCopiedFileTask(FILE* fp, FileInfo file, std::string filepath,
                         bool is_executable, absl::Status status)
      : fp_(fp),
        file_(std::move(file)),
        filepath_(std::move(filepath)),
        is_executable_(is_executable),
        status_(status) {}
  virtual ~FinalizeCopiedFileTask() = default;

  FinalizeCopiedFileTask(const FinalizeCopiedFileTask& other) = delete;
  FinalizeCopiedFileTask& operator=(const FinalizeCopiedFileTask& other) =
      delete;

  const absl::Status& Status() const { return status_; }

  // Task:
  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    assert(fp_);
    fclose(fp_);

    if (!status_.ok()) {
      // Writing the file failed, nothing to finalize.
      status_ = WrapStatus(status_, "Failed to write file %s", filepath_);
      return;
    }

    // Set file write time.
    status_ = path::SetFileTime(filepath_, file_.modified_time);
    if (!status_.ok()) {
      status_ =
          WrapStatus(status_, "Failed to set file mod time for %s", filepath_);
      return;
    }

    // Set executable bit, but just print warnings as it's not critical.
    if (is_executable_) {
      path::Stats stats;
      status_ = path::GetStats(filepath_, &stats);
      if (status_.ok()) {
        status_ = path::ChangeMode(filepath_, stats.mode | kExecutableBits);
      }
      if (!status_.ok()) {
        LOG_WARNING("Failed to set executable bit on '%s': %s", filepath_,
                    status_.ToString());
      }
    }
  }

 private:
  FILE* const fp_ = nullptr;
  const FileInfo file_;
  const std::string filepath_;
  const bool is_executable_;
  absl::Status status_;
};

PathFilter::Rule::Type ToInternalType(
    SetOptionsRequest::FilterRule::Type type) {
  switch (type) {
    case SetOptionsRequest::FilterRule::TYPE_INCLUDE:
      return PathFilter::Rule::Type::kInclude;
    case SetOptionsRequest::FilterRule::TYPE_EXCLUDE:
      return PathFilter::Rule::Type::kExclude;
    // Make compiler happy...
    case SetOptionsRequest_FilterRule_Type_SetOptionsRequest_FilterRule_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
    case SetOptionsRequest_FilterRule_Type_SetOptionsRequest_FilterRule_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
      break;
  }
  assert(false);
  return PathFilter::Rule::Type::kInclude;
}

}  // namespace

CdcRsyncServer::CdcRsyncServer() = default;

CdcRsyncServer::~CdcRsyncServer() = default;

bool CdcRsyncServer::CheckComponents(
    const std::vector<GameletComponent>& components) {
  // Components are expected to reside in the same dir as the executable.
  std::string component_dir;
  absl::Status status = path::GetExeDir(&component_dir);
  if (!status.ok()) {
    return false;
  }

  std::vector<GameletComponent> our_components;
  status = GameletComponent::Get({path::Join(component_dir, kServerFilename)},
                                 &our_components);
  if (!status.ok() || components != our_components) {
    return false;
  }

  return true;
}

absl::Status CdcRsyncServer::Run() {
  RETURN_IF_ERROR(Socket::Initialize(), "Failed to initialize sockets");
  socket_finalizer_ = std::make_unique<SocketFinalizer>();

  socket_ = std::make_unique<ServerSocket>();
  int port;
  ASSIGN_OR_RETURN(port, socket_->StartListening(0),
                   "Failed to start listening for connections");
  LOG_INFO("cdc_rsync_server listening on port %i", port);

  // This is the marker for the client, so it knows it can connect.
  // Print port first so the client can easily parse it when it sees "Server is
  // listening" without dealing with half-transmitted data.
  printf("Port %i: Server is listening\n", port);
  fflush(stdout);

  RETURN_IF_ERROR(socket_->WaitForConnection(),
                  "Failed to establish a connection");

  message_pump_ = std::make_unique<MessagePump>(
      socket_.get(),
      [this](PacketType type) { Thread_OnPackageReceived(type); });
  message_pump_->StartMessagePump();

  LOG_INFO("Client connected. Starting to sync.");
  absl::Status status = Sync();
  if (!status.ok()) {
    socket_->ShutdownSendingEnd().IgnoreError();
    return status;
  }

  LOG_INFO("Exiting cdc_rsync_server");
  return absl::OkStatus();
}

absl::Status CdcRsyncServer::Sync() {
  // First, the client sends us options, e.g. the |destination_| directory.
  absl::Status status = HandleSetOptions();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive options");
  }

  // Find all files in the |destination_| and |copy_dest_| (if set) directories.
  status = FindFiles();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to find files on instance");
  }

  // Get the list of all files that the client sends us.
  status = HandleSendAllFiles();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive client file info");
  }

  // Diff client and server files and send missing files to the client.
  status = DiffFiles();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to compute file difference");
  }

  // Delete files and directories not present on the client.
  if (delete_) {
    status = RemoveExtraneousFilesAndDirs();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to delete files and directories");
    }
  }

  if (!dry_run_) {
    status = CreateMissingDirs();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to create missing directories");
    }
  }

  // Send indices of missing files to the client
  status = SendFileIndices("missing", diff_.missing_files);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send indices of missing files");
  }

  if (!dry_run_) {
    // Get file data of missing files from client.
    status = HandleSendMissingFileData();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to copy files");
    }
  }
  // Send indices of changed files to the client.
  status = SendFileIndices("changed", diff_.changed_files);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send indices of changed files");
  }

  if (!dry_run_) {
    // Applies the rsync algorithm to update the changed files.
    status = SyncChangedFiles();
    if (!status.ok()) {
      return WrapStatus(status, "Failed to sync files");
    }
  }

  // Handle clean shutdown.
  status = HandleShutdown();
  if (!status.ok()) {
    return WrapStatus(status, "Shutdown failed");
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::HandleSetOptions() {
  LOG_INFO("Receiving options");

  SetOptionsRequest request;
  absl::Status status =
      message_pump_->ReceiveMessage(PacketType::kSetOptions, &request);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive SetOptionsRequest");
  }

  destination_ = request.destination();
  delete_ = request.delete_();
  recursive_ = request.recursive();
  verbosity_ = request.verbosity();
  whole_file_ = request.whole_file();
  compress_ = request.compress();
  checksum_ = request.checksum();
  relative_ = request.relative();
  dry_run_ = request.dry_run();
  existing_ = request.existing();
  copy_dest_ = request.copy_dest();

  // Support \ instead of / in destination folders.
  path::FixPathSeparators(&destination_);
  path::EnsureEndsWithPathSeparator(&destination_);
  if (!copy_dest_.empty()) {
    path::FixPathSeparators(&copy_dest_);
    path::EnsureEndsWithPathSeparator(&copy_dest_);
  }

  // Expand e.g. ~.
  status = path::ExpandPathVariables(&destination_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to expand destination '%s'",
                      destination_);
  }

  assert(path_filter_.IsEmpty());
  for (int n = 0; n < request.filter_rules_size(); ++n) {
    std::string fixed_pattern = request.filter_rules(n).pattern();
    path::FixPathSeparators(&fixed_pattern);
    path_filter_.AddRule(ToInternalType(request.filter_rules(n).type()),
                         fixed_pattern);
  }

  Log::Instance()->SetLogLevel(Log::VerbosityToLogLevel(verbosity_));

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::FindFiles() {
  Stopwatch stopwatch;
  FileFinder finder;

  LOG_INFO("Finding all files in destination folder '%s'", destination_);
  absl::Status status =
      finder.AddFiles(destination_, recursive_, &path_filter_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to search '%s'", destination_);
  }

  if (!copy_dest_.empty()) {
    LOG_INFO("Finding all files in copy-dest folder '%s'", copy_dest_);
    status = finder.AddFiles(copy_dest_, recursive_, &path_filter_);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to search '%s'", copy_dest_);
    }
  }

  finder.ReleaseFiles(&server_files_, &server_dirs_);

  LOG_INFO("Found and set %u source files in %0.3f seconds",
           server_files_.size(), stopwatch.ElapsedSeconds());
  return absl::OkStatus();
}

absl::Status CdcRsyncServer::HandleSendAllFiles() {
  std::string current_directory;

  for (;;) {
    AddFilesRequest request;
    absl::Status status =
        message_pump_->ReceiveMessage(PacketType::kAddFiles, &request);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive AddFilesRequest");
    }

    // An empty request indicates that all files have been sent.
    if (request.files_size() == 0 && request.dirs_size() == 0) {
      return absl::OkStatus();
    }

    current_directory = request.directory();
    path::FixPathSeparators(&current_directory);

    // Add client files.
    for (const AddFilesRequest::File& file : request.files()) {
      uint32_t client_index = client_files_.size();
      client_files_.emplace_back(path::Join(current_directory, file.filename()),
                                 file.modified_time(), file.size(),
                                 client_index, nullptr);
    }
    // Add client directories.
    for (const std::string& dir : request.dirs()) {
      uint32_t client_index = client_dirs_.size();
      client_dirs_.emplace_back(path::Join(current_directory, dir),
                                client_index, nullptr);
    }
  }
}

absl::Status CdcRsyncServer::DiffFiles() {
  LOG_INFO("Diffing files");

  // Be sure to move the data. It can grow quite large with millions of files.
  // Special case for relative, non-recursive mode. This puts files with a
  // relative directory into the "missing" bucket since the server-side search
  // doesn't look into sub-folders. Double check that they are really missing.
  const bool double_check_missing = relative_ && !recursive_;
  diff_ =
      file_diff::Generate(std::move(client_files_), std::move(server_files_),
                          std::move(client_dirs_), std::move(server_dirs_),
                          destination_, copy_dest_, double_check_missing);

  // Take sync flags into account and generate the stats response.
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      existing_, checksum_, whole_file_, &diff_);

  // Send stats.
  absl::Status status =
      message_pump_->SendMessage(PacketType::kSendFileStats, response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send SendFileStatsResponse");
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::RemoveExtraneousFilesAndDirs() {
  FileDeleterAndSender deleter(message_pump_.get());

  // To guarantee that the folders are empty before they are removed, files are
  // removed first.
  for (const FileInfo& file : diff_.extraneous_files) {
    absl::Status status = deleter.DeleteAndSendFileOrDir(
        destination_, file.filepath, dry_run_, false);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to delete file '%s' and send info",
                        file.filepath);
    }
  }

  // To guarantee that the subfolders are removed first.
  std::sort(diff_.extraneous_dirs.begin(), diff_.extraneous_dirs.end(),
            [](const DirInfo& dir1, const DirInfo& dir2) {
              return dir1.filepath > dir2.filepath;
            });
  for (const DirInfo& dir : diff_.extraneous_dirs) {
    absl::Status status = deleter.DeleteAndSendFileOrDir(
        destination_, dir.filepath, dry_run_, true);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to delete directory '%s' and send info",
                        dir.filepath);
    }
  }

  // Send remaining files to the client.
  absl::Status status = deleter.Flush();
  if (!status.ok()) {
    return WrapStatus(
        status,
        "Failed to send info of remaining deleted files and directories");
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::CreateMissingDirs() {
  for (const DirInfo& dir : diff_.missing_dirs) {
    // Make directory.
    std::string path = path::Join(destination_, dir.filepath);
    std::error_code error_code;
    // A file with the same name already exists.
    if (path::Exists(path)) {
      assert(!diff_.extraneous_files.empty());
      absl::Status status = path::RemoveFile(path);
      if (!status.ok()) {
        return WrapStatus(
            status, "Failed to remove file '%s' before creating folder '%s'",
            path, path);
      }
    }
    absl::Status status = path::CreateDirRec(path);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to create directory %s", path);
    }
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status CdcRsyncServer::SendFileIndices(const char* file_type,
                                             const std::vector<T>& files) {
  LOG_INFO("Sending indices of %s files to client", file_type);
  constexpr char error_fmt[] = "Failed to send indices of %s files.";

  AddFileIndicesResponse response;
  absl::Status status;
  for (const T& file : files) {
    response.add_client_indices(file.client_index);

    constexpr int kMaxBatchSize = 4000;
    if (response.client_indices_size() >= kMaxBatchSize) {
      status =
          message_pump_->SendMessage(PacketType::kAddFileIndices, response);
      response.clear_client_indices();
    }

    if (!status.ok()) {
      return WrapStatus(status, error_fmt, file_type);
    }
  }

  // Send the rest.
  if (response.client_indices_size() > 0) {
    status = message_pump_->SendMessage(PacketType::kAddFileIndices, response);
    if (!status.ok()) {
      return WrapStatus(status, error_fmt, file_type);
    }

    response.clear_client_indices();
  }

  // Send an empty response to indicate that we're done.
  status = message_pump_->SendMessage(PacketType::kAddFileIndices, response);
  if (!status.ok()) {
    return WrapStatus(status, error_fmt, file_type);
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::HandleSendMissingFileData() {
  if (diff_.missing_files.empty()) {
    return absl::OkStatus();
  }

  LOG_INFO("Receiving missing files");

  // Expect start of compression. The server socket will actually handle
  // compression transparently, there's nothing we have to do here.
  if (compress_) {
    ToggleCompressionRequest request;
    absl::Status status =
        message_pump_->ReceiveMessage(PacketType::kToggleCompression, &request);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive ToggleCompressionRequest");
    }
  }

  Threadpool finalize_pool(kNumFinalizerThreads);

  for (uint32_t server_index = 0; server_index < diff_.missing_files.size();
       server_index++) {
    const FileInfo& file = diff_.missing_files[server_index];
    std::string filepath = path::Join(destination_, file.filepath);
    LOG_INFO("%s", filepath.c_str());

    SendMissingFileDataRequest request;
    absl::Status status = message_pump_->ReceiveMessage(
        PacketType::kSendMissingFileData, &request);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive SendMissingFileDataRequest");
    }

    // Verify if we got the right index.
    if (request.server_index() != server_index) {
      return MakeStatus("Received wrong server index %u. Expected %u.",
                        request.server_index(), server_index);
    }

    // Remove |filepath| if it is a directory.
    if (path::DirExists(filepath)) {
      assert(!diff_.extraneous_dirs.empty());
      status = path::RemoveFile(filepath);
      if (!status.ok()) {
        return WrapStatus(
            status, "Failed to remove folder '%s' before creating file '%s'",
            filepath, filepath);
      }
    }

    // Make directory.
    std::string dir = path::DirName(filepath);
    std::error_code error_code;
    status = path::CreateDirRec(dir);
    if (!status.ok()) {
      return MakeStatus("Failed to create directory %s: %s", dir,
                        error_code.message());
    }

    // Receive file data.
    Buffer buffer;
    bool is_executable = false;
    bool first_chunk = true;
    auto handler = [message_pump = message_pump_.get(), &buffer, &is_executable,
                    &first_chunk](const void** data, size_t* size) {
      absl::Status status = message_pump->ReceiveRawData(&buffer);
      if (!status.ok()) {
        return status;
      }

      // size 0 indicates EOF.
      *data = buffer.size() > 0 ? buffer.data() : nullptr;
      *size = buffer.size();

      // Detect executables.
      if (first_chunk && buffer.size() > 0) {
        first_chunk = false;
        is_executable = Util::IsExecutable(buffer.data(), buffer.size());
      }

      return absl::OkStatus();
    };

    absl::StatusOr<FILE*> fp = path::OpenFile(filepath, "wb");
    if (!fp.ok()) {
      return fp.status();
    }

    status = path::StreamWriteFileContents(*fp, handler);
    finalize_pool.QueueTask(std::make_unique<FinalizeCopiedFileTask>(
        *fp, file, filepath, is_executable, status));
    finalize_pool.WaitForQueuedTasksAtMost(kMaxQueueSize);

    // Drain finalize pool for the last file.
    if (server_index + 1 == diff_.missing_files.size()) {
      finalize_pool.Wait();
    }

    // Check the results of completed tasks.
    for (std::unique_ptr<Task> task = finalize_pool.TryGetCompletedTask();
         task != nullptr; task = finalize_pool.TryGetCompletedTask()) {
      const FinalizeCopiedFileTask* finalize_task =
          static_cast<FinalizeCopiedFileTask*>(task.get());
      if (!finalize_task->Status().ok()) {
        // Close and finish files that have already been copied, so we don't
        // discard several already copied files because one failed.
        finalize_pool.Wait();
        return finalize_task->Status();
      }
    }
  }

  // Notify client that it can resume sending (uncompressed!) messages.
  if (compress_) {
    ToggleCompressionResponse response;
    absl::Status status =
        message_pump_->SendMessage(PacketType::kToggleCompression, response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send ToggleCompressionResponse");
    }
  }

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::SyncChangedFiles() {
  if (diff_.changed_files.empty()) {
    return absl::OkStatus();
  }

  LOG_INFO("Synching changed files");

  // Expect start of compression. The server socket will actually handle
  // compression transparently, there's nothing we have to do here.
  if (compress_) {
    ToggleCompressionRequest request;
    absl::Status status =
        message_pump_->ReceiveMessage(PacketType::kToggleCompression, &request);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive ToggleCompressionRequest");
    }
  }

  CdcInterface cdc(message_pump_.get());

  // Pipeline sending signatures and patching files:
  // MAIN THREAD:       Send signatures to client.
  //                    Only sends to the socket.
  // PATCHER THREAD:    Receive diffs from client and create patch file.
  //                    Only reads from the socket.
  // FINALIZER THREADS: Close patched files and finalize them.
  Threadpool patch_pool(1);
  Threadpool finalize_pool(kNumFinalizerThreads);

  // Forward finished patch task immediately to finalize pool.
  patch_pool.SetTaskCompletedCallback(
      [&finalize_pool](std::unique_ptr<Task> task) {
        // Spin if there are too many outstanding tasks, in order to limit the
        // max number of outstanding tasks.
        finalize_pool.QueueTask(std::move(task));
        finalize_pool.WaitForQueuedTasksAtMost(kMaxQueueSize);
      });

  for (uint32_t server_index = 0; server_index < diff_.changed_files.size();
       server_index++) {
    const ChangedFileInfo& file = diff_.changed_files[server_index];
    std::string base_filepath =
        path::Join(file.base_dir ? file.base_dir : destination_, file.filepath);
    std::string target_filepath = path::Join(destination_, file.filepath);
    LOG_INFO("%s -> %s", base_filepath, target_filepath);

    SendSignatureResponse response;
    response.set_client_index(file.client_index);
    response.set_server_file_size(file.server_size);
    absl::Status status =
        message_pump_->SendMessage(PacketType::kAddSignatures, response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send SendSignatureResponse");
    }

    // Create and send signature.
    status = cdc.CreateAndSendSignature(base_filepath);
    if (!status.ok()) {
      return status;
    }

    // Queue patching task.
    patch_pool.QueueTask(std::make_unique<PatchTask>(
        base_filepath, target_filepath, file, &cdc));

    // Drain pools for the last file.
    if (server_index + 1 == diff_.changed_files.size()) {
      patch_pool.Wait();
      finalize_pool.Wait();
    }

    // Check the results of completed tasks.
    for (std::unique_ptr<Task> task = finalize_pool.TryGetCompletedTask();
         task != nullptr; task = finalize_pool.TryGetCompletedTask()) {
      const PatchTask* patch_task = static_cast<PatchTask*>(task.get());
      const std::string& task_path = patch_task->File().filepath;
      if (!patch_task->Status().ok()) {
        // Close and finish files that have already been synced, so we don't
        // discard several already synced files because one failed.
        finalize_pool.Wait();
        return WrapStatus(patch_task->Status(), "Failed to patch file '%s'",
                          task_path);
      }
      LOG_INFO("Finished patching file %s", task_path.c_str());
    }
  }

  // Notify client that it can resume sending (uncompressed!) messages.
  if (compress_) {
    ToggleCompressionResponse response;
    absl::Status status =
        message_pump_->SendMessage(PacketType::kToggleCompression, response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send ToggleCompressionResponse");
    }
  }

  LOG_INFO("Successfully synced %u files", diff_.changed_files.size());

  return absl::OkStatus();
}

absl::Status CdcRsyncServer::HandleShutdown() {
  ShutdownRequest request;
  absl::Status status =
      message_pump_->ReceiveMessage(PacketType::kShutdown, &request);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to receive ShutdownRequest");
  }

  ShutdownResponse response;
  status = message_pump_->SendMessage(PacketType::kShutdown, response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send ShutdownResponse");
  }

  return absl::OkStatus();
}

void CdcRsyncServer::Thread_OnPackageReceived(PacketType type) {
  if (type != PacketType::kToggleCompression) {
    return;
  }

  // Turn on decompression.
  message_pump_->RedirectInput(std::make_unique<UnzstdStream>(socket_.get()));
}

}  // namespace cdc_ft
