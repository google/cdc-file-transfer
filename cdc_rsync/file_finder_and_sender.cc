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

#include "cdc_rsync/file_finder_and_sender.h"

#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "cdc_rsync/base/message_pump.h"
#include "common/log.h"
#include "common/path.h"
#include "common/path_filter.h"
#include "common/status.h"

namespace cdc_ft {
namespace {

bool EndsWithSpecialDir(const std::string& source) {
  return source == "." || source == ".." || absl::EndsWith(source, "\\.") ||
         absl::EndsWith(source, "\\..");
}

// Returns C:\ from C:\path\to\file or an empty string if there is no drive.
std::string GetDrivePrefixWithBackslash(const std::string& source) {
  std::string prefix = path::GetDrivePrefix(source);
  if (source[prefix.size()] == '\\') {
    prefix += "\\";
  }
  return prefix;
}

// Basically returns |sources_dir| + |source|, but removes drive letters from
// |source| if present and |sources_dir| is not empty.
std::string GetFullSource(const std::string& source,
                          const std::string& sources_dir) {
  if (sources_dir.empty()) {
    return source;
  }

  // Combine |sources_dir_| and |source|, but remove the drive prefix, so
  // that we don't get stuff like "source_dir\C:\path\to\file".
  return path::Join(sources_dir,
                    source.substr(GetDrivePrefixWithBackslash(source).size()));
}

std::string GetBaseDir(const std::string& source,
                       const std::string& sources_dir, bool relative) {
  if (!relative) {
    // For non-relative mode, the base dir is the directory part, so that
    // path\to\file is copied to remote_dir/file and files in path\to\ are
    // copied to remote_dir.
    if (path::EndsWithPathSeparator(source)) return source;
    std::string dir = path::DirName(source);
    if (!dir.empty()) path::EnsureEndsWithPathSeparator(&dir);
    return dir;
  }

  // A "\.\" is a marker for where the relative path should start.
  // The base dir is the part up to that marker, so that
  // path\.\to\file is copied to remote_dir/to/file.
  size_t pos = source.find("\\.\\");
  if (pos != std::string::npos) {
    return source.substr(0, pos + 3);
  }

  // If there is a sources dir, the base dir is the sources dir, so that
  // sources_dir\path\to\file is copied to remote_dir/path/to/file.
  if (!sources_dir.empty()) {
    assert(source.find(sources_dir) == 0);
    return sources_dir;
  }

  // If there is a drive prefix, the base dir is that part, so that
  // C:\path\to\file is copied to remote_dir/path/to/file.
  return GetDrivePrefixWithBackslash(source);
}

}  // namespace

FileFinderAndSender::FileFinderAndSender(PathFilter* path_filter,
                                         MessagePump* message_pump,
                                         ReportFindFilesProgress* progress,
                                         std::string sources_dir,
                                         bool recursive, bool relative,
                                         size_t request_byte_threshold)
    : path_filter_(path_filter),
      message_pump_(message_pump),
      progress_(progress),
      sources_dir_(std::move(sources_dir)),
      recursive_(recursive),
      relative_(relative),
      request_size_threshold_(request_byte_threshold) {
  // Support / instead of \ in the source folder.
  path::FixPathSeparators(&sources_dir_);
}

FileFinderAndSender::~FileFinderAndSender() = default;

absl::Status FileFinderAndSender::FindAndSendFiles(std::string source) {
  // Support / instead of \ in sources.
  path::FixPathSeparators(&source);
  // Special case, "." and ".." should not specify the directory, but the files
  // inside this directory!
  if (EndsWithSpecialDir(source)) {
    path::EnsureEndsWithPathSeparator(&source);
  }

  // Combine |source| and |sources_dir_| if present.
  std::string full_source = GetFullSource(source, sources_dir_);

  // Get the part of the path to remove before sending it to the server.
  base_dir_ = GetBaseDir(full_source, sources_dir_, relative_);

  size_t prev_size = files_.size() + dirs_.size();

  auto handler = [this](std::string dir, std::string filename,
                        int64_t modified_time, uint64_t size,
                        bool is_directory) {
    return HandleFoundFileOrDir(std::move(dir), std::move(filename),
                                modified_time, size, is_directory);
  };

  absl::Status status = path::SearchFiles(full_source, recursive_, handler);
  if (!status.ok()) {
    return WrapStatus(status,
                      "Failed to gather source files and directories for '%s'",
                      full_source);
  }

  if (files_.size() + dirs_.size() == prev_size) {
    LOG_WARNING("Neither files nor directories found that match source '%s'",
                full_source.c_str());
    // This isn't fatal.
  }

  return absl::OkStatus();
}

absl::Status FileFinderAndSender::Flush() {
  // Flush remaining files.
  absl::Status status = SendFilesAndDirs();
  if (!status.ok()) {
    return WrapStatus(status, "SendFilesAndDirs() failed");
  }

  // Send an empty batch as EOF indicator.
  assert(request_.files_size() == 0);
  status = message_pump_->SendMessage(PacketType::kAddFiles, request_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send EOF indicator");
  }

  return absl::OkStatus();
}

void FileFinderAndSender::ReleaseFiles(std::vector<ClientFileInfo>* files) {
  *files = std::move(files_);
}

void FileFinderAndSender::ReleaseDirs(std::vector<ClientDirInfo>* dirs) {
  *dirs = std::move(dirs_);
}

absl::Status FileFinderAndSender::HandleFoundFileOrDir(std::string dir,
                                                       std::string filename,
                                                       int64_t modified_time,
                                                       uint64_t size,
                                                       bool is_directory) {
  std::string relative_dir = dir.substr(base_dir_.size());

  // Is the path excluded? Check IsEmpty() first to save the path::Join()
  // if no filter is used (pretty common case).
  if (!path_filter_->IsEmpty() &&
      !path_filter_->IsMatch(path::Join(relative_dir, filename))) {
    return absl::OkStatus();
  }
  if (is_directory) {
    progress_->ReportDirFound();
  } else {
    progress_->ReportFileFound();
  }

  if (request_.directory() != relative_dir) {
    // Flush files in previous directory.
    absl::Status status = SendFilesAndDirs();
    if (!status.ok()) {
      return WrapStatus(status, "SendFilesAndDirs() failed");
    }

    // Set new directory.
    request_.set_directory(relative_dir);
    request_size_ = request_.directory().length();
  }

  if (is_directory) {
    dirs_.emplace_back(path::Join(dir, filename),
                       static_cast<uint32_t>(base_dir_.size()));
    request_.add_dirs(filename);
    request_size_ += filename.size();
  } else {
    files_.emplace_back(path::Join(dir, filename), size,
                        static_cast<uint32_t>(base_dir_.size()));

    AddFilesRequest::File* file = request_.add_files();
    file->set_filename(filename);
    file->set_modified_time(modified_time);
    file->set_size(size);
    // The serialized proto might have a slightly different length due to
    // packing, but this doesn't need to be exact.
    request_size_ += filename.size() + sizeof(modified_time) + sizeof(size);
  }
  if (request_size_ >= request_size_threshold_) {
    absl::Status status = SendFilesAndDirs();
    if (!status.ok()) {
      return WrapStatus(status, "SendFilesAndDirs() failed");
    }
  }

  return absl::OkStatus();
}

absl::Status FileFinderAndSender::SendFilesAndDirs() {
  if (request_.files_size() == 0 && request_.dirs_size() == 0) {
    return absl::OkStatus();
  }
  absl::Status status =
      message_pump_->SendMessage(PacketType::kAddFiles, request_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send AddFilesRequest");
  }

  request_.clear_files();
  request_.clear_dirs();
  request_size_ = request_.directory().length();
  return absl::OkStatus();
}

}  // namespace cdc_ft
