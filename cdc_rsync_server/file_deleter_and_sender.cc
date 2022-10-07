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

#include "cdc_rsync_server/file_deleter_and_sender.h"

#include "cdc_rsync/base/message_pump.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status.h"

namespace cdc_ft {

FileDeleterAndSender::FileDeleterAndSender(MessagePump* message_pump,
                                           size_t request_size_threshold)
    : message_pump_(message_pump),
      request_size_threshold_(request_size_threshold) {
  assert(message_pump_);
}

FileDeleterAndSender::~FileDeleterAndSender() = default;

absl::Status FileDeleterAndSender::DeleteAndSendFileOrDir(
    const std::string& base_dir, const std::string& relative_path, bool dry_run,
    bool is_directory) {
  std::string filepath = path::Join(base_dir, relative_path);
  if (!dry_run) {
    LOG_INFO("Removing %s", filepath.c_str());
    absl::Status status = path::RemoveFile(filepath);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to remove '%s'", filepath);
    }
  }

  std::string relative_dir = path::DirName(relative_path);
  if (!relative_dir.empty()) path::EnsureEndsWithPathSeparator(&relative_dir);
  if (response_.directory() != relative_dir) {
    // Flush files in previous directory.
    absl::Status status = SendFilesAndDirs();
    if (!status.ok()) {
      return WrapStatus(
          status, "Failed to send deleted files and directories to client");
    }

    // Set new directory.
    response_.set_directory(relative_dir);
    response_size_ = response_.directory().length();
  }

  std::string filename = path::BaseName(relative_path);
  if (is_directory) {
    *response_.add_dirs() = filename;
  } else {
    *response_.add_files() = filename;
  }
  response_size_ += filename.size();

  if (response_size_ >= request_size_threshold_) {
    absl::Status status = SendFilesAndDirs();
    if (!status.ok()) {
      return WrapStatus(
          status, "Failed to send deleted files and directories to client");
    }
  }

  return absl::OkStatus();
}

absl::Status FileDeleterAndSender::Flush() {
  absl::Status status = SendFilesAndDirs();
  if (!status.ok()) {
    return WrapStatus(status,
                      "Failed to send deleted files and directories to client");
  }

  // Send an empty batch as EOF indicator.
  assert(response_.files_size() == 0 && response_.dirs_size() == 0);
  status = message_pump_->SendMessage(PacketType::kAddDeletedFiles, response_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send EOF indicator");
  }

  return absl::OkStatus();
}

absl::Status FileDeleterAndSender::SendFilesAndDirs() {
  if (response_.files_size() == 0 && response_.dirs_size() == 0) {
    return absl::OkStatus();
  }

  absl::Status status =
      message_pump_->SendMessage(PacketType::kAddDeletedFiles, response_);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send AddDeletedFilesResponse");
  }
  response_.Clear();
  response_size_ = response_.directory().length();
  return absl::OkStatus();
}

}  // namespace cdc_ft
