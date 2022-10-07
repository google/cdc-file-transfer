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

#ifndef CDC_RSYNC_SERVER_FILE_DELETER_AND_SENDER_H_
#define CDC_RSYNC_SERVER_FILE_DELETER_AND_SENDER_H_

#include <string>

#include "absl/status/status.h"
#include "cdc_rsync/protos/messages.pb.h"

namespace cdc_ft {

class MessagePump;

// Deletes files and sends info about deleted files to the client.
class FileDeleterAndSender {
 public:
  // Send AddDeletedFileResponse in packets of roughly 10k max by default.
  static constexpr size_t kDefaultResponseSizeThreshold = 10000;

  FileDeleterAndSender(
      MessagePump* message_pump,
      size_t response_size_threshold = kDefaultResponseSizeThreshold);
  ~FileDeleterAndSender();

  // Deletes |base_dir| + |relative_path| and send |relative_path| the client.
  // Deletion happens for either a directory or a file and only in a non dry-run
  // mode.
  absl::Status DeleteAndSendFileOrDir(const std::string& base_dir,
                                      const std::string& relative_path,
                                      bool dry_run, bool is_directory);

  // Sends the remaining file and directory batch to the client, followed by an
  // EOF indicator. Should be called once all files and directories have been
  // deleted.
  absl::Status Flush();

 private:
  // Sends the current batch to the client.
  absl::Status SendFilesAndDirs();

  MessagePump* const message_pump_;
  const size_t request_size_threshold_;

  AddDeletedFilesResponse response_;
  size_t response_size_ = 0;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_FILE_DELETER_AND_SENDER_H_
