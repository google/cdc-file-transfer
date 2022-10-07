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

#ifndef CDC_RSYNC_FILE_FINDER_AND_SENDER_H_
#define CDC_RSYNC_FILE_FINDER_AND_SENDER_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "cdc_rsync/client_file_info.h"
#include "cdc_rsync/protos/messages.pb.h"

namespace cdc_ft {

class MessagePump;
class PathFilter;

class ReportFindFilesProgress {
 public:
  virtual ~ReportFindFilesProgress() = default;
  virtual void ReportFileFound() = 0;
  virtual void ReportDirFound() = 0;
};

class FileFinderAndSender {
 public:
  // Send AddFileRequests in packets of roughly 10k max by default.
  static constexpr size_t kDefaultRequestSizeThreshold = 10000;

  FileFinderAndSender(
      PathFilter* path_filter, MessagePump* message_pump,
      ReportFindFilesProgress* progress_, std::string sources_dir,
      bool recursive, bool relative,
      size_t request_byte_threshold = kDefaultRequestSizeThreshold);
  ~FileFinderAndSender();

  absl::Status FindAndSendFiles(std::string source);

  // Sends the remaining file batch to the client, followed by an EOF indicator.
  // Should be called once all files have been deleted.
  absl::Status Flush();

  void ReleaseFiles(std::vector<ClientFileInfo>* files);
  void ReleaseDirs(std::vector<ClientDirInfo>* dirs);

 private:
  absl::Status HandleFoundFileOrDir(std::string dir, std::string filename,
                                    int64_t modified_time, uint64_t size,
                                    bool is_directory);

  // Sends the current file and directory batch to the server.
  absl::Status SendFilesAndDirs();

  PathFilter* const path_filter_;
  MessagePump* const message_pump_;
  ReportFindFilesProgress* const progress_;
  std::string sources_dir_;
  const bool recursive_;
  const bool relative_;
  const size_t request_size_threshold_;

  // Prefix removed from found files before they are sent to the server.
  std::string base_dir_;
  AddFilesRequest request_;
  size_t request_size_ = 0;

  // Found files.
  std::vector<ClientFileInfo> files_;

  // Found directories.
  std::vector<ClientDirInfo> dirs_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_FILE_FINDER_AND_SENDER_H_
