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

#ifndef CDC_RSYNC_SERVER_FILE_DIFF_GENERATOR_H_
#define CDC_RSYNC_SERVER_FILE_DIFF_GENERATOR_H_

#include <vector>

#include "cdc_rsync/protos/messages.pb.h"
#include "cdc_rsync_server/file_info.h"

namespace cdc_ft {
namespace file_diff {

struct Result {
  // Files present on the client, but not on the server.
  std::vector<FileInfo> missing_files;

  // Files present on the server, but not on the client.
  std::vector<FileInfo> extraneous_files;

  // Files present on both, with different timestamp or file size.
  std::vector<ChangedFileInfo> changed_files;

  // Files present on both, with matching timestamp and file size.
  std::vector<FileInfo> matching_files;

  // Directories present on the client, but not on the server.
  std::vector<DirInfo> missing_dirs;

  // Directories present on the server, but not on the client.
  std::vector<DirInfo> extraneous_dirs;

  // Directories present on both client and server.
  std::vector<DirInfo> matching_dirs;
};

// Generates the diff between
// 1) |client_files| and |server_files| by comparing
// file paths, modified times and file sizes.
// If |double_check_missing| is true, missing files are checked for existence
// (relative to |base_dir| and |copy_dest|, if non-empty)
// before they are put into the missing bucket.
// 2) |client_dirs| and |server_dirs| by comparing directory paths.
// The passed-in vectors are consumed.
Result Generate(std::vector<FileInfo>&& client_files,
                std::vector<FileInfo>&& server_files,
                std::vector<DirInfo>&& client_dirs,
                std::vector<DirInfo>&& server_dirs, const std::string& base_dir,
                const std::string& copy_dest, bool double_check_missing);

// Adjusts file containers according to sync flags.
// |existing|, |checksum|, |whole_file| are the sync flags, see command line
// help. They cause files to be moved between containers.
// |diff| is the result from Generate().
SendFileStatsResponse AdjustToFlagsAndGetStats(bool existing, bool checksum,
                                               bool whole_file, Result* diff);

}  // namespace file_diff
}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_FILE_DIFF_GENERATOR_H_
