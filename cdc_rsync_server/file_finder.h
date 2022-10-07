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

#ifndef CDC_RSYNC_SERVER_FILE_FINDER_H_
#define CDC_RSYNC_SERVER_FILE_FINDER_H_

#include <vector>

#include "absl/status/status.h"
#include "cdc_rsync_server/file_info.h"

namespace cdc_ft {

class PathFilter;

// Scans directories and gathers contained files and directories.
class FileFinder {
 public:
  FileFinder();
  ~FileFinder();

  // Gathers files and directories in |base_dir|. If |recursive| is true,
  // searches recursively. |path_filter| is used to filter files and
  // directories.
  // If subsequent calls to AddFiles find a file or directory with the same
  // relative path, this file or directory is ignored.
  absl::Status AddFiles(const std::string& base_dir, bool recursive,
                        PathFilter* path_filter);

  // Returns all found files and directories.
  void ReleaseFiles(std::vector<FileInfo>* files, std::vector<DirInfo>* dirs);

 private:
  std::vector<FileInfo> files_;
  std::vector<DirInfo> dirs_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_FILE_FINDER_H_
