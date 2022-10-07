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

#include "cdc_rsync_server/file_finder.h"

#include "common/path.h"
#include "common/path_filter.h"
#include "common/platform.h"
#include "common/status.h"

namespace cdc_ft {

FileFinder::FileFinder() {}

absl::Status FileFinder::AddFiles(const std::string& base_dir, bool recursive,
                                  PathFilter* path_filter) {
  std::vector<FileInfo>* files = &files_;
  std::vector<DirInfo>* dirs = &dirs_;
  auto handler = [files, dirs, &base_dir, path_filter](
                     std::string dir, std::string filename,
                     int64_t modified_time, uint64_t size, bool is_directory) {
    std::string path = path::Join(dir.substr(base_dir.size()), filename);
    if (path_filter->IsMatch(path)) {
      if (is_directory) {
        dirs->emplace_back(path, FileInfo::kInvalidIndex, base_dir.c_str());
      } else {
        files->emplace_back(path, modified_time, size, FileInfo::kInvalidIndex,
                            base_dir.c_str());
      }
    }
    return absl::OkStatus();
  };

#if PLATFORM_WINDOWS
  // SearchFiles needs a wildcard on Windows. Currently only used for tests.
  absl::Status status =
      path::SearchFiles(path::Join(base_dir, "*"), recursive, handler);
#elif PLATFORM_LINUX
  absl::Status status = path::SearchFiles(base_dir, recursive, handler);
#endif
  if (!status.ok()) {
    return WrapStatus(status, "Failed to find files for '%s'", base_dir);
  }

  return absl::OkStatus();
}

void FileFinder::ReleaseFiles(std::vector<FileInfo>* files,
                              std::vector<DirInfo>* dirs) {
  assert(files);
  assert(dirs);

  // Dedupe files and directories. Note that the combination of std::stable_sort
  // and std::unique is guaranteed to keep the entries added first to the lists.
  // In practice, this kicks out the element in the copy_dest directory (e.g.
  // the package) and keeps the one in the destination (e.g. /mnt/developer), if
  // both are present.
  std::stable_sort(files_.begin(), files_.end(),
                   [](const FileInfo& a, const FileInfo& b) {
                     return a.filepath < b.filepath;
                   });
  std::stable_sort(dirs_.begin(), dirs_.end(),
                   [](const DirInfo& a, const DirInfo& b) {
                     return a.filepath < b.filepath;
                   });

  files_.erase(std::unique(files_.begin(), files_.end(),
                           [](const FileInfo& a, const FileInfo& b) {
                             return a.filepath == b.filepath;
                           }),
               files_.end());

  dirs_.erase(std::unique(dirs_.begin(), dirs_.end(),
                          [](const DirInfo& a, const DirInfo& b) {
                            return a.filepath == b.filepath;
                          }),
              dirs_.end());

  *files = std::move(files_);
  *dirs = std::move(dirs_);
}

FileFinder::~FileFinder() = default;

}  // namespace cdc_ft
