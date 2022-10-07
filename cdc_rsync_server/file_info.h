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

#ifndef CDC_RSYNC_SERVER_FILE_INFO_H_
#define CDC_RSYNC_SERVER_FILE_INFO_H_

#include <string>

namespace cdc_ft {

struct FileInfo {
  static constexpr uint32_t kInvalidIndex = UINT32_MAX;

  // Path relative to |base_dir|.
  std::string filepath;
  int64_t modified_time;
  uint64_t size;
  // For client files: Index into the client file list.
  uint32_t client_index;
  // For server files: Base directory. If nullptr, |filepath| is assumed to be
  // relative to the destination directory.
  const char* base_dir;

  FileInfo(std::string filepath, int64_t modified_time, uint64_t size,
           uint32_t client_index, const char* base_dir)
      : filepath(std::move(filepath)),
        modified_time(modified_time),
        size(size),
        client_index(client_index),
        base_dir(base_dir) {}
};

struct DirInfo {
  static constexpr uint32_t kInvalidIndex = UINT32_MAX;

  // Path relative to |base_dir|.
  std::string filepath;
  uint32_t client_index;
  // For server files: Base directory. If nullptr, |filepath| is assumed to be
  // relative to the destination directory.
  const char* base_dir;

  DirInfo(std::string filepath, uint32_t client_index, const char* base_dir)
      : filepath(std::move(filepath)),
        client_index(client_index),
        base_dir(base_dir) {}
};

// Similar to FileInfo, but size is needed from both client and server.
struct ChangedFileInfo {
  std::string filepath;
  int64_t client_modified_time;
  uint64_t client_size;
  uint64_t server_size;
  uint32_t client_index;
  const char* base_dir;

  // Moves |client_file| data into this class.
  ChangedFileInfo(const FileInfo& server_file, FileInfo&& client_file)
      : filepath(std::move(client_file.filepath)),
        client_modified_time(client_file.modified_time),
        client_size(client_file.size),
        server_size(server_file.size),
        client_index(client_file.client_index),
        base_dir(server_file.base_dir) {}
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_FILE_INFO_H_
