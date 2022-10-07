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

#include "cdc_rsync_server/file_diff_generator.h"

#include <algorithm>

#include "common/log.h"
#include "common/path.h"
#include "common/util.h"

namespace cdc_ft {
namespace file_diff {

namespace {

struct FilePathComparer {
  bool operator()(const FileInfo& a, const FileInfo& b) {
    return a.filepath < b.filepath;
  }
};

struct FilePathEquals {
  bool operator()(const FileInfo& a, const FileInfo& b) {
    return a.filepath == b.filepath;
  }
};

struct DirPathComparer {
  bool operator()(const DirInfo& a, const DirInfo& b) {
    return a.filepath < b.filepath;
  }
};

struct DirPathEquals {
  bool operator()(const DirInfo& a, const DirInfo& b) {
    return a.filepath == b.filepath;
  }
};

bool FindFile(const std::string& base_dir, const std::string& relative_path,
              FileInfo* file) {
  path::Stats stats;
  if (!path::GetStats(path::Join(base_dir, relative_path), &stats).ok()) {
    return false;
  }

  *file = FileInfo(relative_path, stats.modified_time, stats.size,
                   FileInfo::kInvalidIndex, base_dir.c_str());
  return true;
}

// Returns true if |client_file| and |server_file| are considered a match based
// on filesize and timestamp. An exception is if the |server_file| is in the
// |copy_dest| directory (e.g. the package dir). In that case, the file should
// be considered as changed (the sync algo will copy the file over).
bool FilesMatch(const FileInfo& client_file, const FileInfo& server_file,
                const std::string& copy_dest) {
  return client_file.size == server_file.size &&
         client_file.modified_time == server_file.modified_time &&
         (copy_dest.empty() || server_file.base_dir != copy_dest.c_str());
}

}  // namespace

Result Generate(std::vector<FileInfo>&& client_files,
                std::vector<FileInfo>&& server_files,
                std::vector<DirInfo>&& client_dirs,
                std::vector<DirInfo>&& server_dirs, const std::string& base_dir,
                const std::string& copy_dest, bool double_check_missing) {
  // Sort both arrays by filepath.
  std::sort(client_files.begin(), client_files.end(), FilePathComparer());
  std::sort(server_files.begin(), server_files.end(), FilePathComparer());
  std::sort(client_dirs.begin(), client_dirs.end(), DirPathComparer());
  std::sort(server_dirs.begin(), server_dirs.end(), DirPathComparer());

  // De-dupe client files, just in case. This might happen if someone calls
  // cdc_rsync with overlapping sources, e.g. assets/* and assets/textures/*.
  client_files.erase(
      std::unique(client_files.begin(), client_files.end(), FilePathEquals()),
      client_files.end());

  client_dirs.erase(
      std::unique(client_dirs.begin(), client_dirs.end(), DirPathEquals()),
      client_dirs.end());

  // Compare the arrays, sorting the files into the right buckets.
  std::vector<FileInfo>::iterator client_iter = client_files.begin();
  std::vector<FileInfo>::iterator server_iter = server_files.begin();

  Result diff;

  while (client_iter != client_files.end() ||
         server_iter != server_files.end()) {
    const int order =
        client_iter == client_files.end() ? 1  // Extraneous.
        : server_iter == server_files.end()
            ? -1  // Missing.
            : client_iter->filepath.compare(server_iter->filepath);

    if (order < 0) {
      // File is missing, but first double check if it's really missing if
      // |double_check_missing| is true.
      FileInfo server_file(std::string(), 0, 0, FileInfo::kInvalidIndex,
                           nullptr);
      bool found = false;
      if (double_check_missing) {
        found = FindFile(base_dir, client_iter->filepath, &server_file);
        if (!found && !copy_dest.empty()) {
          found = FindFile(copy_dest, client_iter->filepath, &server_file);
        }
      }

      if (!found) {
        diff.missing_files.push_back(std::move(*client_iter));
      } else if (FilesMatch(*client_iter, server_file, copy_dest)) {
        diff.matching_files.push_back(std::move(*client_iter));
      } else {
        diff.changed_files.emplace_back(server_file, std::move(*client_iter));
      }
      ++client_iter;
    } else if (order > 0) {
      diff.extraneous_files.push_back(std::move(*server_iter));
      ++server_iter;
    } else if (FilesMatch(*client_iter, *server_iter, copy_dest)) {
      diff.matching_files.push_back(std::move(*client_iter));
      ++client_iter;
      ++server_iter;
    } else {
      diff.changed_files.emplace_back(*server_iter, std::move(*client_iter));
      ++client_iter;
      ++server_iter;
    }
  }

  // Compare the arrays, sorting the dirs into the right buckets.
  std::vector<DirInfo>::iterator client_dir_iter = client_dirs.begin();
  std::vector<DirInfo>::iterator server_dir_iter = server_dirs.begin();

  while (client_dir_iter != client_dirs.end() ||
         server_dir_iter != server_dirs.end()) {
    const int order =
        client_dir_iter == client_dirs.end() ? 1  // Extraneous.
        : server_dir_iter == server_dirs.end()
            ? -1  // Missing.
            : client_dir_iter->filepath.compare(server_dir_iter->filepath);

    if (order < 0) {
      diff.missing_dirs.push_back(std::move(*client_dir_iter));
      ++client_dir_iter;
    } else if (order > 0) {
      diff.extraneous_dirs.push_back(std::move(*server_dir_iter));
      ++server_dir_iter;
    } else {
      // Matching dirs in the copy_dest directory need to be created in the
      // destination.
      if (!copy_dest.empty() && server_dir_iter->base_dir == copy_dest.c_str())
        diff.missing_dirs.push_back(std::move(*client_dir_iter));
      else
        diff.matching_dirs.push_back(std::move(*client_dir_iter));
      ++client_dir_iter;
      ++server_dir_iter;
    }
  }

  // Remove all extraneous files and dirs from the |copy_dest| directory.
  // Those should not be deleted with the --delete option.
  if (!copy_dest.empty()) {
    diff.extraneous_files.erase(
        std::remove_if(diff.extraneous_files.begin(),
                       diff.extraneous_files.end(),
                       [&copy_dest](const FileInfo& dir) {
                         return copy_dest.c_str() == dir.base_dir;
                       }),
        diff.extraneous_files.end());

    diff.extraneous_dirs.erase(
        std::remove_if(diff.extraneous_dirs.begin(), diff.extraneous_dirs.end(),
                       [&copy_dest](const DirInfo& dir) {
                         return copy_dest.c_str() == dir.base_dir;
                       }),
        diff.extraneous_dirs.end());
  }

  // Release memory.
  std::vector<FileInfo> empty_client_files;
  std::vector<FileInfo> empty_server_files;
  client_files.swap(empty_client_files);
  server_files.swap(empty_server_files);
  std::vector<DirInfo> empty_client_dirs;
  std::vector<DirInfo> empty_server_dirs;
  client_dirs.swap(empty_client_dirs);
  server_dirs.swap(empty_server_dirs);

  return diff;
}

SendFileStatsResponse AdjustToFlagsAndGetStats(bool existing, bool checksum,
                                               bool whole_file, Result* diff) {
  // Record stats.
  SendFileStatsResponse file_stats_response;
  file_stats_response.set_num_missing_files(
      static_cast<uint32_t>(diff->missing_files.size()));
  file_stats_response.set_num_extraneous_files(
      static_cast<uint32_t>(diff->extraneous_files.size()));
  file_stats_response.set_num_matching_files(
      static_cast<uint32_t>(diff->matching_files.size()));
  file_stats_response.set_num_changed_files(
      static_cast<uint32_t>(diff->changed_files.size()));
  file_stats_response.set_num_missing_dirs(
      static_cast<uint32_t>(diff->missing_dirs.size()));
  file_stats_response.set_num_extraneous_dirs(
      static_cast<uint32_t>(diff->extraneous_dirs.size()));
  file_stats_response.set_num_matching_dirs(
      static_cast<uint32_t>(diff->matching_dirs.size()));

  // Take special flags into account that move files between the lists.

  if (existing) {
    // Do not copy missing files.
    LOG_INFO("Removing missing files (--existing)");
    std::vector<FileInfo> empty_files;
    diff->missing_files.swap(empty_files);
    std::vector<DirInfo> empty_dirs;
    diff->missing_dirs.swap(empty_dirs);
  }

  if (checksum) {
    // Move matching files over to changed files, so the delta-transfer
    // algorithm is applied.
    LOG_INFO("Moving matching files over to changed files (-c/--checksum)");
    for (FileInfo& file : diff->matching_files)
      diff->changed_files.emplace_back(file, std::move(file));
    std::vector<FileInfo> empty;
    diff->matching_files.swap(empty);
  }

  if (whole_file) {
    // Move changed files over to the missing files, so they all get copied.
    LOG_INFO("Moving changed files over to missing files (-W/--whole)");
    for (ChangedFileInfo& file : diff->changed_files) {
      diff->missing_files.emplace_back(
          std::move(file.filepath), file.client_modified_time, file.client_size,
          file.client_index, nullptr);
    }
    std::vector<ChangedFileInfo> empty;
    diff->changed_files.swap(empty);
  }

  // Compute totals.
  uint64_t total_missing_bytes = 0;
  for (const FileInfo& file : diff->missing_files)
    total_missing_bytes += file.size;

  uint64_t total_changed_client_bytes = 0;
  uint64_t total_changed_server_bytes = 0;
  for (const ChangedFileInfo& file : diff->changed_files) {
    total_changed_client_bytes += file.client_size;
    total_changed_server_bytes += file.server_size;
  }

  // Set totals in stats. Note that the totals are computed from the MODIFIED
  // file lists. This is important to get progress reporting right. The other
  // stats, OTOH, are computed from the ORIGINAL file lists. They're displayed
  // to the user.
  file_stats_response.set_total_missing_bytes(total_missing_bytes);
  file_stats_response.set_total_changed_client_bytes(
      total_changed_client_bytes);
  file_stats_response.set_total_changed_server_bytes(
      total_changed_server_bytes);

  return file_stats_response;
}

}  // namespace file_diff
}  // namespace cdc_ft
