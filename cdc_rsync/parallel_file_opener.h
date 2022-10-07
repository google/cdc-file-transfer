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

#ifndef CDC_RSYNC_PARALLEL_FILE_OPENER_H_
#define CDC_RSYNC_PARALLEL_FILE_OPENER_H_

#include <map>
#include <memory>
#include <vector>

#include "cdc_rsync/client_file_info.h"
#include "common/threadpool.h"

namespace cdc_ft {

namespace internal {
class FileOpenTask;
}

// Opens files on a background worker thread pool. This improves performance in
// cases where many files have to be opened quickly.
class ParallelFileOpener {
 public:
  // Starts opening the |files| indexed by |file_indices| in the background.
  ParallelFileOpener(const std::vector<ClientFileInfo>* files,
                     const std::vector<uint32_t>& file_indices);

  ~ParallelFileOpener();

  // Returns FILE* pointer from opening a file from |files| in rb-mode.
  // The first call returns the FILE* pointer for files[file_indices[0]],
  // the second call returns the FILE* pointer for files[file_indices[1]] etc.
  // The caller must close the file.
  FILE* GetNextOpenFile();

 private:
  // Queues another open file task.
  void QueueNextFile();

  // Pointer to list of files, not owned.
  const std::vector<ClientFileInfo>* files_;

  // Indices into the |files_| to open.
  std::vector<uint32_t> file_indices_;

  // Index into |file_indices_| of the next file returned by GetNextOpenFile().
  size_t curr_index_ = 0;

  // Index into |file_indices_| of the file queued by QueueNextFile().
  size_t look_ahead_index_ = 0;

  // Maps index into |file_indices_| to completed task.
  std::map<size_t, std::unique_ptr<internal::FileOpenTask>>
      index_to_completed_tasks_;

  Threadpool pool_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_PARALLEL_FILE_OPENER_H_
