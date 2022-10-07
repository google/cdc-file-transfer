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

#include "cdc_rsync/parallel_file_opener.h"

#include "absl/status/statusor.h"
#include "common/path.h"

namespace cdc_ft {
namespace {

// Number of threads in the pool.
size_t GetPoolSize() {
  uint32_t num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) return 4;
  return num_threads;
}

// Number of file open operations to queue in advance.
const size_t kNumQueuedTasks = 256;

}  // namespace

namespace internal {

class FileOpenTask : public Task {
 public:
  FileOpenTask(size_t index, ClientFileInfo file)
      : index_(index), file_(file) {}

  ~FileOpenTask() {
    if (*fp_) {
      fclose(*fp_);
      *fp_ = nullptr;
    }
  }

  FileOpenTask(const FileOpenTask& other) = delete;
  FileOpenTask(const FileOpenTask&& other) = delete;

  FileOpenTask& operator=(FileOpenTask&) = delete;
  FileOpenTask& operator=(FileOpenTask&&) = delete;

  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    fp_ = path::OpenFile(file_.path, "rb");
  }

  size_t Index() const { return index_; }

  FILE* ReleaseFile() {
    FILE* fp = *fp_;
    *fp_ = nullptr;
    return fp;
  }

 private:
  size_t index_;
  ClientFileInfo file_;
  absl::StatusOr<FILE*> fp_ = nullptr;
};

}  // namespace internal

ParallelFileOpener::ParallelFileOpener(
    const std::vector<ClientFileInfo>* files,
    const std::vector<uint32_t>& file_indices)
    : files_(files), file_indices_(file_indices), pool_(GetPoolSize()) {
  // Queue the first |kNumQueuedTasks| files (if available).
  size_t num_to_queue = std::min(kNumQueuedTasks, file_indices_.size());
  for (size_t n = 0; n < num_to_queue; ++n) {
    QueueNextFile();
  }
}

ParallelFileOpener::~ParallelFileOpener() = default;

FILE* ParallelFileOpener::GetNextOpenFile() {
  if (curr_index_ >= file_indices_.size()) {
    return nullptr;
  }

  QueueNextFile();

  // Wait until the file at |curr_index_| is available.
  // Note that |index_to_completed_tasks_| is sorted by index.
  while (index_to_completed_tasks_.empty() ||
         index_to_completed_tasks_.begin()->first != curr_index_) {
    std::unique_ptr<Task> task = pool_.GetCompletedTask();
    auto* fopen_task = static_cast<internal::FileOpenTask*>(task.release());
    index_to_completed_tasks_[fopen_task->Index()].reset(fopen_task);
  }

  // The first completed task should be the one for |curr_index_|.
  const auto& first_iter = index_to_completed_tasks_.begin();
  FILE* file = first_iter->second->ReleaseFile();
  index_to_completed_tasks_.erase(first_iter);
  curr_index_++;
  return file;
}

void ParallelFileOpener::QueueNextFile() {
  if (look_ahead_index_ >= file_indices_.size()) {
    return;
  }

  pool_.QueueTask(std::make_unique<internal::FileOpenTask>(
      look_ahead_index_, files_->at(file_indices_[look_ahead_index_])));
  ++look_ahead_index_;
}

}  // namespace cdc_ft
