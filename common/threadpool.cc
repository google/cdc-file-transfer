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

#include "common/threadpool.h"

namespace cdc_ft {

Threadpool::Threadpool(size_t num_threads) : shutdown_{false} {
  workers_.reserve(num_threads);
  for (size_t n = 0; n < num_threads; ++n) {
    workers_.emplace_back([this]() { ThreadWorkerMain(); });
  }
}

Threadpool::~Threadpool() { Shutdown(); }

void Threadpool::Wait() {
  absl::MutexLock lock(&task_queue_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(task_queue_mutex_) {
    return outstanding_task_count_ == 0;
  };
  task_queue_mutex_.Await(absl::Condition(&cond));
}

void Threadpool::Shutdown() {
  {
    // Signal shutdown.
    absl::MutexLock lock(&task_queue_mutex_);
    if (shutdown_) return;
    shutdown_ = true;
  }

  // Join thread. This makes sure that the last task finishes.
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }

  // Discard all completed tasks.
  absl::MutexLock lock(&completed_tasks_mutex_);
  std::queue<std::unique_ptr<Task>> empty;
  std::swap(completed_tasks_, empty);
}

void Threadpool::QueueTask(std::unique_ptr<Task> task) {
  absl::MutexLock lock(&task_queue_mutex_);
  ++outstanding_task_count_;
  task_queue_.push(std::move(task));
}

std::unique_ptr<Task> Threadpool::TryGetCompletedTask() {
  absl::MutexLock lock(&completed_tasks_mutex_);

  if (completed_tasks_.empty()) {
    return std::unique_ptr<Task>();
  }

  std::unique_ptr<Task> task = std::move(completed_tasks_.front());
  completed_tasks_.pop();
  return task;
}

std::unique_ptr<Task> Threadpool::GetCompletedTask() {
  absl::MutexLock lock(&completed_tasks_mutex_);
  auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(completed_tasks_mutex_) {
    return !completed_tasks_.empty();
  };
  completed_tasks_mutex_.Await(absl::Condition(&cond));

  std::unique_ptr<Task> task = std::move(completed_tasks_.front());
  completed_tasks_.pop();
  return task;
}

void Threadpool::SetTaskCompletedCallback(TaskCompletedCallback cb) {
  absl::MutexLock lock(&completed_tasks_mutex_);
  on_task_completed_ = std::move(cb);
}

void Threadpool::WaitForQueuedTasksAtMost(size_t count) const {
  absl::MutexLock lock(&task_queue_mutex_);
  auto cond = [this, count]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(task_queue_mutex_) {
    return shutdown_ || outstanding_task_count_ <= count;
  };
  task_queue_mutex_.Await(absl::Condition(&cond));
}

void Threadpool::ThreadWorkerMain() {
  bool task_finished = false;
  for (;;) {
    std::unique_ptr<Task> task;
    {
      absl::MutexLock lock(&task_queue_mutex_);

      // Decrease task count here, so we don't have to lock again at the end of
      // the loop. It is important to first push the task, then decrease this
      // count. Otherwise, there's a race between Wait() and GetCompletedTask().
      if (task_finished) {
        assert(outstanding_task_count_ > 0);
        --outstanding_task_count_;
      }

      // Wait for task to be available (or shutdown).
      auto cond = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(task_queue_mutex_) {
        return shutdown_ || !task_queue_.empty();
      };
      task_queue_mutex_.Await(absl::Condition(&cond));
      if (shutdown_) break;

      // Grab task from queue.
      task = std::move(task_queue_.front());
      task_queue_.pop();
    }

    // Run task, but make it cancellable.
    task->ThreadRun([this]() ABSL_LOCKS_EXCLUDED(task_queue_mutex_) -> bool {
      absl::MutexLock lock(&task_queue_mutex_);
      return shutdown_;
    });

    // Push task to completed queue.
    absl::MutexLock lock(&completed_tasks_mutex_);
    if (on_task_completed_) {
      on_task_completed_(std::move(task));
    } else {
      completed_tasks_.push(std::move(task));
    }
    task_finished = true;
  }
}

}  // namespace cdc_ft
