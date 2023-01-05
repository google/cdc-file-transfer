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

#ifndef COMMON_THREADPOOL_H_
#define COMMON_THREADPOOL_H_

#include <atomic>
#include <functional>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"

namespace cdc_ft {

class Task {
 public:
  using IsCancelledPredicate = std::function<bool()>;

  virtual ~Task() = default;

  // Method that's doing all the work.
  // Called on a background thread.
  virtual void ThreadRun(IsCancelledPredicate is_cancelled) = 0;
};

// Manages a pool of worker threads and schedules tasks to run on the threads.
class Threadpool {
 public:
  // Creates a new thread pool with |num_threads| worker threads.
  explicit Threadpool(size_t num_threads);
  ~Threadpool();

  // Waits for all queued tasks to finish.
  void Wait() ABSL_LOCKS_EXCLUDED(task_queue_mutex_);

  // Stops the worker threads. Cancels currently active tasks.
  void Shutdown() ABSL_LOCKS_EXCLUDED(task_queue_mutex_);

  // Queues a task for execution in a worker thread.
  void QueueTask(std::unique_ptr<Task> task)
      ABSL_LOCKS_EXCLUDED(task_queue_mutex_);

  // Returns the next completed task if available or nullptr all are either
  // queued or in progress.
  // For a single worker thread (|num_threads| == 1), tasks are completed in
  // FIFO order. This is no longer the case for multiple threads
  // (|num_threads| > 1). Tasks that got queued later might complete first.
  std::unique_ptr<Task> TryGetCompletedTask()
      ABSL_LOCKS_EXCLUDED(completed_tasks_mutex_);

  // Returns the next completed task, possibly blocking until it is available.
  // For a single worker thread (|num_threads| == 1), tasks are completed in
  // FIFO order. This is no longer the case for multiple threads
  // (|num_threads| > 1). Tasks that got queued later might complete first.
  std::unique_ptr<Task> GetCompletedTask()
      ABSL_LOCKS_EXCLUDED(completed_tasks_mutex_);

  using TaskCompletedCallback = std::function<void(std::unique_ptr<Task>)>;

  // Set a callback that is called immediately in a background thread when a
  // task is completed. The task will not be put onto the completed queue, so
  // if this callback is set, do not call (Try)GetCompletedTask.
  void SetTaskCompletedCallback(TaskCompletedCallback cb)
      ABSL_LOCKS_EXCLUDED(completed_tasks_mutex_);

  // Returns the total number of worker threads in the pool.
  size_t NumThreads() const { return workers_.size(); }

  // Returns the number of tasks that are either queued or in progress.
  size_t NumQueuedTasks() const ABSL_LOCKS_EXCLUDED(task_queue_mutex_) {
    absl::ReaderMutexLock lock(&task_queue_mutex_);
    return outstanding_task_count_;
  }

  // Block until the number of queued tasks drops below |count|.
  void WaitForQueuedTasksAtMost(size_t count) const ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  // Background thread worker method. Picks tasks and runs them.
  void ThreadWorkerMain()
      ABSL_LOCKS_EXCLUDED(task_queue_mutex_, completed_tasks_mutex_);

  mutable absl::Mutex task_queue_mutex_;
  std::queue<std::unique_ptr<Task>> task_queue_
      ABSL_GUARDED_BY(task_queue_mutex_);
  size_t outstanding_task_count_ ABSL_GUARDED_BY(task_queue_mutex_) = 0;
  std::atomic_bool shutdown_ ABSL_GUARDED_BY(task_queue_mutex_);

  absl::Mutex completed_tasks_mutex_;
  std::queue<std::unique_ptr<Task>> completed_tasks_
      ABSL_GUARDED_BY(completed_tasks_mutex_);
  TaskCompletedCallback on_task_completed_
      ABSL_GUARDED_BY(completed_tasks_mutex_);

  std::vector<std::thread> workers_;
};

}  // namespace cdc_ft

#endif  // COMMON_THREADPOOL_H_
