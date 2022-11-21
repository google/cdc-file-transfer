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

#include <atomic>
#include <functional>
#include <unordered_set>

#include "common/semaphore.h"
#include "common/util.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Wrapper class to make it possible to pass lambdas as task functions.
class TestTask : public Task {
 public:
  using TaskFunc = std::function<void(IsCancelledPredicate is_cancelled)>;

  explicit TestTask(TaskFunc task_func) : task_func_(task_func) {}

  virtual void ThreadRun(IsCancelledPredicate is_cancelled) {
    task_func_(is_cancelled);
  }

 private:
  TaskFunc task_func_;
};

class ThreadpoolTest : public ::testing::Test {};

TEST_F(ThreadpoolTest, WaitShutdownWorkWithoutTasks) {
  Threadpool pool(3);
  pool.Wait();
  pool.Shutdown();
}

TEST_F(ThreadpoolTest, SingleThreadedRunsToCompletion) {
  std::atomic_bool task_finished{false};
  auto task_func = [&task_finished](Task::IsCancelledPredicate) {
    task_finished = true;
  };

  Threadpool pool(1);
  std::unique_ptr<Task> task_ptr = std::make_unique<TestTask>(task_func);
  Task* task = task_ptr.get();
  pool.QueueTask(std::move(task_ptr));
  pool.Wait();

  EXPECT_TRUE(task_finished);

  std::unique_ptr<Task> completed_task = pool.TryGetCompletedTask();
  EXPECT_EQ(completed_task.get(), task);
}

TEST_F(ThreadpoolTest, MultiThreadedRunsToCompletion) {
  const int num_tasks = 19;
  const int num_threads = 7;
  std::atomic_int num_completed{0};

  Threadpool pool(num_threads);
  std::unordered_set<Task*> tasks;
  for (int n = 0; n < num_tasks; ++n) {
    auto task_func = [&num_completed](Task::IsCancelledPredicate) {
      ++num_completed;
    };
    auto task = std::make_unique<TestTask>(task_func);
    tasks.insert(task.get());
    pool.QueueTask(std::move(task));
  }
  pool.Wait();

  EXPECT_EQ(num_completed, num_tasks);
  for (int n = 0; n < num_tasks; ++n) {
    std::unique_ptr<Task> completed_task = pool.TryGetCompletedTask();
    EXPECT_TRUE(completed_task);
    tasks.erase(completed_task.get());
  }
  EXPECT_FALSE(pool.TryGetCompletedTask());
  EXPECT_TRUE(tasks.empty());
}

TEST_F(ThreadpoolTest, TaskIsCancelledOnShutdown) {
  Semaphore task_started(0);
  std::atomic_bool task_finished{false};
  auto task_func = [&task_started,
                    &task_finished](Task::IsCancelledPredicate is_cancelled) {
    task_started.Signal();
    while (!is_cancelled()) {
      Util::Sleep(0);
    }
    task_finished = true;
  };

  Threadpool pool(1);
  pool.QueueTask(std::make_unique<TestTask>(task_func));
  task_started.Wait();
  pool.Shutdown();

  EXPECT_TRUE(task_finished);

  // The cancelled task should be discarded.
  std::unique_ptr<Task> completed_task = pool.TryGetCompletedTask();
  EXPECT_FALSE(completed_task.get());
}

TEST_F(ThreadpoolTest, SingleThreadedCompletesInFifoOrder) {
  const int num_tasks = 10;
  std::vector<int> completed_order;

  Threadpool pool(1);
  for (int n = 0; n < num_tasks; ++n) {
    auto task_func = [n, &completed_order](Task::IsCancelledPredicate) {
      // Thread-safe because there's only one worker thread.
      completed_order.push_back(n);
    };
    pool.QueueTask(std::make_unique<TestTask>(task_func));
  }
  pool.Wait();

  std::unique_ptr<Task> completed_task = pool.TryGetCompletedTask();
  ASSERT_EQ(completed_order.size(), num_tasks);
  for (int n = 0; n < num_tasks; ++n) {
    EXPECT_EQ(completed_order[n], n);
  }
}

TEST_F(ThreadpoolTest, GetCompletedTask) {
  auto task_func = [](Task::IsCancelledPredicate) { Util::Sleep(10); };

  Threadpool pool(1);
  std::unique_ptr<Task> task_ptr = std::make_unique<TestTask>(task_func);
  Task* task = task_ptr.get();
  pool.QueueTask(std::move(task_ptr));
  // Note: No pool.Wait().

  std::unique_ptr<Task> completed_task = pool.GetCompletedTask();
  EXPECT_EQ(completed_task.get(), task);
}

}  // namespace
}  // namespace cdc_ft
