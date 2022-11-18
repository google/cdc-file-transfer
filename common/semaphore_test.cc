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

#include "common/semaphore.h"

#include <atomic>
#include <thread>
#include <vector>

#include "common/util.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class SemaphoreTest : public ::testing::Test {
 protected:
  bool PollUntil(std::function<bool()> predicate, uint32_t timeout_ms) {
    uint32_t ms = 0;
    while (!predicate()) {
      Util::Sleep(1);

      ms++;
      if (ms >= timeout_ms) {
        return false;
      }
    }
    return true;
  }
};

TEST_F(SemaphoreTest, BlocksIfInitialCountIsZero) {
  Semaphore semaphore(0);
  std::atomic_bool ready(false);
  std::atomic_bool done(false);
  std::thread thread([&semaphore, &ready, &done]() {
    ready = true;
    semaphore.Wait();
    done = true;
  });
  EXPECT_TRUE(PollUntil([&ready]() { return ready.load(); }, 5000));
  // This will time out, so use a short timeout. Is there a way to test whether
  // the thread is blocking without using a timeout?
  EXPECT_FALSE(PollUntil([&done]() { return done.load(); }, 10));
  semaphore.Signal();
  EXPECT_TRUE(PollUntil([&done]() { return done.load(); }, 5000));
  thread.join();
}

TEST_F(SemaphoreTest, DoesNotBlockIfInitialCountIsOne) {
  Semaphore semaphore(1);
  std::atomic_bool first_wait(false);
  std::atomic_bool done(false);
  std::thread thread([&semaphore, &first_wait, &done]() {
    semaphore.Wait();
    first_wait = true;
    semaphore.Wait();
    done = true;
  });
  EXPECT_TRUE(PollUntil([&first_wait]() { return first_wait.load(); }, 5000));
  // This will time out, so use a short timeout. Is there a way to test whether
  // the thread is blocking without using a timeout?
  EXPECT_FALSE(PollUntil([&done]() { return done.load(); }, 10));
  semaphore.Signal();
  EXPECT_TRUE(PollUntil([&done]() { return done.load(); }, 5000));
  thread.join();
}

TEST_F(SemaphoreTest, SignalManyThreads) {
  Semaphore semaphore(0);
  std::atomic_int a{0};
  std::atomic_int b{0};
  std::atomic_int c{0};

  const int N = 16;
  std::vector<std::thread> threads;
  for (int n = 0; n < N; ++n) {
    threads.emplace_back([&semaphore, &a, &b, &c]() {
      ++a;
      semaphore.Wait();
      ++b;
      semaphore.Wait();
      ++c;
    });
  }

  // All threads should be at the first wait.
  EXPECT_TRUE(PollUntil([&]() { return a == N; }, 5000));

  for (int n = 0; n < N; ++n) {
    semaphore.Signal();
  }

  // Some threads should be at or past the second wait.
  // Note: If the Semaphore were fair, it would be cnt[1] == N and cnt[2] == 0.
  EXPECT_TRUE(PollUntil([&]() { return b + c == N; }, 5000));

  for (int n = 0; n < N; ++n) {
    semaphore.Signal();
  }

  // All threads should have finished.
  EXPECT_TRUE(PollUntil([&]() { return b == N && c == N; }, 5000));

  for (int n = 0; n < N; ++n) {
    threads[n].join();
  }
}

}  // namespace
}  // namespace cdc_ft
