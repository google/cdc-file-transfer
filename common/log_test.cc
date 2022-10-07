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

#include "common/log.h"

#include "absl/strings/match.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr char kLogMessage[] = "Test log";

class TestLog : public Log {
 public:
  explicit TestLog(LogLevel log_level) : Log(log_level) {}

  LogLevel LastLevel() const { return last_level_; }
  const std::string& LastFunc() const { return last_func_; }
  const std::string& LastMessage() const { return last_message_; }

 protected:
  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override {
    constexpr char expected_file[] = "log_test.cc";
    // The full filename depends on the compiler invocation, so we just compare
    // the basename.
    EXPECT_TRUE(absl::StrContains(file, expected_file))
        << "File name '" << file << "' does not contain expected name '"
        << expected_file << "'.";
    EXPECT_GT(line, 0);
    last_level_ = level;
    last_func_ = func;
    last_message_ = message;
  }

 private:
  LogLevel last_level_;
  std::string last_func_;
  std::string last_message_;
};

class LogTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<TestLog>(LogLevel::kInfo));
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  TestLog* Log() const { return static_cast<TestLog*>(Log::Instance()); }
};

TEST_F(LogTest, LogMessage) {
  LOG_INFO(kLogMessage);
  EXPECT_EQ(kLogMessage, Log()->LastMessage());
}

TEST_F(LogTest, LogFormattedMessage) {
  LOG_INFO("Test %i %s", 123, "message");
  EXPECT_EQ("Test 123 message", Log()->LastMessage());
}

TEST_F(LogTest, IgnoresBelowLogLevel) {
  EXPECT_EQ(Log()->GetLogLevel(), LogLevel::kInfo);
  LOG_DEBUG(kLogMessage);
  EXPECT_EQ("", Log()->LastMessage());

  Log()->SetLogLevel(LogLevel::kDebug);
  LOG_DEBUG(kLogMessage);
  EXPECT_EQ(kLogMessage, Log()->LastMessage());
}

TEST_F(LogTest, VerbosityToLogLevel) {
  EXPECT_EQ(Log::VerbosityToLogLevel(0), LogLevel::kWarning);
  EXPECT_EQ(Log::VerbosityToLogLevel(1), LogLevel::kWarning);
  EXPECT_EQ(Log::VerbosityToLogLevel(2), LogLevel::kInfo);
  EXPECT_EQ(Log::VerbosityToLogLevel(3), LogLevel::kDebug);
  EXPECT_EQ(Log::VerbosityToLogLevel(4), LogLevel::kVerbose);
  EXPECT_EQ(Log::VerbosityToLogLevel(5), LogLevel::kVerbose);
}

TEST(FileLogTest, LogToFile) {
  std::string tmp_dir = path::GetTempDir();
  std::string log_path = path::Join(tmp_dir, "__log_unittest.log");
  Log::Initialize(std::make_unique<FileLog>(LogLevel::kInfo, log_path.c_str()));
  LOG_ERROR("Error");
  LOG_INFO("Info");
  LOG_DEBUG("Debug");
  Log::Shutdown();

  std::vector<std::string> lines;
  ASSERT_OK(
      path::ReadAllLines(log_path, &lines, path::ReadFlags::kRemoveEmpty));
  ASSERT_EQ(lines.size(), 2);

  EXPECT_TRUE(absl::StrContains(lines[0], "ERROR")) << lines[0];
  EXPECT_TRUE(absl::StrContains(lines[0], "log_test.cc")) << lines[0];
  EXPECT_TRUE(absl::StrContains(lines[0], "Error")) << lines[0];

  EXPECT_TRUE(absl::StrContains(lines[1], "INFO")) << lines[1];
  EXPECT_TRUE(absl::StrContains(lines[1], "log_test.cc")) << lines[1];
  EXPECT_TRUE(absl::StrContains(lines[1], "Info")) << lines[1];

  // Check date and time.
  int yy, mo, da, hh, mm, ss, msec;
  int scan_res = sscanf(lines[0].c_str(), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
                        &yy, &mo, &da, &hh, &mm, &ss, &msec);
  EXPECT_EQ(scan_res, 7) << lines[0];
}

TEST(NoLogTest, LogNotInitialized) {
  // Using the log before initializing it should not trigger an assertion.
  LOG_ERROR("Error");
  LOG_INFO("Info");
  LOG_DEBUG("Debug");
}

}  // namespace
}  // namespace cdc_ft
