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

#include "common/process.h"

// Windows must be included first.
// clang-format off
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tlhelp32.h>
// clang-format on

#include <atomic>

#include "common/log.h"
#include "common/scoped_handle_win.h"
#include "common/status_test_macros.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Terminates a process by name.
// Returns true if the process was terminated.
// Returns false if the process was not found or if it failed to be terminated.
bool TerminateProcessByName(const char* name) {
  PROCESSENTRY32 entry;
  entry.dwSize = sizeof(PROCESSENTRY32);

  ScopedHandle snapshot(CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, NULL));

  std::wstring wide_name = Util::Utf8ToWideStr(name);
  if (Process32First(snapshot.Get(), &entry)) {
    do {
      if (_wcsicmp(entry.szExeFile, wide_name.c_str()) != 0) continue;
      ScopedHandle hProcess(
          OpenProcess(PROCESS_ALL_ACCESS, FALSE, entry.th32ProcessID));
      if (!TerminateProcess(hProcess.Get(), 0)) {
        LOG_ERROR("Failed to terminate process '%s'", name)
        return false;
      }
      return true;
    } while (Process32Next(snapshot.Get(), &entry));
  }

  LOG_ERROR("Failed to find process '%s'", name)
  return false;
}

// Filters for "echos" and puts all messages and levels into a vector.
class EchosTestLog : public Log {
 public:
  explicit EchosTestLog(LogLevel log_level) : Log(log_level) {}

  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override {
    if (strncmp(message, "echos", strlen("echos")) == 0) {
      levels.push_back(level);
      messages.push_back(message);
    }
  }

  std::vector<LogLevel> levels;
  std::vector<std::string> messages;
};

class ProcessTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  WinProcessFactory process_factory_;
};

TEST_F(ProcessTest, ProcessNotStarted) {
  ProcessStartInfo start_info;
  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_NOT_OK(process->RunUntilExit());
  EXPECT_EQ(process->ExitCode(), Process::kExitCodeNotStarted);
}

TEST_F(ProcessTest, RunSimpleCommandSucceeds) {
  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"echo\"";

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
}

TEST_F(ProcessTest, RunSimpleCommandFails) {
  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"dir /INVALID\"";

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 1u);
}

TEST_F(ProcessTest, RunAndReadStdOut) {
  std::string std_out;
  std::string message = "hello world";

  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"echo " + message + "\"";
  start_info.stdout_handler = [&std_out](const char* data, size_t) {
    std_out += data;
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
  EXPECT_EQ(std_out, message + "\r\n");
}

TEST_F(ProcessTest, RunAndReadStdOutAndStdErr) {
  std::string std_out;
  std::string std_err;
  std::string stdout_message = "stdout message";
  std::string stderr_message = "stderr message";

  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"echo " + stdout_message + " & echo " +
                       stderr_message + " 1>&2\"";
  start_info.stdout_handler = [&std_out](const char* data, size_t) {
    std_out += data;
    return absl::OkStatus();
  };
  start_info.stderr_handler = [&std_err](const char* data, size_t) {
    std_err += data;
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
  EXPECT_EQ(std_out, stdout_message + " \r\n");
  EXPECT_EQ(std_err, stderr_message + " \r\n");
}

TEST_F(ProcessTest, RunWriteStdInAndReadStdOut) {
  std::string std_out;
  std::string message = "Foo\nBar\nfoo\nbar\nFar\nBoo";

  // Find all lines containing "F".
  ProcessStartInfo start_info;
  start_info.command = "findstr F";
  start_info.redirect_stdin = true;
  start_info.stdout_handler = [&std_out](const char* data, size_t) {
    std_out += data;
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_OK(process->WriteToStdIn(message.data(), message.size()));
  process->CloseStdIn();
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
  EXPECT_EQ(std_out, "Foo\nFar\n");
}

TEST_F(ProcessTest, RunIoStressTest) {
  std::string std_out;
  std::string expected_stdout;

  // Find all lines containing "1".
  ProcessStartInfo start_info;
  start_info.command = "findstr 1";
  start_info.redirect_stdin = true;
  start_info.stdout_handler = [&std_out](const char* data, size_t) {
    std_out += data;
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());

  // Write lots of lines to stdin.
  // Every other line starts with '1' and should be picked up by findstr.
  std::string message(2048, 'x');
  message.back() = '\n';
  const int num_lines = 1000;
  for (int n = 0; n < num_lines; ++n) {
    message[0] = n & 1 ? '1' : '2';
    if (n & 1) {
      expected_stdout += message;
    }
    EXPECT_OK(process->WriteToStdIn(message.data(), message.size()));
  }
  process->CloseStdIn();
  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
  EXPECT_EQ(std_out, expected_stdout);
}

TEST_F(ProcessTest, RunUntil) {
  std::string std_out;

  // Find all lines containing "msg". "findstr" is a convenient command that
  // echoes stdin to stdout for matching lines. "echo" doesn't work with stdin.
  ProcessStartInfo start_info;
  start_info.command = "findstr stop";
  start_info.redirect_stdin = true;
  std::atomic_bool stop{false};
  start_info.stdout_handler = [&std_out, &stop](const char* data, size_t) {
    // Check whether someone sent the "stop" command.
    // Note: This runs in a background thread.
    std_out += data;
    stop = std_out.find("stop") != std::string::npos;
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());

  // Send "msg stop" and check that it stops.
  std::string message = " stop\n";
  EXPECT_OK(process->WriteToStdIn(message.data(), message.size()));
  process->CloseStdIn();
  EXPECT_OK(process->RunUntil([&stop]() { return stop.load(); }));
  EXPECT_TRUE(stop);

  EXPECT_OK(process->RunUntilExit());
  EXPECT_TRUE(process->HasExited());
  EXPECT_EQ(process->ExitCode(), 0u);
}

TEST_F(ProcessTest, ForwardOutputToLogging) {
  Log::Shutdown();
  auto log_ptr = std::make_unique<EchosTestLog>(LogLevel::kInfo);
  EchosTestLog* log = log_ptr.get();
  Log::Initialize(std::move(log_ptr));

  const std::string stdout_message = "stdout message";
  const std::string stderr_message = "stderr message";

  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"echo " + stdout_message + " & echo " +
                       stderr_message + " 1>&2\"";
  start_info.forward_output_to_log = true;
  start_info.name = "echos";

  EXPECT_OK(process_factory_.Run(start_info));

  ASSERT_EQ(log->messages.size(), 2);
  EXPECT_EQ(log->messages[0], "echos_stdout: " + stdout_message + " ");
  EXPECT_EQ(log->messages[1], "echos_stderr: " + stderr_message + " ");
}

TEST_F(ProcessTest, LogOutputLevelDetection) {
  Log::Shutdown();
  auto log_ptr = std::make_unique<EchosTestLog>(LogLevel::kVerbose);
  EchosTestLog* log = log_ptr.get();
  Log::Initialize(std::move(log_ptr));

  ProcessStartInfo start_info;
  start_info.command = "cmd /C \"echo VERBOSE msg1 && ";
  start_info.command += "echo DEBUG msg2 && ";
  start_info.command += "echo INFO msg3 && ";
  start_info.command += "echo WARNING msg4 && ";
  start_info.command += "echo ERROR msg5 && ";
  start_info.command += "echo msg6\"";
  start_info.forward_output_to_log = true;
  start_info.name = "echos";

  EXPECT_OK(process_factory_.Run(start_info));

  ASSERT_EQ(log->messages.size(), 6);
  ASSERT_EQ(log->levels.size(), 6);

  EXPECT_EQ(log->messages[0], "echos_stdout: VERBOSE msg1 ");
  EXPECT_EQ(log->messages[1], "echos_stdout: DEBUG msg2 ");
  EXPECT_EQ(log->messages[2], "echos_stdout: INFO msg3 ");
  EXPECT_EQ(log->messages[3], "echos_stdout: WARNING msg4 ");
  EXPECT_EQ(log->messages[4], "echos_stdout: ERROR msg5 ");
  EXPECT_EQ(log->messages[5], "echos_stdout: msg6");

  EXPECT_EQ(log->levels[0], LogLevel::kVerbose);
  EXPECT_EQ(log->levels[1], LogLevel::kDebug);
  EXPECT_EQ(log->levels[2], LogLevel::kInfo);
  EXPECT_EQ(log->levels[3], LogLevel::kWarning);
  EXPECT_EQ(log->levels[4], LogLevel::kError);
  EXPECT_EQ(log->levels[5], LogLevel::kInfo);
}

TEST_F(ProcessTest, Terminate) {
  // Use ping to simulate a sleep instead of timeout since timeout fails with
  // "Input redirection is not supported".
  ProcessStartInfo start_info;
  start_info.command = "ping -n 30 127.0.0.1";
  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_EQ(process->ExitCode(), Process::kExitCodeStillRunning);
  EXPECT_OK(process->Terminate());
  EXPECT_EQ(process->ExitCode(), Process::kExitCodeNotStarted);
}

TEST_F(ProcessTest, TerminateAlreadyExited) {
  // Use ping to simulate a sleep instead of timeout since timeout fails with
  // "Input redirection is not supported".
  ProcessStartInfo start_info;
  start_info.command = "ping -n 30 127.0.0.1";
  std::unique_ptr<Process> process = process_factory_.Create(start_info);
  EXPECT_OK(process->Start());
  EXPECT_FALSE(process->HasExited());
  bool terminated = false;
  Stopwatch sw;
  while (sw.ElapsedSeconds() < 5 && !terminated) {
    terminated = TerminateProcessByName("ping.exe");
    if (!terminated) Util::Sleep(1);
  }
  EXPECT_TRUE(terminated);
  EXPECT_OK(process->Terminate());
}

TEST_F(ProcessTest, StartupDir) {
  ProcessStartInfo start_info;
  start_info.command = "cmd /C cd";
  start_info.startup_dir = "C:\\";

  std::string std_out;
  start_info.stdout_handler = [&std_out](const char* data, size_t) {
    std_out += data;
    return absl::OkStatus();
  };

  EXPECT_OK(process_factory_.Run(start_info));
  EXPECT_EQ(std_out, "C:\\\r\n");
}

}  // namespace
}  // namespace cdc_ft
