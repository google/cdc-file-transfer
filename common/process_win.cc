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

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>
#include <vector>

#include "absl/strings/str_format.h"
#include "common/scoped_handle_win.h"
#include "common/status.h"
#include "common/util.h"

namespace cdc_ft {

namespace {

// SetThreadDescription is not available on all Windows versions,
// e.g. Win Server 2016, which happens to run on YRunners!.
typedef HRESULT(WINAPI* SetThreadDescription)(HANDLE hThread,
                                              PCWSTR lpThreadDescription);

// Sets the name of the current thread to |name|.
// Works from Windows 10 version 1607 on, no-op otherwise.
void SetThreadName(const std::string& name) {
  static auto set_thread_description_func =
      reinterpret_cast<SetThreadDescription>(::GetProcAddress(
          ::GetModuleHandle(L"Kernel32.dll"), "SetThreadDescription"));
  if (set_thread_description_func) {
    set_thread_description_func(::GetCurrentThread(),
                                Util::Utf8ToWideStr(name).c_str());
  }
}

int ToCreationFlags(ProcessFlags pflags) {
#define HANDLE_FLAG(pflag, cflag)  \
  if ((pflags & pflag) == pflag) { \
    cflags |= cflag;               \
    pdone = pdone | pflag;         \
  }

  int cflags = 0;
  ProcessFlags pdone = ProcessFlags::kNone;
  HANDLE_FLAG(ProcessFlags::kDetached, DETACHED_PROCESS);
  HANDLE_FLAG(ProcessFlags::kNoWindow, CREATE_NO_WINDOW);
  assert(pflags == pdone);

#undef HANDLE_FLAG

  return cflags;
}

std::atomic_int g_pipe_serial_number{0};

// Creates a pipe suitable for overlapped IO. Regular anonymous pipes in Windows
// don't support overlapped IO. This method creates a named pipe with a unique
// name, sets it up for overlapped IO and returns read/write ends.
absl::Status CreatePipeForOverlappedIo(ScopedHandle* pipe_read_end,
                                       ScopedHandle* pipe_write_end) {
  // We need named pipes for overlapped IO, so create a unique name.
  int id = g_pipe_serial_number++;
  std::string pipe_name = absl::StrFormat(R"(\\.\Pipe\CdcIoPipe.%08x.%08x)",
                                          GetCurrentProcessId(), id);

  // Set the bInheritHandle flag so pipe handles are inherited.
  SECURITY_ATTRIBUTES security_attributes;
  security_attributes.nLength = sizeof(SECURITY_ATTRIBUTES);
  security_attributes.bInheritHandle = TRUE;
  security_attributes.lpSecurityDescriptor = nullptr;

  *pipe_read_end = ScopedHandle(CreateNamedPipeA(
      pipe_name.c_str(), PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED,
      PIPE_TYPE_BYTE | PIPE_WAIT,
      1,           // Number of pipes
      4096,        // Out buffer size
      4096,        // In buffer size
      120 * 1000,  // Timeout in ms
      &security_attributes));

  if (!pipe_read_end->IsValid()) {
    return MakeStatus("Failed to create pipe read end: %s",
                      Util::GetLastWin32Error());
  }

  *pipe_write_end =
      ScopedHandle(CreateFileA(pipe_name.c_str(), GENERIC_WRITE,
                               0,  // No sharing
                               &security_attributes, OPEN_EXISTING,
                               FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
                               nullptr));  // Template file

  if (!pipe_write_end->IsValid()) {
    // Note that Close() might change GetLastErrorString()!
    absl::Status status = MakeStatus("Failed to create pipe write end: %s",
                                     Util::GetLastWin32Error());
    pipe_read_end->Close();
    return status;
  }

  return absl::OkStatus();
}

// Creates a pipe intended for piping stdin from this process to a child
// process. The read end is inherited to the child process (so it can read
// stdin from it), the write end is not. The pipe is NOT suitable for async IO.
absl::Status SetUpInputPipe(ScopedHandle* read_end, ScopedHandle* write_end) {
  // Set the bInheritHandle flag so pipe handles are inherited.
  SECURITY_ATTRIBUTES security_attributes;
  security_attributes.nLength = sizeof(SECURITY_ATTRIBUTES);
  security_attributes.bInheritHandle = TRUE;
  security_attributes.lpSecurityDescriptor = nullptr;

  // Create pipe.
  HANDLE read_end_handle, write_end_handle;
  if (!CreatePipe(&read_end_handle, &write_end_handle, &security_attributes,
                  4096)) {
    return MakeStatus("Failed to create input pipes");
  }
  *read_end = ScopedHandle(read_end_handle);
  *write_end = ScopedHandle(write_end_handle);

  // Ensure the write end of the pipe is not inherited to the child process.
  if (!SetHandleInformation(write_end->Get(), HANDLE_FLAG_INHERIT, 0)) {
    // Note that Close() might change GetLastErrorString()!
    absl::Status status = MakeStatus("Failed to set handle information: %s",
                                     Util::GetLastWin32Error());
    read_end->Close();
    write_end->Close();
    return status;
  }

  return absl::OkStatus();
}

// Creates a pipe intended for piping stdout/stderr from a child process to this
// process. The write end is inherited to the child process (so it can write
// stdout/stderr to it), the read end is not. The pipe is suitable for async IO.
absl::Status SetUpOutputPipe(ScopedHandle* read_end, ScopedHandle* write_end) {
  // Create pipe.
  absl::Status status = CreatePipeForOverlappedIo(read_end, write_end);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to create output pipes");
  }

  // Ensure the read end of the pipe is not inherited to the child process.
  if (!SetHandleInformation(read_end->Get(), HANDLE_FLAG_INHERIT, 0)) {
    // Note that Close() might change GetLastErrorString()!
    status = MakeStatus("Failed to set handle information: %s",
                        Util::GetLastWin32Error());
    read_end->Close();
    write_end->Close();
    return status;
  }

  return absl::OkStatus();
}

// Helper class for performing async IO from the stdout/stderr pipes created by
// SetUpOutputPipe().
class AsyncReader {
 public:
  using OutputHandler = ProcessStartInfo::OutputHandler;

  AsyncReader(HANDLE pipe_handle, OutputHandler output_handler)
      : pipe_handle_(pipe_handle),
        output_handler_(std::move(output_handler)),
        buffer_(4096) {
    ZeroMemory(&overlapped_, sizeof(overlapped_));
  }

  ~AsyncReader() {
    // Better cancel pending IO before the buffers get deleted. I heard from a
    // "friend" that they got a heap corruption when they didn't do it.
    absl::Status status = CancelPendingIo();
    if (!status.ok()) {
      LOG_WARNING("%s", status.ToString());
    }
  }

  // Returns the event that is triggered when async IO completes.
  HANDLE GetEvent() const { return event_.Get(); }

  // Initialize the IO event (see GetEvent()) and issue an async IO request.
  absl::Status Initialize() {
    // Create signaled manual reset event.
    event_ = ScopedHandle(CreateEvent(nullptr, TRUE, TRUE, nullptr));
    if (!event_.IsValid()) {
      return MakeStatus("CreateEvent failed: %s", Util::GetLastWin32Error());
    }
    overlapped_.hEvent = event_.Get();

    // Start reading.
    absl::Status status = IssueRead();
    if (!status.ok()) {
      return WrapStatus(status, "IssueRead() failed");
    }

    return absl::OkStatus();
  }

  // Reads the result of the async IO request. Async IO must be pending.
  // Should be called if the IO event (see GetEvent()) was triggered, otherwise
  // the method will block until the async IO result is available.
  absl::Status Read() {
    assert(io_pending_);
    io_pending_ = false;

    DWORD num_bytes_read;
    if (!GetOverlappedResult(pipe_handle_, &overlapped_, &num_bytes_read,
                             true)) {
      switch (GetLastError()) {
        case ERROR_BROKEN_PIPE:
          // The pipe was closed by the child process. Set the EOF() marker.
          LOG_VERBOSE("EOF");
          eof_ = true;
          break;

        default:
          return MakeStatus("GetOverlappedResult() failed: %s",
                            Util::GetLastWin32Error());
      }
    } else {
      // Async IO succeeded. Append null terminator in case the handler accesses
      // the data like a C string.
      assert(num_bytes_read < buffer_.size());
      buffer_[num_bytes_read] = 0;
      absl::Status status = output_handler_(buffer_.data(), num_bytes_read);
      if (!status.ok()) {
        // Don't return an error, it stops the pump thread and leads to freezes.
        LOG_DEBUG("%s", WrapStatus(status, "Output handler failed").ToString());
      }
    }

    if (!ResetEvent(event_.Get())) {
      return MakeStatus("ResetEvent() failed: %s", Util::GetLastWin32Error());
    }

    // Only issue a new read if the pipe is still open.
    if (!eof_) {
      absl::Status status = IssueRead();
      if (!status.ok()) {
        return WrapStatus(status, "IssueRead() failed");
      }
    }

    return absl::OkStatus();
  }

  // Cancels the currently pending async IO request if there is any.
  // Must be called from the same thread as Initialize() and Read().
  absl::Status CancelPendingIo() {
    if (!io_pending_) {
      return absl::OkStatus();
    }

    if (!CancelIo(pipe_handle_)) {
      return MakeStatus(
          "CancelIo() failed. If you get a heap corruption, this is why.");
    }

    io_pending_ = false;
    return absl::OkStatus();
  }

 private:
  // Queues a new async IO request. If data is already available, immediately
  // calls the output handler. Returns false on error (failing output handler,
  // IO error).
  absl::Status IssueRead() {
    assert(!io_pending_);

    // The pipe might already contain data that can be read synchronously. Just
    // keep reading it.
    DWORD num_bytes_read;
    while (ReadFile(pipe_handle_, buffer_.data(),
                    static_cast<DWORD>(buffer_.size()) - 1, &num_bytes_read,
                    &overlapped_)) {
      // Append a null terminator in case handler interprets the data as C str.
      assert(num_bytes_read < buffer_.size());
      buffer_[num_bytes_read] = 0;
      absl::Status status = output_handler_(buffer_.data(), num_bytes_read);
      if (!status.ok()) {
        // Don't return an error, it stops the pump thread and leads to freezes.
        LOG_DEBUG("%s", WrapStatus(status, "Output handler failed").ToString());
      }
    }

    switch (GetLastError()) {
      case ERROR_IO_PENDING:
        // Async IO in progress, this is expected. The caller should wait for
        // the event (see GetEvent()) to retrieve the data.
        io_pending_ = true;
        return absl::OkStatus();

      case ERROR_BROKEN_PIPE:
        // The pipe was closed by the child process. Set the EOF() marker.
        LOG_VERBOSE("EOF");
        eof_ = true;
        return absl::OkStatus();

      default:
        return MakeStatus("ReadFile failed: %s", Util::GetLastWin32Error());
    }
  }

  // Not owned.
  HANDLE pipe_handle_;
  OutputHandler output_handler_;

  std::vector<char> buffer_;
  OVERLAPPED overlapped_;
  ScopedHandle event_;
  bool eof_ = false;
  bool io_pending_ = false;
};

struct ProcessInfo {
  PROCESS_INFORMATION pi;

  ScopedHandle job;

  ScopedHandle stdin_write_end;
  ScopedHandle stdout_read_end;
  ScopedHandle stderr_read_end;

  ProcessInfo() { ZeroMemory(&pi, sizeof(pi)); }
};

// Background thread to read stdout/stderr from the child process.
// Also watches the child process for exit.
class MessagePumpThread {
 public:
  MessagePumpThread(const ProcessInfo& process_info,
                    const ProcessStartInfo& start_info)
      : process_handle_(process_info.pi.hProcess), name_(start_info.Name()) {
    // Initialize stdout reader if necessary.
    if (process_info.stdout_read_end.IsValid()) {
      stdout_reader_ = std::make_unique<AsyncReader>(
          process_info.stdout_read_end.Get(), start_info.stdout_handler);
    }

    // Initialize stderr reader if necessary.
    if (process_info.stderr_read_end.IsValid()) {
      stderr_reader_ = std::make_unique<AsyncReader>(
          process_info.stderr_read_end.Get(), start_info.stderr_handler);
    }

    // Create manual reset event that is not signaled.
    shutdown_event_ = ScopedHandle(CreateEvent(nullptr, TRUE, FALSE, nullptr));

    worker_ = std::thread([this]() { ThreadWorkerMain(); });
  }

  ~MessagePumpThread() { Shutdown(); }

  void Shutdown() {
    if (shutdown_event_.IsValid() && !SetEvent(shutdown_event_.Get())) {
      // Can't do much if this fails.
      LOG_ERROR("Shutting down process message thread failed");
      exit(1);
    }

    if (worker_.joinable()) {
      worker_.join();
    }
  }

  // Contains the error message if some error occurred and the message pump
  // thread was shut down.
  absl::Status GetStatus() {
    std::lock_guard<std::mutex> lock(status_mutex_);
    return status_;
  }

  // Returns true if the process has exited.
  bool HasExited() {
    std::lock_guard<std::mutex> lock(exit_mutex_);
    return has_exited_;
  }

  // Returns the process exit code.
  uint32_t ExitCode() {
    std::lock_guard<std::mutex> lock(exit_mutex_);
    return exit_code_;
  }

 private:
  void SetStatus(absl::Status status) {
    LOG_DEBUG("Setting status %s of process %s", status.ToString().c_str(),
              name_.c_str());
    std::lock_guard<std::mutex> lock(status_mutex_);
    status_ = status;
  }

  void ThreadWorkerMain() {
    SetThreadName(name_);
    LOG_VERBOSE("Process message thread started: %s", name_.c_str());

    if (!shutdown_event_.IsValid()) {
      SetStatus(
          MakeStatus("CreateEvent failed: %s", Util::GetLastWin32Error()));
      return;
    }

    // Be sure to call AsyncReader::Initialize() from this thread, so all
    // AsyncIO stays here.

    // Initialize stdout reader if present (schedules AsyncIO).
    if (stdout_reader_) {
      absl::Status init_status = stdout_reader_->Initialize();
      if (!init_status.ok()) {
        SetStatus(
            WrapStatus(init_status, "Failed to initialize stdout reader"));
        return;
      }
    }

    // Initialize stderr reader if present (schedules AsyncIO).
    if (stderr_reader_) {
      absl::Status init_status = stderr_reader_->Initialize();
      if (!init_status.ok()) {
        SetStatus(
            WrapStatus(init_status, "Failed to initialize stderr reader"));
        return;
      }
    }

    // Initialize handles to watch.
    std::vector<HANDLE> watch_handles;

    size_t process_index = watch_handles.size();
    watch_handles.push_back(process_handle_);

    size_t shutdown_index = watch_handles.size();
    watch_handles.push_back(shutdown_event_.Get());

    size_t stdout_index = SIZE_MAX;
    if (stdout_reader_) {
      stdout_index = watch_handles.size();
      watch_handles.push_back(stdout_reader_->GetEvent());
    }

    size_t stderr_index = SIZE_MAX;
    if (stderr_reader_) {
      stderr_index = watch_handles.size();
      watch_handles.push_back(stderr_reader_->GetEvent());
    }

    for (;;) {
      const uint32_t wait_index =
          WaitForMultipleObjects(static_cast<DWORD>(watch_handles.size()),
                                 watch_handles.data(), false, UINT_MAX);
      if (wait_index == WAIT_TIMEOUT) {
        continue;
      }

      const uint32_t handle_index = wait_index - WAIT_OBJECT_0;
      if (handle_index >= watch_handles.size()) {
        SetStatus(MakeStatus(
            "WaitForMultipleObjects failed with invalid handle index %u",
            handle_index));
        return;
      }

      if (handle_index == process_index) {
        // Process exited.
        std::lock_guard<std::mutex> lock(exit_mutex_);
        has_exited_ = true;

        // Get process exit code.
        if (!GetExitCodeProcess(process_handle_, &exit_code_)) {
          LOG_WARNING("Failed to get exit code for process '%s': %s", name_,
                      Util::GetLastWin32Error());
          exit_code_ = Process::kExitCodeFailedToGetExitCode;
        }

        break;
      }

      if (handle_index == shutdown_index) {
        // Shutdown() was called.
        break;
      }

      if (handle_index == stdout_index) {
        // Data on stdout is available.
        absl::Status status = stdout_reader_->Read();
        if (!status.ok()) {
          SetStatus(WrapStatus(status, "Failed to read stdout"));
          return;
        }
        continue;
      }

      if (handle_index == stderr_index) {
        // Data on stderr is available.
        absl::Status status = stderr_reader_->Read();
        if (!status.ok()) {
          SetStatus(WrapStatus(status, "Failed to read stderr"));
          return;
        }
        continue;
      }
    }

    // Cancel any pending IO.
    stdout_reader_.reset();
    stderr_reader_.reset();

    LOG_VERBOSE("Process message thread stopped: %s", name_.c_str());
  }

  HANDLE process_handle_;
  std::string name_;

  std::thread worker_;
  ScopedHandle shutdown_event_;

  absl::Status status_;
  std::mutex status_mutex_;

  bool has_exited_ = false;
  DWORD exit_code_ = Process::kExitCodeStillRunning;
  std::mutex exit_mutex_;

  std::unique_ptr<AsyncReader> stdout_reader_;
  std::unique_ptr<AsyncReader> stderr_reader_;
};

// Try to guess the log level from |str|, e.g. LogLevel::kError if |str| starts
// with "ERROR".
LogLevel GuessLogLevel(const char* str) {
  if (strncmp(str, "ERROR", 5) == 0)
    return LogLevel::kError;
  else if (strncmp(str, "WARNING", 7) == 0)
    return LogLevel::kWarning;
  else if (strncmp(str, "DEBUG", 5) == 0)
    return LogLevel::kDebug;
  else if (strncmp(str, "VERBOSE", 7) == 0)
    return LogLevel::kVerbose;
  return LogLevel::kInfo;
}

}  // namespace

absl::Status LogOutput(const char* name, const char* data, size_t /*size*/,
                       absl::optional<LogLevel> log_level) {
  const char* newline_pos = strpbrk(data, "\r\n");
  while (newline_pos) {
    if (newline_pos > data) {
      LOG_LEVEL(log_level ? *log_level : GuessLogLevel(data), "%s: %s", name,
                std::string(data, newline_pos - data));
    }
    data = newline_pos + 1;
    newline_pos = strpbrk(data, "\r\n");
  }
  // There's always guaranteed to be NULL terminator, even if |size| is 0.
  // Note that the rest here might be an incomplete line, but we print it,
  // anyway, to not risk loosing data.
  if (data[0] != 0) {
    LOG_LEVEL(log_level ? *log_level : GuessLogLevel(data), "%s: %s", name,
              data);
  }
  return absl::OkStatus();
}

const std::string& ProcessStartInfo::Name() const {
  return !name.empty() ? name : command;
}

bool ProcessStartInfo::HasFlag(ProcessFlags flag) const {
  return (flags & flag) == flag;
}

Process::Process(const ProcessStartInfo& start_info)
    : start_info_(start_info) {}

Process::~Process() = default;

absl::Status Process::RunUntilExit() {
  return RunUntil([]() { return false; });
}

// Implementation of a Windows process.
class WinProcess : public Process {
 public:
  explicit WinProcess(const ProcessStartInfo& start_info);
  ~WinProcess() override;

  // Process:
  absl::Status Start() override;
  absl::Status RunUntil(std::function<bool()> exit_condition) override;
  absl::Status Terminate() override;
  absl::Status WriteToStdIn(const void* data, size_t size) override;
  void CloseStdIn() override;
  bool HasExited() const override;
  uint32_t ExitCode() const override;
  absl::Status GetStatus() const override;

 private:
  void Reset();

  std::unique_ptr<ProcessInfo> process_info_;
  std::unique_ptr<MessagePumpThread> message_pump_;
};

WinProcess::WinProcess(const ProcessStartInfo& start_info)
    : Process(start_info) {}

WinProcess::~WinProcess() {
  if (start_info_.HasFlag(ProcessFlags::kDetached)) {
    // If the process runs detached, just reset handles, don't terminate it.
    Reset();
  } else {
    Terminate().IgnoreError();
  }
}

absl::Status WinProcess::Start() {
  LOG_INFO("Starting process %s", start_info_.command.c_str());

  std::wstring command = Util::Utf8ToWideStr(start_info_.command);
  wchar_t* command_cstr = const_cast<wchar_t*>(command.c_str());

  STARTUPINFO si;
  ZeroMemory(&si, sizeof(si));
  si.cb = sizeof(si);

  process_info_ = std::make_unique<ProcessInfo>();

  // Create stdout pipes if necessary.
  ScopedHandle stdin_read_end;
  if (start_info_.redirect_stdin) {
    absl::Status status =
        SetUpInputPipe(&stdin_read_end, &process_info_->stdin_write_end);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to set up stdin pipes");
    }
  }

  // Set up handlers to forward output to logging.
  if (!start_info_.stdout_handler && start_info_.forward_output_to_log) {
    start_info_.stdout_handler = [name = start_info_.Name() + "_stdout"](
                                     const char* data, size_t size) {
      return LogOutput(name.c_str(), data, size);
    };
  }
  if (!start_info_.stderr_handler && start_info_.forward_output_to_log) {
    start_info_.stderr_handler = [name = start_info_.Name() + "_stderr"](
                                     const char* data, size_t size) {
      return LogOutput(name.c_str(), data, size, LogLevel::kError);
    };
  }

  // Create stdout pipes if necessary.
  ScopedHandle stdout_write_end;
  if (start_info_.stdout_handler) {
    absl::Status status =
        SetUpOutputPipe(&process_info_->stdout_read_end, &stdout_write_end);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to set up stdout pipes");
    }
  }

  // Create stderr pipes if necessary.
  ScopedHandle stderr_write_end;
  if (start_info_.stderr_handler) {
    absl::Status status =
        SetUpOutputPipe(&process_info_->stderr_read_end, &stderr_write_end);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to set up stderr pipes");
    }
  }

  // Set up handle redirection. Note that it's not possible to redirect only
  // some handles, so use GetStdHandle() for the ones not redirected.
  si.dwFlags |= STARTF_USESTDHANDLES;
  si.hStdInput = stdin_read_end.IsValid() ? stdin_read_end.Get()
                                          : GetStdHandle(STD_INPUT_HANDLE);
  si.hStdOutput = stdout_write_end.IsValid() ? stdout_write_end.Get()
                                             : GetStdHandle(STD_OUTPUT_HANDLE);
  si.hStdError = stderr_write_end.IsValid() ? stderr_write_end.Get()
                                            : GetStdHandle(STD_ERROR_HANDLE);

  // Make sure the process gets closed if the parent (this!) process exits.
  process_info_->job = ScopedHandle(CreateJobObject(NULL, NULL));
  if (!process_info_->job.IsValid()) {
    return MakeStatus("CreateJobObject() failed: %s",
                      Util::GetLastWin32Error());
  }

  JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = {0};
  if (!start_info_.HasFlag(ProcessFlags::kDetached)) {
    jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  }
  bool success = SetInformationJobObject(process_info_->job.Get(),
                                         JobObjectExtendedLimitInformation,
                                         &jeli, sizeof(jeli));

  if (!success) {
    return MakeStatus("SetInformationJobObject() failed: %s",
                      Util::GetLastWin32Error());
  }

  std::wstring startup_dir = Util::Utf8ToWideStr(start_info_.startup_dir);
  const wchar_t* startup_dir_cstr =
      !startup_dir.empty() ? startup_dir.c_str() : nullptr;

  // Start the child process.
  success = CreateProcess(NULL,  // No module name (use command line)
                          command_cstr,
                          NULL,  // Process handle not inheritable
                          NULL,  // Thread handle not inheritable
                          TRUE,  // Inherit handles
                          ToCreationFlags(start_info_.flags),
                          NULL,  // Use parent's environment block
                          startup_dir_cstr, &si, &process_info_->pi);

  if (!success) {
    return MakeStatus("CreateProcess() failed: %s", Util::GetLastWin32Error());
  }

  success = AssignProcessToJobObject(process_info_->job.Get(),
                                     process_info_->pi.hProcess);
  if (!success) {
    return MakeStatus("AssignProcessToJobObject() failed: %s",
                      Util::GetLastWin32Error());
  }

  // Explicitly close our copies of the input read end and output write ends.
  // The child process still has a copy.
  stdin_read_end.Close();
  stdout_write_end.Close();
  stderr_write_end.Close();

  // Start message pump thread.
  message_pump_ =
      std::make_unique<MessagePumpThread>(*process_info_, start_info_);

  return absl::OkStatus();
}

absl::Status WinProcess::RunUntil(std::function<bool()> exit_condition) {
  if (!process_info_) {
    return MakeStatus("Process was never started");
  }

  // Wait until exit condition is fulfilled.
  while (!exit_condition()) {
    // Poll |message_pump_|. This is not super performance critical, so a simple
    // sleep should do.
    Util::Sleep(10);

    absl::Status status = message_pump_->GetStatus();
    if (!status.ok()) return status;
    if (HasExited()) return absl::OkStatus();
  }

  return absl::OkStatus();
}

absl::Status WinProcess::WriteToStdIn(const void* data, size_t size) {
  if (!start_info_.redirect_stdin) {
    return absl::FailedPreconditionError("Stdin not redirected");
  }

  DWORD bytes_written = 0;
  assert(size <= UINT32_MAX);
  const DWORD dw_size = static_cast<DWORD>(size);
  if (!WriteFile(process_info_->stdin_write_end.Get(), data, dw_size,
                 &bytes_written, nullptr)) {
    return MakeStatus("WriteFile() failed: %s", Util::GetLastWin32Error());
  }

  if (bytes_written != size) {
    return MakeStatus("WriteFile() failed: Only %u / %u bytes written",
                      bytes_written, size);
  }

  return absl::OkStatus();
}

void WinProcess::CloseStdIn() {
  assert(start_info_.redirect_stdin);
  process_info_->stdin_write_end.Close();
}

bool WinProcess::HasExited() const {
  return message_pump_ ? message_pump_->HasExited() : false;
}

uint32_t WinProcess::ExitCode() const {
  return message_pump_ ? message_pump_->ExitCode() : kExitCodeNotStarted;
}

absl::Status WinProcess::GetStatus() const {
  return message_pump_ ? message_pump_->GetStatus() : absl::OkStatus();
}

absl::Status WinProcess::Terminate() {
  // Stop message pump.
  bool should_terminate = false;
  if (message_pump_) {
    should_terminate = !message_pump_->HasExited();
    message_pump_.reset();
  }

  std::string error_msg;
  if (process_info_ && should_terminate &&
      !TerminateProcess(process_info_->pi.hProcess, 0)) {
    if (GetLastError() == ERROR_ACCESS_DENIED) {
      // This means that the process has already exited, but in a way that
      // the exit wasn't properly reported to this code (e.g. the process got
      // killed somewhere). Just handle this silently.
      LOG_DEBUG("Process '%s' already exited", start_info_.Name());
    } else {
      error_msg = Util::GetLastWin32Error();
    }
  }

  // Reset handles.
  Reset();

  if (!error_msg.empty()) {
    return MakeStatus("TerminateProcess() failed: %s", error_msg);
  }
  return absl::OkStatus();
}

void WinProcess::Reset() {
  // Shut down message pump.
  message_pump_.reset();

  if (process_info_) {
    // Close the handles that are not scoped handles.
    ScopedHandle(process_info_->pi.hProcess).Close();
    ScopedHandle(process_info_->pi.hThread).Close();

    process_info_.reset();
  }
}

ProcessFactory::~ProcessFactory() = default;

absl::Status ProcessFactory::Run(const ProcessStartInfo& start_info) {
  std::unique_ptr<Process> process = Create(start_info);

  absl::Status status = process->Start();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to start process '%s'",
                      start_info.Name());
  }

  status = process->RunUntilExit();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to run process '%s'", start_info.Name());
  }

  uint32_t exit_code = process->ExitCode();
  if (exit_code != 0) {
    return MakeStatus("Process '%s' exited with code %u", start_info.Name(),
                      exit_code);
  }

  return absl::OkStatus();
}

WinProcessFactory::~WinProcessFactory() = default;

std::unique_ptr<Process> WinProcessFactory::Create(
    const ProcessStartInfo& start_info) {
  return std::unique_ptr<Process>(new WinProcess(start_info));
}

}  // namespace cdc_ft
