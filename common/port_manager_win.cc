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

#include "common/port_manager.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <map>

#include "absl/strings/str_split.h"
#include "common/arch_type.h"
#include "common/log.h"
#include "common/process.h"
#include "common/remote_util.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/stopwatch.h"
#include "common/util.h"

namespace cdc_ft {

constexpr char kErrorArchTypeUnhandled[] = "arch_type_unhandled";

// Returns the arch-specific netstat command.
const char* GetNetstatCommand(ArchType arch_type) {
  if (IsWindowsArchType(arch_type)) {
    // -a to get the connection and ports the computer is listening on.
    // -n to get numerical addresses to avoid the overhead of getting names.
    // -p tcp to limit the output to TCPv4 connections.
    return "netstat -a -n -p tcp";
  }

  if (IsLinuxArchType(arch_type)) {
    // Prefer ss over netstat. The flags out output are compatible.
    // --numeric to get numerical addresses.
    // --listening to get only listening sockets.
    // --tcp to get only TCP connections.
    return "which ss2 && ss --numeric --listening --tcp || netstat "
           "--numeric --listening --tcp";
  }

  assert(!kErrorArchTypeUnhandled);
  return "";
}

// Returns the arch-specific IP address to filter netstat results by.
const char* GetNetstatFilterIp(ArchType arch_type) {
  if (IsWindowsArchType(arch_type)) {
    return "127.0.0.1";
  }

  if (IsLinuxArchType(arch_type)) {
    return "0.0.0.0";
  }

  assert(!kErrorArchTypeUnhandled);
  return "";
}

class SharedMemory {
 public:
  // Creates a new shared memory instance with given |name| and |size| in bytes.
  // Different instances with matching names reference the same piece of memory,
  // even if they belong to different processes. If shared memory with the given
  // |name| already exists, the existing memory is referenced. Otherwise, a new
  // piece of memory is allocated and zero-initialized.
  SharedMemory(std::string name, size_t size)
      : name_(std::move(name)), size_(size) {}

  absl::StatusOr<void*> Get() {
    // Already initialized?
    if (shared_mem_) return shared_mem_;
    assert(!map_file_handle_);

    LARGE_INTEGER size;
    size.QuadPart = size_;
    map_file_handle_ = CreateFileMapping(
        INVALID_HANDLE_VALUE,  // use paging file
        nullptr,               // default security
        PAGE_READWRITE,        // read/write access
        size.HighPart,         // maximum object size (high-order DWORD)
        size.LowPart,          // maximum object size (low-order DWORD)
        Util::Utf8ToWideStr(name_).c_str());  // name of mapping object

    if (!map_file_handle_) {
      return MakeStatus("Failed to create file mapping object: %s",
                        Util::GetLastWin32Error());
    }

    // The shared memory holds the timestamps when the ports were reserved.
    shared_mem_ = MapViewOfFile(map_file_handle_,     // handle to map object
                                FILE_MAP_ALL_ACCESS,  // read/write permission
                                0, 0, size.QuadPart);

    if (!shared_mem_) {
      std::string errorMessage = Util::GetLastWin32Error();
      CloseHandle(map_file_handle_);
      map_file_handle_ = nullptr;
      return MakeStatus("Failed to map view of file: %s", errorMessage);
    }

    return shared_mem_;
  }

  ~SharedMemory() {
    if (shared_mem_) {
      UnmapViewOfFile(shared_mem_);
      shared_mem_ = nullptr;
    }

    if (map_file_handle_) {
      CloseHandle(map_file_handle_);
      map_file_handle_ = nullptr;
    }
  }

 private:
  std::string name_;
  size_t size_;
  HANDLE map_file_handle_ = nullptr;
  void* shared_mem_ = nullptr;
};

PortManager::PortManager(std::string name, int first_port, int last_port,
                         ProcessFactory* process_factory,
                         RemoteUtil* remote_util, SystemClock* system_clock,
                         SteadyClock* steady_clock)
    : first_port_(first_port),
      last_port_(last_port),
      process_factory_(process_factory),
      remote_util_(remote_util),
      system_clock_(system_clock),
      steady_clock_(steady_clock),
      shared_mem_(std::make_unique<SharedMemory>(
          std::move(name), (last_port - first_port + 1) * sizeof(time_t))) {
  assert(last_port_ >= first_port_);
}

PortManager::~PortManager() {
  std::vector<int> ports_copy;
  ports_copy.insert(ports_copy.end(), reserved_ports_.begin(),
                    reserved_ports_.end());
  for (int port : ports_copy) {
    absl::Status status = ReleasePort(port);
    if (!status.ok()) {
      LOG_WARNING("Failed to release port %d: %s", port, status.ToString());
    }
  }
}

absl::StatusOr<int> PortManager::ReservePort(int remote_timeout_sec,
                                             ArchType remote_arch_type) {
  // Find available port on workstation.
  std::unordered_set<int> local_ports;
  ASSIGN_OR_RETURN(
      local_ports,
      FindAvailableLocalPorts(first_port_, last_port_,
                              ArchType::kWindows_x86_64, process_factory_),
      "Failed to find available ports on workstation");

  // Find available port on remote instance.
  std::unordered_set<int> remote_ports = local_ports;
  if (remote_util_ != nullptr) {
    ASSIGN_OR_RETURN(
        remote_ports,
        FindAvailableRemotePorts(first_port_, last_port_, remote_arch_type,
                                 process_factory_, remote_util_,
                                 remote_timeout_sec, steady_clock_),
        "Failed to find available ports on instance");
  }

  // Fetch shared memory.
  void* mem;
  ASSIGN_OR_RETURN(mem, shared_mem_->Get(), "Failed to get shared memory");
  time_t* port_timestamps = static_cast<time_t*>(mem);

  // Put ports into a multimap to iterate in LRU order.
  int num_ports = last_port_ - first_port_ + 1;
  std::multimap<time_t, int> ports_to_index;
  for (int n = 0; n < num_ports; ++n) {
    ports_to_index.insert({port_timestamps[n], n});
  }

  // Iterate over the ports, unused first (timestamp 0), the rest in LRU order.
  // The ones with timestamps != 0 might either be stuck (e.g. process crashed
  // and did not release port) or still in use.
  const time_t now = std::chrono::system_clock::to_time_t(system_clock_->Now());
  for (const auto& [port_timestamp, n] : ports_to_index) {
    // Note that some other process might have hijacked the port in the
    // meantime, hence do an InterlockedCompareExchange.
    volatile time_t* ts_ptr = &port_timestamps[n];
    static_assert(sizeof(time_t) == sizeof(uint64_t), "time_t must be 64 bit");
    assert((reinterpret_cast<uintptr_t>(ts_ptr) & 7) == 0);
    if (InterlockedCompareExchange64(ts_ptr, now, port_timestamp) ==
        port_timestamp) {
      int port = first_port_ + n;
      LOG_DEBUG("Trying to reserve port %i", port);

      // We have reserved this port. Double-check that it's actually not in use
      // on both the workstation and the server.
      if (local_ports.find(port) == local_ports.end()) {
        LOG_DEBUG("Port %i not available on workstation", port);
        InterlockedCompareExchange64(ts_ptr, now, port_timestamp);
        continue;
      }
      if (remote_ports.find(port) == remote_ports.end()) {
        LOG_DEBUG("Port %i not available on instance", port);
        InterlockedCompareExchange64(ts_ptr, now, port_timestamp);
        continue;
      }

      LOG_DEBUG("Port %i is available on workstation and instance", port);
      reserved_ports_.insert(port);
      return port;
    }
  }

  return absl::ResourceExhaustedError(absl::StrFormat(
      "No port available in range [%i, %i]", first_port_, last_port_));
}

absl::Status PortManager::ReleasePort(int port) {
  if (reserved_ports_.find(port) == reserved_ports_.end())
    return absl::OkStatus();
  void* mem;
  ASSIGN_OR_RETURN(mem, shared_mem_->Get(), "Failed to get shared memory");
  time_t* port_timestamps = static_cast<time_t*>(mem);
  volatile time_t* ts_ptr = &port_timestamps[port - first_port_];
  InterlockedExchange64(ts_ptr, 0);
  reserved_ports_.erase(port);
  return absl::OkStatus();
}

// static
absl::StatusOr<std::unordered_set<int>> PortManager::FindAvailableLocalPorts(
    int first_port, int last_port, ArchType arch_type,
    ProcessFactory* process_factory) {
  // TODO: Use local APIs instead of netstat.
  ProcessStartInfo start_info;
  start_info.command = GetNetstatCommand(arch_type);
  start_info.name = "netstat";
  start_info.flags = ProcessFlags::kNoWindow;

  std::string output;
  start_info.stdout_handler = [&output](const char* data, size_t data_size) {
    output.append(data, data_size);
    return absl::OkStatus();
  };
  std::string errors;
  start_info.stderr_handler = [&errors](const char* data, size_t data_size) {
    errors.append(data, data_size);
    return absl::OkStatus();
  };

  absl::Status status = process_factory->Run(start_info);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to run netstat:\n%s", errors);
  }

  LOG_DEBUG("netstat (workstation) output:\n%s", output);
  return FindAvailablePorts(first_port, last_port, output, arch_type);
}

// static
absl::StatusOr<std::unordered_set<int>> PortManager::FindAvailableRemotePorts(
    int first_port, int last_port, ArchType arch_type,
    ProcessFactory* process_factory, RemoteUtil* remote_util, int timeout_sec,
    SteadyClock* steady_clock) {
  std::string remote_command = GetNetstatCommand(arch_type);
  ProcessStartInfo start_info =
      remote_util->BuildProcessStartInfoForSsh(remote_command, arch_type);
  start_info.name = "netstat";
  start_info.flags = ProcessFlags::kNoWindow;

  std::string output;
  start_info.stdout_handler = [&output](const char* data, size_t data_size) {
    output.append(data, data_size);
    return absl::OkStatus();
  };
  std::string errors;
  start_info.stderr_handler = [&errors](const char* data, size_t data_size) {
    errors.append(data, data_size);
    return absl::OkStatus();
  };

  std::unique_ptr<Process> process = process_factory->Create(start_info);
  absl::Status status = process->Start();
  if (!status.ok()) return WrapStatus(status, "Failed to start netstat");

  Stopwatch timeout_timer(steady_clock);
  bool is_timeout = false;
  auto detect_timeout = [&timeout_timer, timeout_sec, &is_timeout]() {
    is_timeout = timeout_timer.ElapsedSeconds() > timeout_sec;
    return is_timeout;
  };
  status = process->RunUntil(detect_timeout);
  if (!status.ok()) return WrapStatus(status, "Failed to run netstat process");
  if (is_timeout)
    return absl::DeadlineExceededError("Timeout while running netstat");

  uint32_t exit_code = process->ExitCode();
  if (exit_code != 0) {
    return MakeStatus("netstat process exited with code %u:\n%s", exit_code,
                      errors);
  }

  LOG_DEBUG("netstat (instance) output:\n%s", output);
  return FindAvailablePorts(first_port, last_port, output, arch_type);
}

// static
absl::StatusOr<std::unordered_set<int>> PortManager::FindAvailablePorts(
    int first_port, int last_port, const std::string& netstat_output,
    ArchType arch_type) {
  std::unordered_set<int> available_ports;
  std::vector<std::string> lines;
  const char* filter_ip = GetNetstatFilterIp(arch_type);
  for (const auto& line : absl::StrSplit(netstat_output, '\n')) {
    if (absl::StrContains(line, filter_ip)) {
      lines.push_back(std::string(line));
    }
  }

  for (int port = first_port; port <= last_port; ++port) {
    bool port_occupied = false;
    std::string portToken = absl::StrFormat("%s:%i", filter_ip, port);
    for (const std::string& line : lines) {
      // Ports in the TIME_WAIT state can be reused. It is common that ports
      // stay in this state for O(minutes).
      if (absl::StrContains(line, portToken) &&
          !absl::StrContains(line, "TIME_WAIT")) {
        port_occupied = true;
        break;
      }
    }
    if (!port_occupied) available_ports.insert(port);
  }

  if (available_ports.empty()) {
    return absl::ResourceExhaustedError(absl::StrFormat(
        "No port available in range [%i, %i]", first_port, last_port));
  }

  return available_ports;
}

}  // namespace cdc_ft
