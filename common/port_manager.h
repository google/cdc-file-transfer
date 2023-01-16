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

#ifndef COMMON_PORT_MANAGER_H_
#define COMMON_PORT_MANAGER_H_

#include <memory>
#include <string>
#include <unordered_set>

#include "absl/status/statusor.h"
#include "common/arch_type.h"
#include "common/clock.h"

namespace cdc_ft {

class ProcessFactory;
class RemoteUtil;
class SharedMemory;

// Class for reserving ports globally. Use if there can be multiple processes
// of the same type that might request ports at the same time, e.g. multiple
// cdc_rsync.exe processes running concurrently.
class PortManager {
 public:
  // |unique_name| is a globally unique name used for shared memory to
  // synchronize port reservation. The range of possible ports managed by this
  // instance is [|first_port|, |last_port|]. |process_factory| is a valid
  // pointer to a ProcessFactory instance to run processes locally.
  // |remote_util| is the RemoteUtil instance to run processes remotely. If it
  //   is nullptr, no remote ports are reserved.
  PortManager(std::string unique_name, int first_port, int last_port,
              ProcessFactory* process_factory, RemoteUtil* remote_util,
              SystemClock* system_clock = DefaultSystemClock::GetInstance(),
              SteadyClock* steady_clock = DefaultSteadyClock::GetInstance());
  ~PortManager();

  // Reserves a port in the range passed to the constructor. The port is
  // released automatically upon destruction if ReleasePort() is not called
  // explicitly.
  // |remote_timeout_sec| is the timeout for finding available ports on the
  // remote instance.
  // |remote_arch_type| is the architecture of the remote device.
  // Both |remote_timeout_sec| and |remote_arch_type| are ignored if
  // |remote_util| is nullptr. Returns a DeadlineExceeded error if the timeout
  // is exceeded. Returns a ResourceExhausted error if no ports are available.
  absl::StatusOr<int> ReservePort(int remote_timeout_sec,
                                  ArchType remote_arch_type);

  // Releases a reserved port.
  absl::Status ReleasePort(int port);

  //
  // Lower-level interface for finding available ports directly.
  //

  // Finds available ports in the range [first_port, last_port] for port
  // forwarding on the local workstation.
  // |arch_type| is the architecture of the local device.
  // |process_factory| is used to create a netstat process.
  // Returns ResourceExhaustedError if no port is available.
  static absl::StatusOr<std::unordered_set<int>> FindAvailableLocalPorts(
      int first_port, int last_port, ArchType arch_type,
      ProcessFactory* process_factory);

  // Finds available ports in the range [first_port, last_port] for port
  // forwarding on the instance.
  // |arch_type| is the architecture of the remote device.
  // |process_factory| is used to create a netstat process.
  // |remote_util| is used to connect to the instance.
  // |timeout_sec| is the connection timeout in seconds.
  // Returns a DeadlineExceeded error if the timeout is exceeded.
  // Returns ResourceExhaustedError if no port is available.
  static absl::StatusOr<std::unordered_set<int>> FindAvailableRemotePorts(
      int first_port, int last_port, ArchType arch_type,
      ProcessFactory* process_factory, RemoteUtil* remote_util, int timeout_sec,
      SteadyClock* steady_clock = DefaultSteadyClock::GetInstance());

 private:
  // Returns a list of available ports in the range [|first_port|, |last_port|]
  // from the given |netstat_output|.
  // |arch_type| is the architecture of the device where netstat was called.
  // Returns ResourceExhaustedError if no port is available.
  static absl::StatusOr<std::unordered_set<int>> FindAvailablePorts(
      int first_port, int last_port, const std::string& netstat_output,
      ArchType arch_type);

  int first_port_;
  int last_port_;
  ProcessFactory* process_factory_;
  RemoteUtil* remote_util_;
  SystemClock* system_clock_;
  SteadyClock* steady_clock_;
  std::unique_ptr<SharedMemory> shared_mem_;
  std::unordered_set<int> reserved_ports_;
};

}  // namespace cdc_ft

#endif  // COMMON_PORT_MANAGER_H_
