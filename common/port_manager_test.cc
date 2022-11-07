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

#include "absl/strings/match.h"
#include "common/log.h"
#include "common/remote_util.h"
#include "common/status_test_macros.h"
#include "common/stub_process.h"
#include "common/testing_clock.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr int kSshPort = 12345;
constexpr char kHostname[] = "user@1.2.3.4";

constexpr char kGuid[] = "f77bcdfe-368c-4c45-9f01-230c5e7e2132";
constexpr int kFirstPort = 44450;
constexpr int kLastPort = 44459;
constexpr int kNumPorts = kLastPort - kFirstPort + 1;

constexpr int kTimeoutSec = 1;

constexpr char kLocalNetstat[] = "netstat -a -n -p tcp";
constexpr char kRemoteNetstat[] = "netstat --numeric --listening --tcp";

constexpr char kLocalNetstatOutFmt[] =
    "TCP    127.0.0.1:50000        127.0.0.1:%i        ESTABLISHED";
constexpr char kRemoteNetstatOutFmt[] =
    "tcp        0      0 0.0.0.0:%i           0.0.0.0:*               LISTEN";

class PortManagerTest : public ::testing::Test {
 public:
  PortManagerTest()
      : remote_util_(/*verbosity=*/0, /*quiet=*/false, &process_factory_,
                     /*forward_output_to_log=*/true),
        port_manager_(kGuid, kFirstPort, kLastPort, &process_factory_,
                      &remote_util_, &system_clock_, &steady_clock_) {}

  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    remote_util_.SetHostAndPort(kHostname, kSshPort);
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  StubProcessFactory process_factory_;
  TestingSystemClock system_clock_;
  TestingSteadyClock steady_clock_;
  RemoteUtil remote_util_;
  PortManager port_manager_;
};

TEST_F(PortManagerTest, ReservePortSuccess) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  ASSERT_OK(port);
  EXPECT_EQ(*port, kFirstPort);
}

TEST_F(PortManagerTest, ReservePortNoRemoteSuccess) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(false, kTimeoutSec);
  ASSERT_OK(port);
  EXPECT_EQ(*port, kFirstPort);
}

TEST_F(PortManagerTest, ReservePortAllLocalPortsTaken) {
  std::string local_netstat_out = "";
  for (int port = kFirstPort; port <= kLastPort; ++port) {
    local_netstat_out += absl::StrFormat(kLocalNetstatOutFmt, port);
  }
  process_factory_.SetProcessOutput(kLocalNetstat, local_netstat_out, "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_TRUE(absl::IsResourceExhausted(port.status()));
  EXPECT_TRUE(
      absl::StrContains(port.status().message(), "No port available in range"));
}

TEST_F(PortManagerTest, ReservePortAllRemotePortsTaken) {
  std::string remote_netstat_out = "";
  for (int port = kFirstPort; port <= kLastPort; ++port) {
    remote_netstat_out += absl::StrFormat(kRemoteNetstatOutFmt, port);
  }
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, remote_netstat_out, "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_TRUE(absl::IsResourceExhausted(port.status()));
  EXPECT_TRUE(
      absl::StrContains(port.status().message(), "No port available in range"));
}

TEST_F(PortManagerTest, ReservePortLocalNetstatFails) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 1);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_NOT_OK(port);
  EXPECT_TRUE(
      absl::StrContains(port.status().message(),
                        "Failed to find available ports on workstation"));
}

TEST_F(PortManagerTest, ReservePortRemoteNetstatFails) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 1);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_NOT_OK(port);
  EXPECT_TRUE(absl::StrContains(port.status().message(),
                                "Failed to find available ports on instance"));
}

TEST_F(PortManagerTest, ReservePortRemoteNetstatTimesOut) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessNeverExits(kRemoteNetstat);
  steady_clock_.AutoAdvance(kTimeoutSec * 2 * 1000);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_NOT_OK(port);
  EXPECT_TRUE(absl::IsDeadlineExceeded(port.status()));
  EXPECT_TRUE(absl::StrContains(port.status().message(),
                                "Timeout while running netstat"));
}

TEST_F(PortManagerTest, ReservePortMultipleInstances) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  PortManager port_manager2(kGuid, kFirstPort, kLastPort, &process_factory_,
                            &remote_util_);

  // Port managers use shared memory, so different instances know about each
  // other. This would even work if |port_manager_| and |port_manager2| belonged
  // to different processes, but we don't test that here.
  EXPECT_EQ(*port_manager_.ReservePort(true, kTimeoutSec), kFirstPort + 0);
  EXPECT_EQ(*port_manager2.ReservePort(true, kTimeoutSec), kFirstPort + 1);
  EXPECT_EQ(*port_manager_.ReservePort(true, kTimeoutSec), kFirstPort + 2);
  EXPECT_EQ(*port_manager2.ReservePort(true, kTimeoutSec), kFirstPort + 3);
}

TEST_F(PortManagerTest, ReservePortReusesPortsInLRUOrder) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  for (int n = 0; n < kNumPorts * 2; ++n) {
    EXPECT_EQ(*port_manager_.ReservePort(true, kTimeoutSec),
              kFirstPort + n % kNumPorts);
    system_clock_.Advance(1000);
  }
}

TEST_F(PortManagerTest, ReleasePort) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  absl::StatusOr<int> port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_EQ(*port, kFirstPort);
  EXPECT_OK(port_manager_.ReleasePort(*port));
  port = port_manager_.ReservePort(true, kTimeoutSec);
  EXPECT_EQ(*port, kFirstPort);
}

TEST_F(PortManagerTest, ReleasePortOnDestruction) {
  process_factory_.SetProcessOutput(kLocalNetstat, "", "", 0);
  process_factory_.SetProcessOutput(kRemoteNetstat, "", "", 0);

  auto port_manager2 = std::make_unique<PortManager>(
      kGuid, kFirstPort, kLastPort, &process_factory_, &remote_util_);
  EXPECT_EQ(*port_manager2->ReservePort(true, kTimeoutSec), kFirstPort + 0);
  EXPECT_EQ(*port_manager_.ReservePort(true, kTimeoutSec), kFirstPort + 1);
  port_manager2.reset();
  EXPECT_EQ(*port_manager_.ReservePort(true, kTimeoutSec), kFirstPort + 0);
}

TEST_F(PortManagerTest, FindAvailableLocalPortsSuccess) {
  // First port is taken
  std::string local_netstat_out =
      absl::StrFormat(kLocalNetstatOutFmt, kFirstPort);
  process_factory_.SetProcessOutput(kLocalNetstat, local_netstat_out, "", 0);

  absl::StatusOr<std::unordered_set<int>> ports =
      PortManager::FindAvailableLocalPorts(kFirstPort, kLastPort, "127.0.0.1",
                                           &process_factory_, true);
  ASSERT_OK(ports);
  EXPECT_EQ(ports->size(), kNumPorts - 1);
  for (int port = kFirstPort + 1; port <= kLastPort; ++port) {
    EXPECT_TRUE(ports->find(port) != ports->end());
  }
}

TEST_F(PortManagerTest, FindAvailableLocalPortsFailsNoPorts) {
  // All ports taken
  std::string local_netstat_out = "";
  for (int port = kFirstPort; port <= kLastPort; ++port) {
    local_netstat_out += absl::StrFormat(kLocalNetstatOutFmt, port);
  }
  process_factory_.SetProcessOutput(kLocalNetstat, local_netstat_out, "", 0);

  absl::StatusOr<std::unordered_set<int>> ports =
      PortManager::FindAvailableLocalPorts(kFirstPort, kLastPort, "127.0.0.1",
                                           &process_factory_, true);
  EXPECT_TRUE(absl::IsResourceExhausted(ports.status()));
  EXPECT_TRUE(absl::StrContains(ports.status().message(),
                                "No port available in range"));
}

TEST_F(PortManagerTest, FindAvailableRemotePortsSuccess) {
  // First port is taken
  std::string remote_netstat_out =
      absl::StrFormat(kRemoteNetstatOutFmt, kFirstPort);
  process_factory_.SetProcessOutput(kRemoteNetstat, remote_netstat_out, "", 0);

  absl::StatusOr<std::unordered_set<int>> ports =
      PortManager::FindAvailableRemotePorts(kFirstPort, kLastPort, "0.0.0.0",
                                            &process_factory_, &remote_util_,
                                            kTimeoutSec, true);
  ASSERT_OK(ports);
  EXPECT_EQ(ports->size(), kNumPorts - 1);
  for (int port = kFirstPort + 1; port <= kLastPort; ++port) {
    EXPECT_TRUE(ports->find(port) != ports->end());
  }
}

TEST_F(PortManagerTest, FindAvailableRemotePortsFailsNoPorts) {
  // All ports taken
  std::string remote_netstat_out = "";
  for (int port = kFirstPort; port <= kLastPort; ++port) {
    remote_netstat_out += absl::StrFormat(kRemoteNetstatOutFmt, port);
  }
  process_factory_.SetProcessOutput(kRemoteNetstat, remote_netstat_out, "", 0);

  absl::StatusOr<std::unordered_set<int>> ports =
      PortManager::FindAvailableRemotePorts(kFirstPort, kLastPort, "0.0.0.0",
                                            &process_factory_, &remote_util_,
                                            kTimeoutSec, true);
  EXPECT_TRUE(absl::IsResourceExhausted(ports.status()));
  EXPECT_TRUE(absl::StrContains(ports.status().message(),
                                "No port available in range"));
}

}  // namespace
}  // namespace cdc_ft
