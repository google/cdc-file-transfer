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

#include "common/remote_util.h"

#include "absl/strings/match.h"
#include "common/log.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

constexpr int kSshPort = 12345;
constexpr char kSshPortArg[] = "-p 12345";

constexpr char kHostname[] = "user@example.com";
constexpr char kHostnameArg[] = "\"user@example.com\"";

constexpr int kLocalPort = 23456;
constexpr int kRemotePort = 34567;
constexpr bool kRegular = false;  // Regular port forwarding
constexpr bool kReverse = true;   // Reverse port forwarding
constexpr char kPortForwardingArg[] = "-L23456:localhost:34567";
constexpr char kReversePortForwardingArg[] = "-R34567:localhost:23456";

constexpr char kCommand[] = "my_command";

class RemoteUtilTest : public ::testing::Test {
 public:
  RemoteUtilTest()
      : util_(/*verbosity=*/0, /*quiet=*/false, &process_factory_,
              /*forward_output_to_log=*/true) {}

  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    util_.SetHostAndPort(kHostname, kSshPort);
  }

  void TearDown() override { Log::Shutdown(); }

 protected:
  void ExpectContains(const std::string& str, std::vector<const char*> tokens) {
    for (const char* token : tokens) {
      EXPECT_TRUE(absl::StrContains(str, token))
          << str << "\ndoes not contain\n"
          << token;
    }
  }

  WinProcessFactory process_factory_;
  RemoteUtil util_;
};

TEST_F(RemoteUtilTest, BuildProcessStartInfoForSsh) {
  ProcessStartInfo si = util_.BuildProcessStartInfoForSsh(kCommand);
  ExpectContains(si.command, {"ssh.exe", kSshPortArg, kHostnameArg, kCommand});
}

TEST_F(RemoteUtilTest, BuildProcessStartInfoForSshPortForward) {
  ProcessStartInfo si = util_.BuildProcessStartInfoForSshPortForward(
      kLocalPort, kRemotePort, kRegular);
  ExpectContains(si.command,
                 {"ssh.exe", kSshPortArg, kHostnameArg, kPortForwardingArg});

  si = util_.BuildProcessStartInfoForSshPortForward(kLocalPort, kRemotePort,
                                                    kReverse);
  ExpectContains(si.command, {"ssh.exe", kSshPortArg, kHostnameArg,
                              kReversePortForwardingArg});
}

TEST_F(RemoteUtilTest, BuildProcessStartInfoForSshPortForwardAndCommand) {
  ProcessStartInfo si = util_.BuildProcessStartInfoForSshPortForwardAndCommand(
      kLocalPort, kRemotePort, kRegular, kCommand);
  ExpectContains(si.command, {"ssh.exe", kSshPortArg, kHostnameArg,
                              kPortForwardingArg, kCommand});

  si = util_.BuildProcessStartInfoForSshPortForwardAndCommand(
      kLocalPort, kRemotePort, kReverse, kCommand);
  ExpectContains(si.command, {"ssh.exe", kSshPortArg, kHostnameArg,
                              kReversePortForwardingArg, kCommand});
}
TEST_F(RemoteUtilTest, BuildProcessStartInfoForSshWithCustomCommand) {
  constexpr char kCustomSshCmd[] = "C:\\path\\to\\ssh.exe --fooarg --bararg=42";
  util_.SetSshCommand(kCustomSshCmd);
  ProcessStartInfo si = util_.BuildProcessStartInfoForSsh(kCommand);
  ExpectContains(si.command, {kCustomSshCmd});
}

}  // namespace
}  // namespace cdc_ft
