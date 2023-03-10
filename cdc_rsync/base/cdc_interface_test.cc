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

#include "cdc_rsync/base/cdc_interface.h"

#include <cstdio>
#include <fstream>

#include "cdc_rsync/base/message_pump.h"
#include "common/fake_socket.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class FakeCdcProgress : public ReportCdcProgress {
 public:
  void ReportSyncProgress(uint64_t num_client_bytes_processed,
                          uint64_t num_server_bytes_processed) override {
    total_client_bytes_processed += num_client_bytes_processed;
    total_server_bytes_processed += num_server_bytes_processed;
  }

  uint64_t total_client_bytes_processed = 0;
  uint64_t total_server_bytes_processed = 0;
};

class CdcInterfaceTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    message_pump_.StartMessagePump();
  }

  void TearDown() override {
    socket_.ShutdownSendingEnd();
    message_pump_.StopMessagePump();
    Log::Shutdown();
  }

 protected:
  FakeSocket socket_;
  MessagePump message_pump_{&socket_, MessagePump::PacketReceivedDelegate()};

  std::string base_dir_ = GetTestDataDir("cdc_interface");
};

TEST_F(CdcInterfaceTest, SyncTest) {
  CdcInterface cdc(&message_pump_);
  FakeCdcProgress progress;

  const std::string old_filepath = path::Join(base_dir_, "old_file.txt");
  const std::string new_filepath = path::Join(base_dir_, "new_file.txt");
  const std::string patched_filepath =
      path::Join(base_dir_, "patched_file.txt");

  path::Stats old_stats;
  EXPECT_OK(path::GetStats(old_filepath, &old_stats));

  path::Stats new_stats;
  EXPECT_OK(path::GetStats(new_filepath, &new_stats));

  // Create signature of old file and send it to the fake socket (it'll just
  // send it to itself).
  EXPECT_OK(cdc.CreateAndSendSignature(old_filepath));

  // Receive the signature from the fake socket, generate the diff to the file
  // at |new_filepath| and send it to the socket again.
  absl::StatusOr<FILE*> new_file = path::OpenFile(new_filepath, "rb");
  EXPECT_OK(new_file);
  EXPECT_OK(cdc.ReceiveSignatureAndCreateAndSendDiff(*new_file, &progress));
  fclose(*new_file);

  // Receive the diff from the fake socket and create a patched file.
  std::FILE* patched_file = std::tmpfile();
  ASSERT_TRUE(patched_file != nullptr);
  bool is_executable = false;
  EXPECT_OK(
      cdc.ReceiveDiffAndPatch(old_filepath, patched_file, &is_executable));
  EXPECT_FALSE(is_executable);

  // Read new file.
  std::ifstream new_file_stream(new_filepath.c_str(), std::ios::binary);
  std::vector<uint8_t> new_file_data(
      std::istreambuf_iterator<char>(new_file_stream), {});

  // Read patched file.
  fseek(patched_file, 0, SEEK_END);
  std::vector<uint8_t> patched_file_data(ftell(patched_file));
  fseek(patched_file, 0, SEEK_SET);
  fread(patched_file_data.data(), 1, patched_file_data.size(), patched_file);

  // New and patched file should be equal now.
  EXPECT_EQ(patched_file_data, new_file_data);
  fclose(patched_file);

  // Verify progress tracker.
  EXPECT_EQ(progress.total_server_bytes_processed, old_stats.size);
  EXPECT_EQ(progress.total_client_bytes_processed, new_stats.size);
}

}  // namespace
}  // namespace cdc_ft
