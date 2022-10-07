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

#include "cdc_rsync_server/file_deleter_and_sender.h"

#include "cdc_rsync/base/fake_socket.h"
#include "cdc_rsync/base/message_pump.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

constexpr bool kFile = false;
constexpr bool kDir = true;

constexpr bool kDryRun = true;
constexpr bool kNoDryRun = false;

namespace cdc_ft {
namespace {

// Note: FileDiffGenerator is a server-only class and only runs on GGP, but the
// code is independent of the platform, so we can test it from Windows.
class FileDeleterAndSenderTest : public ::testing::Test {
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    message_pump_.StartMessagePump();
    tmp_dir_ = path::GetTempDir();
    path::EnsureDoesNotEndWithPathSeparator(&tmp_dir_);
  }

  void TearDown() override {
    // Make sure there are no more AddDeletedFilesResponse.
    ShutdownRequest shutdown;
    EXPECT_OK(message_pump_.SendMessage(PacketType::kShutdown, shutdown));
    EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kShutdown, &shutdown));

    socket_.ShutdownSendingEnd();
    message_pump_.StopMessagePump();
    Log::Shutdown();
  }

 protected:
  // Creates a temp file in %TMP% with given |relative_path|.
  std::string CreateTempFile(std::string relative_path) {
    std::string full_path = path::Join(tmp_dir_, relative_path);

    std::string dir = path::DirName(full_path);
    EXPECT_OK(path::CreateDirRec(dir));
    EXPECT_OK(path::WriteFile(full_path, ""));
    EXPECT_TRUE(path::Exists(full_path));
    return full_path;
  }

  // Creates a bunch of temp files in %TMP% with given |relative_paths|.
  std::vector<std::string> CreateTempFiles(
      std::vector<std::string> relative_paths) {
    std::vector<std::string> full_paths;
    for (const std::string& relative_path : relative_paths) {
      full_paths.push_back(CreateTempFile(relative_path));
    }
    return full_paths;
  }

  // Creates a temp directory in %TMP% with given |relative_path|.
  std::string CreateTempDir(std::string relative_path) {
    std::string full_path = path::Join(tmp_dir_, relative_path);

    EXPECT_OK(path::CreateDirRec(full_path));
    EXPECT_TRUE(path::Exists(full_path));
    return full_path;
  }

  // Creates a bunch of temp directories in %TMP% with given |relative_paths|.
  std::vector<std::string> CreateTempDirs(
      std::vector<std::string> relative_paths) {
    std::vector<std::string> full_paths;
    for (const std::string& relative_path : relative_paths) {
      full_paths.push_back(CreateTempDir(relative_path));
    }
    return full_paths;
  }

  // Expects an AddDeletedFilesResponse with no files as EOF indicator.
  void ExpectEofMarker() {
    // Verify that there is only the empty "EOF" indicator message.
    AddDeletedFilesResponse response;
    EXPECT_OK(
        message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
    EXPECT_EQ(response.files_size(), 0);
  }

  FakeSocket socket_;
  MessagePump message_pump_{&socket_, MessagePump::PacketReceivedDelegate()};

  std::string tmp_dir_;
};

TEST_F(FileDeleterAndSenderTest, NoFiles) {
  // Delete no files, no dirs.
  FileDeleterAndSender deleter(&message_pump_);
  EXPECT_OK(deleter.Flush());

  ExpectEofMarker();
}

TEST_F(FileDeleterAndSenderTest, FilesDeletedAndSent) {
  // Create temp files.
  std::vector<std::string> full_paths = CreateTempFiles(
      {"__fdas_unittest_1.txt", "__fdas_unittest_2.txt",
       path::ToNative("__fdas_unittest_dir/__fdas_unittest_3.txt")});

  // Delete files.
  FileDeleterAndSender deleter(&message_pump_);
  for (const std::string& file : full_paths) {
    EXPECT_OK(deleter.DeleteAndSendFileOrDir(
        tmp_dir_, file.substr(tmp_dir_.size()), kNoDryRun, kFile));
  }
  EXPECT_OK(deleter.Flush());

  // Did the files get deleted?
  for (const std::string& file : full_paths) {
    EXPECT_FALSE(path::Exists(file));
  }

  // Verify that the data sent to the socket matches.
  AddDeletedFilesResponse response;
  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), path::ToNative("/"));
  ASSERT_EQ(response.files_size(), 2);
  ASSERT_EQ(response.dirs_size(), 0);
  EXPECT_EQ(response.files(0), "__fdas_unittest_1.txt");
  EXPECT_EQ(response.files(1), "__fdas_unittest_2.txt");

  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  ASSERT_EQ(response.dirs_size(), 0);
  EXPECT_EQ(response.directory(), path::ToNative("/__fdas_unittest_dir/"));
  ASSERT_EQ(response.files_size(), 1);
  EXPECT_EQ(response.files(0), "__fdas_unittest_3.txt");

  ExpectEofMarker();
}

TEST_F(FileDeleterAndSenderTest, DirsDeletedAndSent) {
  // Create temp dirs.
  std::vector<std::string> full_paths = CreateTempDirs(
      {"__fdas_unittest_dir",
       path::ToNative("__fdas_unittest_dir/__fdas_unittest_1"),
       path::ToNative("__fdas_unittest_dir/__fdas_unittest_2"),
       path::ToNative(
           "__fdas_unittest_dir/__fdas_unittest_1/__fdas_unittest_1_1")});

  // Delete files.
  FileDeleterAndSender deleter(&message_pump_);
  for (size_t idx = full_paths.size(); idx > 0; --idx) {
    EXPECT_OK(deleter.DeleteAndSendFileOrDir(
        tmp_dir_, full_paths[idx - 1].substr(tmp_dir_.size() + 1), kNoDryRun,
        kDir));
  }
  EXPECT_OK(deleter.Flush());

  // Did the dirs get deleted?
  for (const std::string& dir : full_paths) {
    EXPECT_FALSE(path::Exists(dir));
  }

  // Verify that the data sent to the socket matches.
  AddDeletedFilesResponse response;
  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(),
            path::ToNative("__fdas_unittest_dir/__fdas_unittest_1/"));
  ASSERT_EQ(response.files_size(), 0);
  ASSERT_EQ(response.dirs_size(), 1);
  EXPECT_EQ(response.dirs(0), "__fdas_unittest_1_1");

  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), path::ToNative("__fdas_unittest_dir/"));
  ASSERT_EQ(response.files_size(), 0);
  ASSERT_EQ(response.dirs_size(), 2);
  EXPECT_EQ(response.dirs(0), "__fdas_unittest_2");
  EXPECT_EQ(response.dirs(1), "__fdas_unittest_1");

  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), "");
  ASSERT_EQ(response.files_size(), 0);
  ASSERT_EQ(response.dirs_size(), 1);
  EXPECT_EQ(response.dirs(0), "__fdas_unittest_dir");

  ExpectEofMarker();
}

TEST_F(FileDeleterAndSenderTest, FilesDeletedAndSentDryRun) {
  // Create a temp file.
  std::string file_to_remove = CreateTempFile("__fdas_unittest_1.txt");

  // "Delete" the file in dry-run mode: the file should not be deleted.
  // It should be just sent to the socket.
  FileDeleterAndSender deleter(&message_pump_);
  EXPECT_OK(deleter.DeleteAndSendFileOrDir(
      tmp_dir_, file_to_remove.substr(tmp_dir_.size()), kDryRun, kFile));
  EXPECT_OK(deleter.Flush());

  EXPECT_TRUE(path::Exists(file_to_remove));

  // Verify that the data sent to the socket matches.
  AddDeletedFilesResponse response;
  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), path::ToNative("/"));
  ASSERT_EQ(response.files_size(), 1);
  EXPECT_EQ(response.files(0), "__fdas_unittest_1.txt");

  ExpectEofMarker();
}

TEST_F(FileDeleterAndSenderTest, MessageSplitByMaxSize) {
  // Create temp files.
  std::vector<std::string> full_paths =
      CreateTempFiles({"__fdas_unittest_1.txt", "__fdas_unittest_2.txt"});

  // Delete files. The size is picked so that the message gets split.
  FileDeleterAndSender deleter(&message_pump_, /*max_request_byte_size=*/20);
  for (const std::string& file : full_paths) {
    EXPECT_OK(deleter.DeleteAndSendFileOrDir(
        tmp_dir_, file.substr(tmp_dir_.size()), kNoDryRun, kFile));
  }
  EXPECT_OK(deleter.Flush());

  // Verify that the data sent to the socket matches.
  AddDeletedFilesResponse response;
  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), path::ToNative("/"));
  ASSERT_EQ(response.files_size(), 1);
  EXPECT_EQ(response.files(0), "__fdas_unittest_1.txt");

  EXPECT_OK(
      message_pump_.ReceiveMessage(PacketType::kAddDeletedFiles, &response));
  EXPECT_EQ(response.directory(), path::ToNative("/"));
  ASSERT_EQ(response.files_size(), 1);
  EXPECT_EQ(response.files(0), "__fdas_unittest_2.txt");

  ExpectEofMarker();
}

}  // namespace
}  // namespace cdc_ft
