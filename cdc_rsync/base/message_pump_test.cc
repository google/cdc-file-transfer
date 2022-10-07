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

#include "cdc_rsync/base/message_pump.h"

#include "cdc_rsync/base/fake_socket.h"
#include "cdc_rsync/protos/messages.pb.h"
#include "common/log.h"
#include "common/status.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class MessagePumpTest : public ::testing::Test {
 public:
  void SetUp() override {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
    message_pump_.StartMessagePump();
  }

  void TearDown() override {
    fake_socket_.ShutdownSendingEnd();
    message_pump_.StopMessagePump();
    Log::Shutdown();
  }

 protected:
  // Called on the receiver thread.
  void ThreadPackageReceived(PacketType type) {
    // Empty by default. Only takes effect if set by tests.
    if (type == PacketType::kToggleCompression) {
      message_pump_.RedirectInput(std::move(fake_compressed_input_reader_));
    }
  }

  FakeSocket fake_socket_;
  MessagePump message_pump_{
      &fake_socket_, [this](PacketType type) { ThreadPackageReceived(type); }};
  std::unique_ptr<MessagePump::InputReader> fake_compressed_input_reader_;
};

TEST_F(MessagePumpTest, SendReceiveRawData) {
  // The FakeSocket just routes everything that's sent to the receiving end.
  const Buffer raw_data = {'r', 'a', 'w'};
  EXPECT_OK(message_pump_.SendRawData(raw_data.data(), raw_data.size()));

  Buffer received_raw_data;
  EXPECT_OK(message_pump_.ReceiveRawData(&received_raw_data));

  EXPECT_EQ(raw_data, received_raw_data);
}

TEST_F(MessagePumpTest, SendReceiveMessage) {
  TestRequest request;
  request.set_message("message");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, request));

  TestRequest received_request;
  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &received_request));

  EXPECT_EQ(request.message(), received_request.message());
}

TEST_F(MessagePumpTest, SendReceiveMultiple) {
  const Buffer raw_data_1 = {'r', 'a', 'w', '1'};
  const Buffer raw_data_2 = {'r', 'a', 'w', '2'};
  TestRequest request;
  request.set_message("message");

  EXPECT_OK(message_pump_.SendRawData(raw_data_1.data(), raw_data_1.size()));
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, request));
  EXPECT_OK(message_pump_.SendRawData(raw_data_2.data(), raw_data_2.size()));

  Buffer received_raw_data_1;
  Buffer received_raw_data_2;
  TestRequest received_request;

  EXPECT_OK(message_pump_.ReceiveRawData(&received_raw_data_1));
  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &received_request));
  EXPECT_OK(message_pump_.ReceiveRawData(&received_raw_data_2));

  EXPECT_EQ(raw_data_1, received_raw_data_1);
  EXPECT_EQ(request.message(), received_request.message());
  EXPECT_EQ(raw_data_2, received_raw_data_2);
}

TEST_F(MessagePumpTest, ReceiveMessageInstreadOfRaw) {
  const Buffer raw_data = {'r', 'a', 'w'};
  EXPECT_OK(message_pump_.SendRawData(raw_data.data(), raw_data.size()));

  TestRequest received_request;
  EXPECT_NOT_OK(
      message_pump_.ReceiveMessage(PacketType::kTest, &received_request));
}

TEST_F(MessagePumpTest, ReceiveRawInsteadOfMessage) {
  TestRequest request;
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, request));

  Buffer received_raw_data;
  EXPECT_NOT_OK(message_pump_.ReceiveRawData(&received_raw_data));
}

TEST_F(MessagePumpTest, ReceiveMessageWrongType) {
  TestRequest request;
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, request));

  ShutdownRequest received_request;
  EXPECT_NOT_OK(
      message_pump_.ReceiveMessage(PacketType::kShutdown, &received_request));
}

TEST_F(MessagePumpTest, MessageMaxSizeExceeded) {
  TestRequest request;
  size_t max_size = message_pump_.GetMaxPacketSizeForTesting();
  request.set_message(std::string(max_size + 1, 'x'));
  EXPECT_NOT_OK(message_pump_.SendMessage(PacketType::kTest, request));
}

TEST_F(MessagePumpTest, FlushOutgoingQueue) {
  TestRequest request;
  request.set_message(std::string(1024 * 4, 'x'));
  constexpr size_t kNumMessages = 1000;

  // Note: Must stay below max queue size or else SendMessage starts blocking.
  ASSERT_LT((request.message().size() + 4) * kNumMessages,
            message_pump_.GetMaxInOutBufferSizeForTesting());

  // Queue up a bunch of large messages.
  fake_socket_.SuspendSending(true);
  for (size_t n = 0; n < kNumMessages; ++n) {
    EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, request));
  }
  EXPECT_GT(message_pump_.GetNumOutgoingPackagesForTesting(), 0);

  // Flush the queue.
  fake_socket_.SuspendSending(false);
  message_pump_.FlushOutgoingQueue();

  // Check if the queue is empty.
  EXPECT_EQ(message_pump_.GetNumOutgoingPackagesForTesting(), 0);
}

class FakeCompressedInputReader : public MessagePump::InputReader {
 public:
  explicit FakeCompressedInputReader(Socket* socket) : socket_(socket) {}

  // Doesn't actually do compression, just replaces the word "compressed" by
  // "COMPRESSED" as a sign that this handler was executed. In the real rsync
  // algorithm, this is used to decompress data.
  absl::Status Read(void* out_buffer, size_t out_size, size_t* bytes_read,
                    bool* eof) override {
    absl::Status status = socket_->Receive(
        out_buffer, out_size, /*allow_partial_read=*/false, bytes_read);
    if (!status.ok()) {
      return WrapStatus(status, "socket_->Receive() failed");
    }
    assert(*bytes_read == out_size);
    char* char_buffer = static_cast<char*>(out_buffer);
    char* pos = strstr(char_buffer, "compressed");
    if (pos) {
      memcpy(pos, "COMPRESSED", strlen("COMPRESSED"));
    }
    *eof = strstr(char_buffer, "set_eof") != nullptr;
    return absl::OkStatus();
  };

 private:
  Socket* socket_;
};

TEST_F(MessagePumpTest, RedirectInput) {
  fake_compressed_input_reader_ =
      std::make_unique<FakeCompressedInputReader>(&fake_socket_);

  TestRequest test_request;
  ToggleCompressionRequest compression_request;

  test_request.set_message("uncompressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  // Once this message is received, |fake_compressed_input_reader_| is set by
  // ThreadPackageReceived().
  EXPECT_OK(message_pump_.SendMessage(PacketType::kToggleCompression,
                                      compression_request));

  // Send a "compressed" message (should be converted to upper case).
  test_request.set_message("compressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  // Trigger reset of the input reader.
  test_request.set_message("set_eof");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  // The next message should be "uncompressed" (lower case) again.
  test_request.set_message("uncompressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "uncompressed");

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kToggleCompression,
                                         &compression_request));

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "COMPRESSED");

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "set_eof");

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "uncompressed");
}

TEST_F(MessagePumpTest, RedirectOutput) {
  // Doesn't actually do compression, just replaces the word "compressed" by
  // "COMPRESSED" as a sign that this handler was executed. In the real rsync
  // algorithm, this handler would pipe the data through zstd to compress it.
  auto fake_compressed_output_handler = [this](const void* data, size_t size) {
    std::string char_buffer(static_cast<const char*>(data), size);
    std::string::size_type pos = char_buffer.find("compressed");
    if (pos != std::string::npos) {
      char_buffer.replace(pos, strlen("COMPRESSED"), "COMPRESSED");
    }
    return fake_socket_.Send(char_buffer.data(), size);
  };

  TestRequest test_request;
  ToggleCompressionRequest compression_request;

  test_request.set_message("uncompressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  // Set output handler.
  message_pump_.RedirectOutput(fake_compressed_output_handler);

  // Send a "compressed" message (should be converted to upper case).
  test_request.set_message("compressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  // Clear output handler again.
  message_pump_.RedirectOutput(MessagePump::OutputHandler());

  // The next message should be "uncompressed" (lower case) again.
  test_request.set_message("uncompressed");
  EXPECT_OK(message_pump_.SendMessage(PacketType::kTest, test_request));

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "uncompressed");

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "COMPRESSED");

  EXPECT_OK(message_pump_.ReceiveMessage(PacketType::kTest, &test_request));
  EXPECT_EQ(test_request.message(), "uncompressed");
}

}  // namespace
}  // namespace cdc_ft
