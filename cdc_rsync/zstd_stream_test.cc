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

#include "cdc_rsync/zstd_stream.h"

#include "cdc_rsync/base/fake_socket.h"
#include "cdc_rsync_server/unzstd_stream.h"
#include "common/status_test_macros.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

class ZstdStreamTest : public ::testing::Test {
 protected:
  FakeSocket socket_;
  ZstdStream cstream_{&socket_, /*level=*/6, /*num_threads=*/8};
  UnzstdStream dstream_{&socket_};
};

TEST_F(ZstdStreamTest, Small) {
  const std::string want = "Lorem ipsum gibberisulum foobarberis";
  EXPECT_OK(cstream_.Write(want.data(), want.size()));
  EXPECT_OK(cstream_.Flush());

  Buffer buff(1024);
  size_t bytes_read;
  bool eof = false;
  EXPECT_OK(dstream_.Read(buff.data(), buff.size(), &bytes_read, &eof));
  EXPECT_TRUE(eof);
  std::string got(buff.data(), bytes_read);
  EXPECT_EQ(got, want);
}

TEST_F(ZstdStreamTest, Large) {
  Buffer want(1024 * 1024 * 10 + 12345);
  constexpr uint64_t prime = 919393;
  for (size_t n = 0; n < want.size(); ++n) {
    want.data()[n] = ((n * prime) % 26) + 'a';
  }

  constexpr int kChunkSize = 19 * 1024;
  for (size_t pos = 0; pos < want.size(); pos += kChunkSize) {
    size_t size = std::min<size_t>(kChunkSize, want.size() - pos);
    EXPECT_OK(cstream_.Write(want.data() + pos, size));
  }
  EXPECT_OK(cstream_.Flush());

  bool eof = false;
  Buffer buff(128 * 1024);
  Buffer got;
  while (!eof) {
    size_t bytes_read;
    EXPECT_OK(dstream_.Read(buff.data(), buff.size(), &bytes_read, &eof));
    got.append(buff.data(), bytes_read);
  }
  EXPECT_EQ(want, got);
}

}  // namespace
}  // namespace cdc_ft
