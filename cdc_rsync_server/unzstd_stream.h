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

#ifndef CDC_RSYNC_SERVER_UNZSTD_STREAM_H_
#define CDC_RSYNC_SERVER_UNZSTD_STREAM_H_

#include "absl/status/status.h"
#include "cdc_rsync/base/message_pump.h"
#include "lib/zstd.h"

namespace cdc_ft {

class Socket;

// Streaming decompression using zstd.
class UnzstdStream : public MessagePump::InputReader {
 public:
  explicit UnzstdStream(Socket* socket);
  virtual ~UnzstdStream();

  // MessagePump::InputReader:
  absl::Status Read(void* out_buffer, size_t out_size, size_t* bytes_read,
                    bool* eof) override;

 private:
  absl::Status Initialize();

  Socket* const socket_;
  std::vector<uint8_t> in_buffer_;
  ZSTD_inBuffer input_;
  ZSTD_DCtx* dctx_;
  absl::Status init_status_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_SERVER_UNZSTD_STREAM_H_
