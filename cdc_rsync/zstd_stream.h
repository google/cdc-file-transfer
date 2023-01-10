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

#ifndef CDC_RSYNC_ZSTD_STREAM_H_
#define CDC_RSYNC_ZSTD_STREAM_H_

#include <thread>

#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "cdc_rsync/base/socket.h"
#include "common/buffer.h"
#include "lib/zstd.h"

namespace cdc_ft {

// Streaming compression using zstd.
class ZstdStream {
 public:
  ZstdStream(Socket* socket, int level, uint32_t num_threads);
  ~ZstdStream();

  // Sends the given |data| to the compressor.
  absl::Status Write(const void* data, size_t size) ABSL_LOCKS_EXCLUDED(mutex_);

  // Finishes the stream and flushes all remaining data.
  absl::Status Finish() ABSL_LOCKS_EXCLUDED(mutex_);

  // Flushes internal buffers if no new data is written for longer than this
  // time. This makes sure that no data is stuck in the pipeline if no new input
  // is available. Default is 500 ms.
  void AutoFlushAfter(absl::Duration dur) { auto_flush_period_ = dur; }

 private:
  // Initializes the compressor and related data.
  absl::Status Initialize(int level, uint32_t num_threads)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Compressor thread, pushes |in_buffer_| to the zstd compressor and sends
  // compressed data to the socket.
  void ThreadCompressorMain() ABSL_LOCKS_EXCLUDED(mutex_);

  Socket* const socket_;
  ZSTD_CCtx* cctx_;

  absl::Mutex mutex_;
  Buffer in_buffer_ ABSL_GUARDED_BY(mutex_);
  bool shutdown_ ABSL_GUARDED_BY(mutex_) = false;
  bool last_chunk_ ABSL_GUARDED_BY(mutex_) = false;
  bool last_chunk_sent_ ABSL_GUARDED_BY(mutex_) = false;
  absl::Status status_ ABSL_GUARDED_BY(mutex_);
  std::thread compressor_thread_;

  absl::Duration auto_flush_period_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_ZSTD_STREAM_H_
