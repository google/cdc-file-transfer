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

#include <thread>

#include "common/log.h"
#include "common/status.h"
#include "common/status_macros.h"

namespace cdc_ft {
namespace {

// If the compressor gets less data than 1 buffer (128k) every 500 ms, then
// trigger a flush. This happens when files with no changes are diff'ed (this
// produces very low volume data). Flushing prevents that the server gets stale
// and becomes overwhelmed later.
constexpr absl::Duration kDefaultAutoFlushPeriod = absl::Milliseconds(500);

}  // namespace

ZstdStream::ZstdStream(Socket* socket, int level, uint32_t num_threads)
    : socket_(socket),
      cctx_(nullptr),
      auto_flush_period_(kDefaultAutoFlushPeriod) {
  status_ = WrapStatus(Initialize(level, num_threads),
                       "Failed to initialize stream compressor");
}

ZstdStream::~ZstdStream() {
  {
    absl::MutexLock lock(&mutex_);
    shutdown_ = true;
  }
  if (compressor_thread_.joinable()) {
    compressor_thread_.join();
  }

  if (cctx_) {
    ZSTD_freeCCtx(cctx_);
    cctx_ = nullptr;
  }
}

absl::Status ZstdStream::Write(const void* data, size_t size) {
  absl::MutexLock lock(&mutex_);
  if (!status_.ok()) return status_;

  size_t data_bytes_left = size;
  const char* data_ptr = static_cast<const char*>(data);
  while (data_bytes_left > 0) {
    // Wait until the compressor thread has consumed data from |in_buffer_|.
    auto cond = [&]() {
      return shutdown_ || in_buffer_.size() < in_buffer_.capacity() ||
             !status_.ok();
    };
    mutex_.Await(absl::Condition(&cond));
    if (shutdown_) return MakeStatus("Compression stream was shut down");
    if (!status_.ok()) return status_;

    // Copy data to input buffer.
    size_t free_in_buffer_bytes = in_buffer_.capacity() - in_buffer_.size();
    const size_t to_copy = std::min(data_bytes_left, free_in_buffer_bytes);
    in_buffer_.append(data_ptr, to_copy);
    data_bytes_left -= to_copy;
    free_in_buffer_bytes -= to_copy;
    data_ptr += to_copy;
  }
  return absl::OkStatus();
}

absl::Status ZstdStream::Finish() {
  absl::MutexLock lock(&mutex_);
  if (!status_.ok()) return status_;

  last_chunk_ = true;
  last_chunk_sent_ = false;

  // Wait until data is flushed.
  auto cond = [&]() { return shutdown_ || last_chunk_sent_ || !status_.ok(); };
  mutex_.Await(absl::Condition(&cond));
  if (shutdown_) return MakeStatus("Compression stream was shut down");
  return status_;
}

absl::Status ZstdStream::Initialize(int level, uint32_t num_threads) {
  cctx_ = ZSTD_createCCtx();
  if (!cctx_) {
    return MakeStatus("Failed to create compression context");
  }

  size_t res = ZSTD_CCtx_setParameter(cctx_, ZSTD_c_compressionLevel, level);
  if (ZSTD_isError(res)) {
    return MakeStatus("Failed to set compression level: %s",
                      ZSTD_getErrorName(res));
  }

  // This fails if ZStd was not compiled with -DZSTD_MULTITHREAD.
  res = ZSTD_CCtx_setParameter(cctx_, ZSTD_c_nbWorkers, num_threads);
  if (ZSTD_isError(res)) {
    return MakeStatus("Failed to set number of worker threads: %s",
                      ZSTD_getErrorName(res));
  }

  {
    absl::MutexLock lock(&mutex_);
    in_buffer_.reserve(ZSTD_CStreamInSize());
  }

  compressor_thread_ = std::thread([this]() { ThreadCompressorMain(); });

  return absl::OkStatus();
}

void ZstdStream::ThreadCompressorMain() {
  std::vector<uint8_t> out_buffer;
  out_buffer.resize(ZSTD_CStreamOutSize());

  absl::MutexLock lock(&mutex_);
  while (!shutdown_) {
    // Wait for input data.
    auto cond = [&]() {
      return shutdown_ || last_chunk_ ||
             in_buffer_.size() == in_buffer_.capacity();
    };
    bool flush =
        !mutex_.AwaitWithTimeout(absl::Condition(&cond), auto_flush_period_);
    if (shutdown_) {
      return;
    }

    // If data arrives at a very slow rate (<1 buffer per kMinCompressPeriod),
    // then flush the compression pipes.
    const ZSTD_EndDirective mode = last_chunk_ ? ZSTD_e_end
                                   : flush     ? ZSTD_e_flush
                                               : ZSTD_e_continue;
    LOG_VERBOSE("Compressing %u bytes (mode=%s)", in_buffer_.size(),
                mode == ZSTD_e_end     ? "end"
                : mode == ZSTD_e_flush ? "flush"
                                       : "continue");
    ZSTD_inBuffer input = {in_buffer_.data(), in_buffer_.size(), 0};
    bool finished = false;
    do {
      ZSTD_outBuffer output = {out_buffer.data(), out_buffer.size(), 0};
      size_t remaining = ZSTD_compressStream2(cctx_, &output, &input, mode);
      if (ZSTD_isError(remaining)) {
        status_ = MakeStatus("Failed to compress data: %s",
                             ZSTD_getErrorName(remaining));
        return;
      }

      if (output.pos > 0) {
        status_ = socket_->Send(output.dst, output.pos);
        if (!status_.ok()) return;
      }

      finished = mode != ZSTD_e_continue ? (remaining == 0)
                                         : (input.pos == input.size);
    } while (!finished);

    if (last_chunk_) {
      last_chunk_ = false;
      last_chunk_sent_ = true;
    }

    // zstd should only return 0 when the input is consumed.
    assert(input.pos == input.size);
    in_buffer_.clear();
  }
}

}  // namespace cdc_ft
