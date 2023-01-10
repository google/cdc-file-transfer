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

#include "cdc_rsync_server/unzstd_stream.h"

#include "cdc_rsync/base/socket.h"
#include "common/status.h"

namespace cdc_ft {

UnzstdStream::UnzstdStream(Socket* socket) : socket_(socket) {
  init_status_ =
      WrapStatus(Initialize(), "Failed to initialize stream decompressor");
}

UnzstdStream::~UnzstdStream() {
  if (dctx_) {
    ZSTD_freeDCtx(dctx_);
    dctx_ = nullptr;
  }
}

absl::Status UnzstdStream::Read(void* out_buffer, size_t out_size,
                                size_t* bytes_read, bool* eof) {
  *bytes_read = 0;
  *eof = false;
  if (!init_status_.ok()) {
    return init_status_;
  }

  ZSTD_outBuffer output = {out_buffer, out_size, 0};
  while (output.pos < output.size && !*eof) {
    // Decompress.
    size_t ret = ZSTD_decompressStream(dctx_, &output, &input_);
    if (ZSTD_isError(ret)) {
      return MakeStatus("Failed to decompress data: %s",
                        ZSTD_getErrorName(ret));
    }

    *eof = (ret == 0);
    if (*eof && input_.pos < input_.size) {
      return MakeStatus("EOF with %u bytes input data available",
                        input_.size - input_.pos);
    }

    if (input_.pos == input_.size && output.pos < output.size && !*eof) {
      // Read more compressed input data.
      // Allow partial reads since the stream could end any time.
      size_t in_size;
      absl::Status status =
          socket_->Receive(in_buffer_.data(), in_buffer_.size(),
                           /*allow_partial_read=*/true, &in_size);
      if (!status.ok()) {
        return WrapStatus(status, "socket_->ReceiveEx() failed");
      }
      input_.pos = 0;
      input_.size = in_size;
    }
  }

  // Output buffer is full or eof.
  *bytes_read = output.pos;
  return absl::OkStatus();
}

absl::Status UnzstdStream::Initialize() {
  dctx_ = ZSTD_createDCtx();
  if (!dctx_) {
    return MakeStatus("Decompression context creation failed");
  }

  in_buffer_.resize(ZSTD_DStreamInSize());
  input_ = {in_buffer_.data(), 0, 0};

  return absl::OkStatus();
}

}  // namespace cdc_ft
