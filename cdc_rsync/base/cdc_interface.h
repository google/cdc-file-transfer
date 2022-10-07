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

#ifndef CDC_RSYNC_BASE_CDC_INTERFACE_H_
#define CDC_RSYNC_BASE_CDC_INTERFACE_H_

#include <string>

#include "absl/status/status.h"
#include "common/threadpool.h"

namespace cdc_ft {

class MessagePump;

class ReportCdcProgress {
 public:
  virtual ~ReportCdcProgress() = default;
  virtual void ReportSyncProgress(size_t num_client_bytes_processed,
                                  size_t num_server_bytes_processed) = 0;
};

// Creates signatures, diffs and patches files. Abstraction layer for fastcdc
// chunking and blake3 hashing.
class CdcInterface {
 public:
  explicit CdcInterface(MessagePump* message_pump);

  // Creates the signature of the file at |filepath| and sends it to the socket.
  // Typically called on the server.
  absl::Status CreateAndSendSignature(const std::string& filepath);

  // Receives the server-side signature of |file| from the socket, creates diff
  // data using the signature and the file, and sends the diffs to the socket.
  // Typically called on the client.
  absl::Status ReceiveSignatureAndCreateAndSendDiff(
      FILE* file, ReportCdcProgress* progress);

  // Receives diffs from the socket and patches the file at |basis_filepath|.
  // The patched data is written to |patched_file|, which must be open in "wb"
  // mode. Sets |is_executable| to true if the patched file is an executable
  // (based on magic headers).
  // Typically called on the server.
  absl::Status ReceiveDiffAndPatch(const std::string& basis_filepath,
                                   FILE* patched_file, bool* is_executable);

 private:
  MessagePump* const message_pump_;

  // Thread pool for computing chunk hashes.
  std::unique_ptr<Threadpool> hash_pool_;

  // List of unused hash computation tasks. Tasks are reused by the hash pool
  // in order to prevent buffer reallocation.
  std::vector<std::unique_ptr<Task>> free_tasks_;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_BASE_CDC_INTERFACE_H_
