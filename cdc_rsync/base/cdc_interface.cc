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

#include <vector>

#include "absl/strings/str_format.h"
#include "blake3.h"
#include "cdc_rsync/base/message_pump.h"
#include "cdc_rsync/protos/messages.pb.h"
#include "common/buffer.h"
#include "common/path.h"
#include "common/status.h"
#include "common/util.h"
#include "fastcdc/fastcdc.h"

#if PLATFORM_LINUX
#include <fcntl.h>
#endif

namespace cdc_ft {
namespace {

// The average chunk size should be as low as possible, but not too low.
// Lower sizes mean better delta-encoding and hence less data uploads.
// However, chunking becomes slower for lower sizes. At 8 KB, a gamelet can
// still process close to 700 MB/sec, which matches hard drive speed.
// Signature data rate is another factor. The gamelet generates signature data
// at a rate of 700 MB/sec / kAvgChunkSize * sizeof(Chunk) = 1.7 MB/sec for 8 KB
// chunks. That means, the client needs at least 16 MBit download bandwidth to
// stream signatures or else this part becomes slower. 4 KB chunks would require
// a 32 MBit connection.
constexpr size_t kAvgChunkSize = 8 * 1024;
constexpr size_t kMinChunkSize = kAvgChunkSize / 2;
constexpr size_t kMaxChunkSize = kAvgChunkSize * 4;

// This number was found by experimentally optimizing chunking throughput.
constexpr size_t kFileIoBufferSize = kMaxChunkSize * 4;

// Limits the size of contiguous patch chunks where data is copied from the
// basis file. Necessary since the server copies chunks in one go and doesn't
// split them up (would be possible, but unnecessarily complicates code).
constexpr size_t kCombinedChunkSizeThreshold = 64 * 1024;

// Number of hashing tasks in flight at a given point of time.
constexpr size_t kMaxNumHashTasks = 64;

#pragma pack(push, 1)
// 16 byte hashes guarantee a sufficiently low chance of hash collisions. For
// 8 byte the chance of a hash collision is actually quite high for large files
// 0.0004% for a 100 GB file and 8 KB chunks.
struct Hash {
  uint64_t low;
  uint64_t high;

  bool operator==(const Hash& other) const {
    return low == other.low && high == other.high;
  }
  bool operator!=(const Hash& other) const { return !(*this == other); }
};
#pragma pack(pop)

static_assert(sizeof(Hash) <= BLAKE3_OUT_LEN, "");

}  // namespace
}  // namespace cdc_ft

namespace std {

template <>
struct hash<cdc_ft::Hash> {
  size_t operator()(const cdc_ft::Hash& hash) const { return hash.low; }
};

}  // namespace std

namespace cdc_ft {
namespace {

// Send a batch of signatures every 8 MB of processed data (~90 packets per
// second at 700 MB/sec processing rate). The size of each signature batch is
// kMinNumChunksPerBatch * sizeof(Chunk), e.g. 20 KB for an avg chunk size of
// 8 KB.
constexpr int kMinSigBatchDataSize = 8 * 1024 * 1024;
constexpr int kMinNumChunksPerBatch = kMinSigBatchDataSize / kAvgChunkSize;

// Send patch commands in batches of at least that size for efficiency.
constexpr int kPatchRequestSizeThreshold = 65536;

// 16 bytes hash, 4 bytes size = 20 bytes.
struct Chunk {
  Hash hash;
  uint32_t size = 0;
  Chunk(const Hash& hash, uint32_t size) : hash(hash), size(size) {}
};

Hash ComputeHash(const void* data, size_t size) {
  assert(data);
  Hash hash;
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, data, size);
  blake3_hasher_finalize(&hasher, reinterpret_cast<uint8_t*>(&hash),
                         sizeof(hash));
  return hash;
}

// Task that computes hashes for a single chunk and adds the result to
// AddSignaturesResponse.
class HashTask : public Task {
 public:
  HashTask() {}
  ~HashTask() {}

  HashTask(const HashTask& other) = delete;
  HashTask& operator=(HashTask&) = delete;

  // Sets the data to compute the hash of.
  // Should be called before queuing the task.
  void SetData(const void* data, size_t size) {
    buffer_.reserve(size);
    buffer_.resize(size);
    memcpy(buffer_.data(), data, size);
  }

  // Appends the computed hash to |response|.
  // Should be called once the task is finished.
  void AppendHash(AddSignaturesResponse* response) const {
    response->add_sizes(static_cast<uint32_t>(buffer_.size()));
    std::string* hashes = response->mutable_hashes();
    hashes->append(reinterpret_cast<const char*>(&hash_), sizeof(hash_));
  }

  void ThreadRun(IsCancelledPredicate is_cancelled) override {
    hash_ = ComputeHash(buffer_.data(), buffer_.size());
  }

 private:
  Buffer buffer_;
  struct Hash hash_ = {0};
};

class ServerChunkReceiver {
 public:
  explicit ServerChunkReceiver(MessagePump* message_pump)
      : message_pump_(message_pump) {
    assert(message_pump_);
  }

  // Receives server signature packets and places the data into a map
  // (chunk hash) -> (server-side file offset).
  // If |block| is false, returns immediately if no data is available.
  // If |block| is true, blocks until some data is available.
  // |num_server_bytes_processed| is set to the total size of the chunks
  // received.
  absl::Status Receive(bool block, uint64_t* num_server_bytes_processed) {
    assert(num_server_bytes_processed);
    *num_server_bytes_processed = 0;

    // Already all server chunks received?
    if (all_chunks_received_) {
      return absl::OkStatus();
    }

    // If no data is available, early out (unless blocking is requested).
    if (!block && !message_pump_->CanReceive()) {
      return absl::OkStatus();
    }

    // Receive signatures.
    AddSignaturesResponse response;
    absl::Status status =
        message_pump_->ReceiveMessage(PacketType::kAddSignatures, &response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to receive AddSignaturesResponse");
    }

    // Validate size of packed hashes, just in case.
    const int num_chunks = response.sizes_size();
    if (response.hashes().size() != num_chunks * sizeof(Hash)) {
      return MakeStatus("Bad hashes size. Expected %u. Actual %u.",
                        num_chunks * sizeof(Hash), response.hashes().size());
    }

    // An empty packet marks the end of the server chunks.
    if (num_chunks == 0) {
      all_chunks_received_ = true;
      return absl::OkStatus();
    }

    // Copy the data over to |server_chunk_offsets|.
    const Hash* hashes =
        reinterpret_cast<const Hash*>(response.hashes().data());
    for (int n = 0; n < num_chunks; ++n) {
      uint32_t size = response.sizes(n);
      chunk_offsets_.insert({hashes[n], curr_offset_});
      curr_offset_ += size;
      *num_server_bytes_processed += size;
    }

    return absl::OkStatus();
  }

  // True if all server chunks have been received.
  bool AllChunksReceived() const { return all_chunks_received_; }

  // Returns a map (server chunk hash) -> (offset of that chunk in server file).
  const std::unordered_map<Hash, uint64_t>& ChunkOffsets() const {
    return chunk_offsets_;
  }

 private:
  MessagePump* message_pump_;

  // Maps server chunk hashes to the file offset in the server file.
  std::unordered_map<Hash, uint64_t> chunk_offsets_;

  // Current server file offset.
  uint64_t curr_offset_ = 0;

  // Whether all server files have been received.
  bool all_chunks_received_ = false;
};

class PatchSender {
  // 1 byte for source, 8 bytes for offset and 4 bytes for size.
  static constexpr size_t kPatchMetadataSize =
      sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t);

 public:
  PatchSender(FILE* file, MessagePump* message_pump)
      : file_(file), message_pump_(message_pump) {}

  // Tries to send patch data for the next chunk in |client_chunks|. The class
  // keeps an internal counter for the current chunk index. Patch data is not
  // sent if the current client chunk is not found among the server chunks and
  // there are outstanding server chunks. In that case, the method returns
  // with an OK status and should be called later as soon as additional server
  // chunks have been received.
  // |num_client_bytes_processed| is set to the total size of the chunks added.
  absl::Status TryAddChunks(const std::vector<Chunk>& client_chunks,
                            const ServerChunkReceiver& server_chunk_receiver,
                            uint64_t* num_client_bytes_processed) {
    assert(num_client_bytes_processed);
    *num_client_bytes_processed = 0;

    while (curr_chunk_idx_ < client_chunks.size()) {
      const Chunk& chunk = client_chunks[curr_chunk_idx_];
      auto it = server_chunk_receiver.ChunkOffsets().find(chunk.hash);
      bool exists = it != server_chunk_receiver.ChunkOffsets().end();

      // If there are outstanding server chunks and the client hash is not
      // found, do not send the patch data yet. A future server chunk might
      // contain the data.
      if (!exists && !server_chunk_receiver.AllChunksReceived()) {
        return absl::OkStatus();
      }

      absl::Status status = exists ? AddExistingChunk(it->second, chunk.size)
                                   : AddNewChunk(chunk.size);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to add chunk");
      }

      ++curr_chunk_idx_;
      *num_client_bytes_processed += chunk.size;

      // Break loop if all server chunks are received. Otherwise, progress
      // reporting is blocked.
      if (server_chunk_receiver.AllChunksReceived()) {
        break;
      }
    }

    return absl::OkStatus();
  }

  // Sends the remaining patch commands and an EOF marker.
  absl::Status Flush() {
    if (request_size_ > 0) {
      absl::Status status =
          message_pump_->SendMessage(PacketType::kAddPatchCommands, request_);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to send final patch commands");
      }
      total_request_size_ += request_size_;
      request_.Clear();
    }

    // Send an empty patch commands request as EOF marker.
    absl::Status status =
        message_pump_->SendMessage(PacketType::kAddPatchCommands, request_);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send patch commands EOF marker");
    }

    return absl::OkStatus();
  }

  // Returns the (estimated) total size of all patch data sent.
  uint64_t GetTotalRequestSize() const { return total_request_size_; }

  // Index of the next client chunk.
  size_t CurrChunkIdx() const { return curr_chunk_idx_; }

 private:
  // Adds patch data for a client chunk that has a matching server chunk of
  // given |size| at given |offset| in the server file.
  absl::Status AddExistingChunk(uint64_t offset, uint32_t size) {
    int last_idx = request_.sources_size() - 1;
    if (last_idx >= 0 &&
        request_.sources(last_idx) ==
            AddPatchCommandsRequest::SOURCE_BASIS_FILE &&
        request_.offsets(last_idx) + request_.sizes(last_idx) == offset &&
        request_.sizes(last_idx) < kCombinedChunkSizeThreshold) {
      // Same source and contiguous data -> Append to last entry.
      request_.set_sizes(last_idx, request_.sizes(last_idx) + size);
    } else {
      // Different source or first chunk -> Create new entry.
      request_.add_sources(AddPatchCommandsRequest::SOURCE_BASIS_FILE);
      request_.add_offsets(offset);
      request_.add_sizes(size);
      request_size_ += kPatchMetadataSize;
    }

    return OnChunkAdded(size);
  }

  absl::Status AddNewChunk(uint32_t size) {
    std::string* data = request_.mutable_data();
    int last_idx = request_.sources_size() - 1;
    if (last_idx >= 0 &&
        request_.sources(last_idx) == AddPatchCommandsRequest::SOURCE_DATA) {
      // Same source -> Append to last entry.
      request_.set_sizes(last_idx, request_.sizes(last_idx) + size);
    } else {
      // Different source or first chunk -> Create new entry.
      request_.add_sources(AddPatchCommandsRequest::SOURCE_DATA);
      request_.add_offsets(data->size());
      request_.add_sizes(size);
      request_size_ += kPatchMetadataSize;
    }

    // Read data from client file into |data|. Be sure to restore the previous
    // file offset as the chunker might still be processing the file.
    size_t prev_size = data->size();
    data->resize(prev_size + size);
    int64_t prev_offset = ftell64(file_);
    if (fseek64(file_, file_offset_, SEEK_SET) != 0 ||
        fread(&(*data)[prev_size], 1, size, file_) != size ||
        fseek64(file_, prev_offset, SEEK_SET) != 0) {
      return MakeStatus("Failed to read %u bytes at offset %u", size,
                        file_offset_);
    }
    request_size_ += size;

    return OnChunkAdded(size);
  }

  absl::Status OnChunkAdded(uint32_t size) {
    file_offset_ += size;

    // Send patch commands if there's enough data.
    if (request_size_ > kPatchRequestSizeThreshold) {
      absl::Status status =
          message_pump_->SendMessage(PacketType::kAddPatchCommands, request_);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to send patch commands");
      }
      total_request_size_ += request_size_;
      request_size_ = 0;
      request_.Clear();
    }

    return absl::OkStatus();
  }

  FILE* file_;
  MessagePump* message_pump_;

  AddPatchCommandsRequest request_;
  size_t request_size_ = 0;
  size_t total_request_size_ = 0;
  uint64_t file_offset_ = 0;
  size_t curr_chunk_idx_ = 0;
};

}  // namespace

CdcInterface::CdcInterface(MessagePump* message_pump)
    : message_pump_(message_pump) {}

absl::Status CdcInterface::CreateAndSendSignature(const std::string& filepath) {
  absl::StatusOr<FILE*> file = path::OpenFile(filepath, "rb");
  if (!file.ok()) {
    return file.status();
  }
#if PLATFORM_LINUX
  // Tell the kernel we'll load the file sequentially (improves IO bandwidth).
  posix_fadvise(fileno(*file), 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

  // Use a background thread for computing hashes on the server.
  // Allocate lazily since it is not needed on the client.
  // MUST NOT use more than 1 worker thread since the order of finished tasks
  // would then not necessarily match the pushing order. However, the order is
  // important for computing offsets.
  if (!hash_pool_) hash_pool_ = std::make_unique<Threadpool>(1);

  // |chunk_handler| is called for each CDC chunk. It pushes a hash task to the
  // pool. Tasks are "recycled" from |free_tasks_|, so that buffers don't have
  // to reallocated constantly.
  size_t num_hash_tasks = 0;
  auto chunk_handler = [pool = hash_pool_.get(), &num_hash_tasks,
                        free_tasks = &free_tasks_](const void* data,
                                                   size_t size) {
    ++num_hash_tasks;
    if (free_tasks->empty()) {
      free_tasks->push_back(std::make_unique<HashTask>());
    }
    std::unique_ptr<Task> task = std::move(free_tasks->back());
    free_tasks->pop_back();
    static_cast<HashTask*>(task.get())->SetData(data, size);
    pool->QueueTask(std::move(task));
  };

  fastcdc::Config config(kMinChunkSize, kAvgChunkSize, kMaxChunkSize);
  fastcdc::Chunker chunker(config, chunk_handler);

  AddSignaturesResponse response;
  auto read_handler = [&chunker, &response, pool = hash_pool_.get(),
                       &num_hash_tasks, free_tasks = &free_tasks_,
                       message_pump = message_pump_](const void* data,
                                                     size_t size) {
    chunker.Process(static_cast<const uint8_t*>(data), size);

    // Finish hashing tasks. Block if there are too many of them in flight.
    for (;;) {
      std::unique_ptr<Task> task = num_hash_tasks >= kMaxNumHashTasks
                                       ? pool->GetCompletedTask()
                                       : pool->TryGetCompletedTask();
      if (!task) break;
      num_hash_tasks--;
      static_cast<HashTask*>(task.get())->AppendHash(&response);
      free_tasks->push_back(std::move(task));
    }

    // Send data if we have enough chunks.
    if (response.sizes_size() >= kMinNumChunksPerBatch) {
      absl::Status status =
          message_pump->SendMessage(PacketType::kAddSignatures, response);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to send signatures");
      }
      response.Clear();
    }

    return absl::OkStatus();
  };

  absl::Status status =
      path::StreamReadFileContents(*file, kFileIoBufferSize, read_handler);
  fclose(*file);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to compute signatures");
  }
  chunker.Finalize();

  // Finish hashing tasks.
  hash_pool_->Wait();
  std::unique_ptr<Task> task = hash_pool_->TryGetCompletedTask();
  while (task) {
    static_cast<HashTask*>(task.get())->AppendHash(&response);
    free_tasks_.push_back(std::move(task));
    task = hash_pool_->TryGetCompletedTask();
  }

  // Send the remaining chunks, if any.
  if (response.sizes_size() > 0) {
    status = message_pump_->SendMessage(PacketType::kAddSignatures, response);
    if (!status.ok()) {
      return WrapStatus(status, "Failed to send final signatures");
    }
    response.Clear();
  }

  // Send an empty response as EOF marker.
  status = message_pump_->SendMessage(PacketType::kAddSignatures, response);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to send signatures EOF marker");
  }

  return absl::OkStatus();
}

absl::Status CdcInterface::ReceiveSignatureAndCreateAndSendDiff(
    FILE* file, ReportCdcProgress* progress) {
  //
  // Compute signatures from client |file| and send patches while receiving
  // server signatures.
  //
  std::vector<Chunk> client_chunks;
  ServerChunkReceiver server_chunk_receiver(message_pump_);
  PatchSender patch_sender(file, message_pump_);

  auto chunk_handler = [&client_chunks](const void* data, size_t size) {
    client_chunks.emplace_back(ComputeHash(data, size),
                               static_cast<uint32_t>(size));
  };

  fastcdc::Config config(kMinChunkSize, kAvgChunkSize, kMaxChunkSize);
  fastcdc::Chunker chunker(config, chunk_handler);

  uint64_t file_size = 0;
  auto read_handler = [&chunker, &client_chunks, &server_chunk_receiver,
                       &file_size, progress,
                       &patch_sender](const void* data, size_t size) {
    // Process client chunks for the data read.
    chunker.Process(static_cast<const uint8_t*>(data), size);
    file_size += size;

    const bool all_client_chunks_read = data == nullptr;
    if (all_client_chunks_read) {
      chunker.Finalize();
    }

    do {
      // Receive any server chunks available.
      uint64_t num_server_bytes_processed = 0;
      absl::Status status = server_chunk_receiver.Receive(
          /*block=*/all_client_chunks_read, &num_server_bytes_processed);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to receive server chunks");
      }

      // Try to send patch data.
      uint64_t num_client_bytes_processed = 0;
      status = patch_sender.TryAddChunks(client_chunks, server_chunk_receiver,
                                         &num_client_bytes_processed);
      if (!status.ok()) {
        return WrapStatus(status, "Failed to send patch data");
      }

      progress->ReportSyncProgress(num_client_bytes_processed,
                                   num_server_bytes_processed);
    } while (all_client_chunks_read &&
             (!server_chunk_receiver.AllChunksReceived() ||
              patch_sender.CurrChunkIdx() < client_chunks.size()));

    return absl::OkStatus();
  };

  absl::Status status =
      path::StreamReadFileContents(file, kFileIoBufferSize, read_handler);
  if (!status.ok()) {
    return WrapStatus(status, "Failed to stream file");
  }

  // Should have sent all client chunks by now.
  assert(patch_sender.CurrChunkIdx() == client_chunks.size());

  // Flush remaining patches.
  status = patch_sender.Flush();
  if (!status.ok()) {
    return WrapStatus(status, "Failed to flush patches");
  }

  return absl::OkStatus();
}

absl::Status CdcInterface::ReceiveDiffAndPatch(
    const std::string& basis_filepath, FILE* patched_file,
    bool* is_executable) {
  Buffer buffer;
  *is_executable = false;

  absl::StatusOr<FILE*> basis_file = path::OpenFile(basis_filepath, "rb");
  if (!basis_file.ok()) {
    return basis_file.status();
  }
#if PLATFORM_LINUX
  // Tell the kernel we'll load the file sequentially (improves IO bandwidth).
  // It is not strictly true that the basis file is accessed sequentially, but
  // for larger parts of this file this should be the case.
  posix_fadvise(fileno(*basis_file), 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

  bool first_chunk = true;
  for (;;) {
    AddPatchCommandsRequest request;
    absl::Status status =
        message_pump_->ReceiveMessage(PacketType::kAddPatchCommands, &request);
    if (!status.ok()) {
      fclose(*basis_file);
      return WrapStatus(status, "Failed to receive AddPatchCommandsRequest");
    }

    // All arrays must be of the same size.
    int num_chunks = request.sources_size();
    if (num_chunks != request.offsets_size() ||
        num_chunks != request.sizes_size()) {
      fclose(*basis_file);
      return MakeStatus(
          "Corrupted patch command arrays: Expected sizes %i. Actual %i/%i.",
          num_chunks, request.offsets_size(), request.sizes_size());
    }

    if (num_chunks == 0) {
      // A zero-size request marks the end of patch commands.
      break;
    }

    for (int n = 0; n < num_chunks; ++n) {
      AddPatchCommandsRequest::Source source = request.sources(n);
      uint64_t chunk_offset = request.offsets(n);
      uint32_t chunk_size = request.sizes(n);

      const char* chunk_data = nullptr;
      if (source == AddPatchCommandsRequest::SOURCE_BASIS_FILE) {
        // Copy [chunk_offset, chunk_offset + chunk_size) from |basis_file|.
        buffer.resize(chunk_size);
        if (fseek64(*basis_file, chunk_offset, SEEK_SET) != 0 ||
            fread(buffer.data(), 1, chunk_size, *basis_file) != chunk_size) {
          fclose(*basis_file);
          return MakeStatus(
              "Failed to read %u bytes at offset %u from basis file",
              chunk_size, chunk_offset);
        }
        chunk_data = buffer.data();
      } else {
        // Write [chunk_offset, chunk_offset + chunk_size) from request data.
        assert(source == AddPatchCommandsRequest::SOURCE_DATA);
        if (request.data().size() < chunk_offset + chunk_size) {
          fclose(*basis_file);
          return MakeStatus(
              "Insufficient data in patch commands. Required %u. Actual %u.",
              chunk_offset + chunk_size, request.data().size());
        }
        chunk_data = &request.data()[chunk_offset];
      }

      if (first_chunk && chunk_size > 0) {
        first_chunk = false;
        *is_executable = Util::IsExecutable(chunk_data, chunk_size);
      }
      if (fwrite(chunk_data, 1, chunk_size, patched_file) != chunk_size) {
        fclose(*basis_file);
        return MakeStatus("Failed to write %u bytes to patched file",
                          chunk_size);
      }
    }
  }
  fclose(*basis_file);

  return absl::OkStatus();
}

}  // namespace cdc_ft
