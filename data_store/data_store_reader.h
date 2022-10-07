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

#ifndef DATA_STORE_DATA_STORE_READER_H_
#define DATA_STORE_DATA_STORE_READER_H_

#include <vector>

#include "absl/status/statusor.h"
#include "common/buffer.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "manifest/content_id.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// Describes which part of a chunk needs to copied into a given buffer.
struct ChunkTransferTask {
  ChunkTransferTask() {}
  ChunkTransferTask(ContentIdProto id, uint64_t offset, void* data,
                    uint64_t size)
      : id(std::move(id)), offset(offset), data(data), size(size) {}
  // Identifies the chunk.
  ContentIdProto id;
  // Relative offset into the chunk from where data should be copied.
  uint64_t offset = 0;
  // Data buffer into which the chunk is written. May be null for prefetching.
  void* data = nullptr;
  // Size of the |data| buffer. May be zero for prefetching.
  uint64_t size = 0;
  // If the storage layer fetches the complete chunk data, it can be moved into
  // this string so that the data provider layer can cache the chunk.
  std::string chunk_data;
  // Indicates if the chunk was successfully copied into |data| or prefetched.
  bool done = false;
};

// A std::vector of ChunkTransferTask elements.
class ChunkTransferList : public std::vector<ChunkTransferTask> {
 public:
  // Returns true if all tasks with a non-zero size have |done| set to true.
  bool ReadDone() const;

  // Returns true if all tasks have |done| set to true, including those only
  // meant for prefetching (|size| == 0).
  bool PrefetchDone() const;

  // Returns a comma separated string of hex IDs of all chunks in this list. If
  // the optional function |filter| is given, only those chunks are included for
  // which |filter| returns true.
  std::string ToHexString(
      std::function<bool(const ChunkTransferTask&)> filter = nullptr) const;

  // Same as ToHexString, but only includes tasks having |done| set to false.
  std::string UndoneToHexString() const;
};

// DataStoreReader is an abstract interface to read from all data stores used
// for the file transfer, for example: a local cache, a data store, which
// receives data via a gRPC channel, etc.
class DataStoreReader {
 public:
  DataStoreReader() = default;
  virtual ~DataStoreReader() = default;

  DataStoreReader(const DataStoreReader&) = delete;
  DataStoreReader& operator=(const DataStoreReader&) = delete;

  // Suggests a data prefetch size based on the given |read_size|. The default
  // implementation just returns |read_size|. Override this function to
  // implement a prefetching strategy.
  virtual size_t PrefetchSize(size_t read_size) const;

  // Reads |size| bytes from the chunk specified by |content_id|, starting
  // at the given |offset|, and writes the result into |data|.
  // The return value is the number of read bytes.
  // If the chunk is not found in the data store, returns NotFoundError.
  virtual absl::StatusOr<size_t> Get(const ContentIdProto& content_id,
                                     void* data, size_t offset,
                                     size_t size) = 0;

  // Reads all chunks from the given task list |chunks| that are not done yet,
  // copies the data into the associated buffer, and marks the chunk as done. If
  // the reader fetches the full chunk, the raw data may be moved to the task as
  // well for caching.
  //
  // Returns success even if no chunk was found. Check |chunks->ReadDone()| or
  // |chunks->PrefetchDone()| to verify all chunks were fetched. Returns any
  // error other than absl::NotFoundError from the underlying implementation.
  //
  // The default implementation calls the single item `Get()` method for each
  // task in |chunks|. Override this method in a sub-class for optimized batch
  // processing.
  virtual absl::Status Get(ChunkTransferList* chunks);

  // Reads the complete data chunk specified by |content_id| and writes the
  // result into |data|.
  // If the chunk is not found in the data store, returns NotFoundError.
  virtual absl::Status Get(const ContentIdProto& content_id, Buffer* data) = 0;

  // Reads the complete chunk identified by |content_id| and parses it as the
  // given protocol buffer.
  absl::Status GetProto(const ContentIdProto& content_id,
                        google::protobuf::Message* proto);

  // Reads the complete chunk identified by |content_id| and parses it as the
  // given protocol buffer. Uses the given Buffer |buf| as intermediate
  // storage.
  absl::Status GetProto(const ContentIdProto& content_id, Buffer* buf,
                        google::protobuf::Message* proto);
};  // class DataStoreReader

}  // namespace cdc_ft

#endif  // DATA_STORE_DATA_STORE_READER_H_
