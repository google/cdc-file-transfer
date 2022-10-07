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

#ifndef DATA_STORE_DATA_STORE_WRITER_H_
#define DATA_STORE_DATA_STORE_WRITER_H_

#include <unordered_set>

#include "absl/status/statusor.h"
#include "common/buffer.h"
#include "data_store/data_store_reader.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// DataStoreWriter is an abstract interface for read/write operations for a data
// store, for example: a disk-based or in-memory cache.
class DataStoreWriter : public DataStoreReader {
 public:
  DataStoreWriter() = default;
  DataStoreWriter(const DataStoreWriter&) = delete;
  DataStoreWriter& operator=(const DataStoreWriter&) = delete;
  virtual ~DataStoreWriter() = default;

  // Returns true if the chunk with the given |content_id| is available
  // in the data store. Otherwise, returns false. The default implementation
  // uses Get() to retrieve the chunk and should be overridden.
  virtual bool Contains(const ContentIdProto& content_id);

  // Stores a data chunk |data| of |size| and |content_id| into the data store.
  virtual absl::Status Put(const ContentIdProto& content_id, const void* data,
                           size_t size) = 0;

  // Stores the given protocol buffer |proto| as a unique chunk and updates
  // |content_id| with the corresponding digest. If the optional parameter
  // |proto_size| is given, it will be set to the byte size of the serialized
  // proto.
  absl::Status PutProto(const google::protobuf::MessageLite& proto,
                        ContentIdProto* content_id,
                        size_t* proto_size = nullptr);

  // Removes the data chunk with |content_id| from the writer. Returns success
  // if the chunk does not exist or was removed.
  virtual absl::Status Remove(const ContentIdProto& content_id) = 0;

  // Wipes the data. All statistics and data chunks are removed from the data
  // store.
  virtual absl::Status Wipe() = 0;

  // Removes all chunks except for |ids_to_keep|. Also checks whether all chunks
  // in |ids_to_keep| are present. If not, returns a NotFound error.
  virtual absl::Status Prune(
      std::unordered_set<ContentIdProto> ids_to_keep) = 0;

  // Removes the data if the data store size exceeds its capacity.
  virtual absl::Status Cleanup() { return absl::OkStatus(); }

  // Allows to interrupt methods by setting |interrupt_|.
  void RegisterInterrupt(std::atomic<bool>* interrupt) {
    interrupt_ = interrupt;
  }

 protected:
  // Shows whether a function can be cancelled. Used in Cleanup().
  std::atomic<bool>* interrupt_ = nullptr;
};  // class DataStoreWriter

}  // namespace cdc_ft

#endif  // DATA_STORE_DATA_STORE_WRITER_H_
