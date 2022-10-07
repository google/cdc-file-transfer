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

#ifndef DATA_STORE_MEM_DATA_STORE_H_
#define DATA_STORE_MEM_DATA_STORE_H_

#include <string>
#include <vector>

#include "data_store/data_store_writer.h"
#include "manifest/content_id.h"

namespace cdc_ft {

// In-memory implementation of a DataStoreWriter. Data needs to be pre-
// populated manually using AddData() and AddProto(). Useful for testing.
class MemDataStore : public DataStoreWriter {
 public:
  using ChunkMap = std::unordered_map<ContentIdProto, std::vector<char>>;

  MemDataStore();
  MemDataStore(const MemDataStore&) = delete;
  MemDataStore& operator=(const MemDataStore&) = delete;

  virtual ~MemDataStore();

  // TODO: Extract AddData in a helper function.
  // Adds |data| to the memory-backed storage and returns the id to it.
  ContentIdProto AddData(std::vector<char> data);

  // TODO: Extract AddProto in a helper function.
  // Serializes |message|, adds it to the memory-backed storage, and returns the
  // id to it.
  ContentIdProto AddProto(const google::protobuf::MessageLite& message);

  // Note: DO NOT MIX Add* get Get* methods in a multi-threaded environment!
  // Get* methods are thread-safe as they are read-only, but Add* methods write
  // to the data. They are not thread-safe.

  // DataStoreReader:
  absl::StatusOr<size_t> Get(const ContentIdProto& id, void* data,
                             size_t offset, size_t size) override;
  absl::Status Get(const ContentIdProto& content_id, Buffer* data) override;
  absl::Status Get(ChunkTransferList* chunks) override;

  // DataStoreWriter:
  bool Contains(const ContentIdProto& content_id) override;

  absl::Status Put(const ContentIdProto& content_id, const void* data,
                   size_t size) override;

  absl::Status Remove(const ContentIdProto& content_id) override;

  absl::Status Wipe() override;

  absl::Status Prune(std::unordered_set<ContentIdProto> ids_to_keep) override;

  // Direct access to the chunks for testing.
  const ChunkMap& Chunks() const { return data_lookup_; }
  ChunkMap& Chunks() { return data_lookup_; }

 private:
  // Maps content IDs to chunks.
  ChunkMap data_lookup_;
};

}  // namespace cdc_ft

#endif  // DATA_STORE_MEM_DATA_STORE_H_
