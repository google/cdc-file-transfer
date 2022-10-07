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

#ifndef DATA_STORE_GRPC_READER_H_
#define DATA_STORE_GRPC_READER_H_

#include "absl/status/statusor.h"
#include "data_store/data_store_reader.h"
#include "grpcpp/channel.h"
#include "manifest/content_id.h"

namespace cdc_ft {

class AssetStreamClient;

// Implementation of a DataStoreReader that loads chunks through gRpc
// exclusively. Does not have any local caching.
class GrpcReader : public DataStoreReader {
 public:
  // |channel| is a grpc channel to connect to.
  // |enable_stats| determines whether additional statistics are sent.
  GrpcReader(std::shared_ptr<grpc::Channel> channel, bool enable_stats);
  virtual ~GrpcReader();

  GrpcReader(const GrpcReader&) = delete;
  GrpcReader& operator=(const GrpcReader&) = delete;

  // Sends the IDs of all cached chunks to the workstation for statistical
  // purposes.
  absl::Status SendCachedContentIds(std::vector<ContentIdProto> content_ids);

  // DataStoreReader:
  absl::StatusOr<size_t> Get(const ContentIdProto& key, void* data,
                             uint64_t size, uint64_t offset) override;
  absl::Status Get(ChunkTransferList* chunks) override;
  absl::Status Get(const ContentIdProto& key, Buffer* data) override;

 private:
  std::unique_ptr<AssetStreamClient> client_;
};

}  // namespace cdc_ft

#endif  // DATA_STORE_GRPC_READER_H_
