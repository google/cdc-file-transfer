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

#ifndef CDC_STREAM_ASSET_STREAM_SERVER_H_
#define CDC_STREAM_ASSET_STREAM_SERVER_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/time/time.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// Handles an event when content is transmitted from the workstation to a
// gamelet.
// |byte_count| number of bytes transferred during the session so far.
// |chunk_count| number of chunks transferred during the session so far.
// |instance_id| instance id, which identifies the session.
using ContentSentHandler = std::function<void(
    size_t byte_count, size_t chunk_count, std::string instance_id)>;

// Handles requests to prioritize the given list of assets while updating the
// manifest. |rel_paths| relative Unix paths of assets to prioritize.
using PrioritizeAssetsHandler =
    std::function<void(std::vector<std::string> rel_paths)>;

class DataStoreReader;
class FileChunkMap;

enum class AssetStreamServerType { kGrpc, kTest };

class AssetStreamServer {
 public:
  // Returns AssetStreamServer of |type|.
  // |src_dir| is the directory on the workstation to mount.
  // |data_store_reader| is responsible for loading content by ID.
  // |file_chunks| is used for mapping data chunk ids to file locations.
  // |content_sent| handles event when data is transferred from the workstation
  // to a gamelet.
  static std::unique_ptr<AssetStreamServer> Create(
      AssetStreamServerType type, std::string src_dir,
      DataStoreReader* data_store_reader, FileChunkMap* file_chunks,
      ContentSentHandler content_sent, PrioritizeAssetsHandler prio_assets);

  AssetStreamServer(const AssetStreamServer& other) = delete;
  AssetStreamServer& operator=(const AssetStreamServer& other) = delete;
  virtual ~AssetStreamServer() = default;

  // Starts the asset stream server on the given |port|.
  // Asserts that the server is not yet running.
  virtual absl::Status Start(int port) = 0;

  // Sets |manifest_id| to be distributed to gamelets.
  // Thread-safe.
  virtual void SetManifestId(const ContentIdProto& manifest_id) = 0;

  // Waits until the FUSE for the given |instance| id has acknowledged the
  // reception of the currently set manifest id. Returns a DeadlineExceeded
  // error if the ack is not received within the given |timeout|.
  // Thread-safe.
  virtual absl::Status WaitForManifestAck(const std::string& instance,
                                          absl::Duration timeout) = 0;

  // Stops internal services and waits for the server to shut down.
  virtual void Shutdown() = 0;

  // Returns the used manifest id.
  // Thread-safe.
  virtual ContentIdProto GetManifestId() const = 0;

 protected:
  // Creates a new asset streaming server.
  // |src_dir| is the directory on the workstation to mount.
  // |data_store_reader| is responsible for loading content by ID.
  // |file_chunks| is used for mapping data chunk ids to file locations.
  AssetStreamServer(std::string src_dir, DataStoreReader* data_store_reader,
                    FileChunkMap* file_chunks);
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_ASSET_STREAM_SERVER_H_
