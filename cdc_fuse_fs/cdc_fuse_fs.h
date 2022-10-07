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

#ifndef CDC_FUSE_FS_CDC_FUSE_FS_H_
#define CDC_FUSE_FS_CDC_FUSE_FS_H_

#ifndef R_OK
#define R_OK 4
#endif
#ifndef W_OK
#define W_OK 2
#endif
#ifndef X_OK
#define X_OK 1
#endif

#include <memory>

#include "absl/status/status.h"
#include "cdc_fuse_fs/config_stream_client.h"
#include "grpcpp/channel.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

class DataStoreReader;

// CdcFuse filesystem constants, exposed for testing.
namespace internal {
// Number of hardlinks is not important since the fs is read-only (I think).
constexpr int kCdcFuseDefaultNLink = 1;

// Cloudcast user and group id.
constexpr int kCdcFuseCloudcastUid = 1000;
constexpr int kCdcFuseCloudcastGid = 1000;

// Root user and group id.
constexpr int kCdcFuseRootUid = 0;
constexpr int kCdcFuseRootGid = 0;

// Default timeout after which the kernel will assume inodes are stale.
constexpr double kCdcFuseInodeTimeoutSec = 1.0;
}  // namespace internal

namespace cdc_fuse_fs {

// Initializes the CDC FUSE filesystem. Parses the command line, sets up a
// channel and a session, and optionally forks the process. For valid arguments
// see fuse_common.h.
absl::Status Initialize(int argc, char** argv);

// Starts a client to read configuration updates over gRPC |channel|.
// |instance| is the gamelet instance id.
absl::Status StartConfigClient(std::string instance,
                               std::shared_ptr<grpc::Channel> channel);

// Sets the |data_store_reader| to load data from, initializes FUSE with a
// manifest for an empty directory, and starts the filesystem. The call does
// not return until the filesystem finishes running.
// |consistency_check| defines whether FUSE consistency should be inspected
// after each manifest update.
absl::Status Run(DataStoreReader* data_store_reader, bool consistency_check);

// Releases resources. Should be called when the filesystem finished running.
void Shutdown();

// Sets |manifest_id| as a CDC FUSE root.
absl::Status SetManifest(const ContentIdProto& manifest_id);

}  // namespace cdc_fuse_fs
}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_CDC_FUSE_FS_H_
