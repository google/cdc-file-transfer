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

#ifndef CDC_FUSE_FS_CONSTANTS_H_
#define CDC_FUSE_FS_CONSTANTS_H_

namespace cdc_ft {

// FUSE prints
//   Port 12345 cdc_fuse_fs is up-to-date
// to stdout when its version matches the version (=build version or
// size/timestamp for DEV builds) on the local device. The port is the gRPC port
// that FUSE will try to connect to.
static constexpr char kFusePortPrefix[] = "Port ";
static constexpr char kFuseUpToDate[] = " cdc_fuse_fs is up-to-date";

// FUSE prints this to stdout when its version does not match the version on the
// local device. It indicates that the binary has to be redeployed.
static constexpr char kFuseNotUpToDate[] = "cdc_fuse_fs is not up-to-date";

// FUSE prints this to stdout when it can connect to its port. This means that
// port forwarding has finished setting up, and startup is finished.
static constexpr char kFuseConnected[] = "cdc_fuse_fs is connected";

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_CONSTANTS_H_
