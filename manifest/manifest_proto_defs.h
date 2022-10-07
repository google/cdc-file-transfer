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

#ifndef MANIFEST_MANIFEST_PROTO_DEFS_H_
#define MANIFEST_MANIFEST_PROTO_DEFS_H_

#include "proto/manifest.pb.h"

namespace cdc_ft {

// Convenience typedefs to make the protos more easily accessible.
using AssetListProto = proto::AssetList;
using AssetProto = proto::Asset;
using CdcParamsProto = proto::CdcParameters;
using ChunkListProto = proto::ChunkList;
using ChunkRefProto = proto::ChunkRef;
using ContentIdProto = proto::ContentId;
using IndirectChunkListProto = proto::IndirectChunkList;
using ManifestProto = proto::Manifest;
using RepeatedAssetProto = google::protobuf::RepeatedPtrField<AssetProto>;
using RepeatedChunkRefProto = google::protobuf::RepeatedPtrField<ChunkRefProto>;
using RepeatedContentIdProto =
    google::protobuf::RepeatedPtrField<ContentIdProto>;
using RepeatedIndirectChunkListProto =
    google::protobuf::RepeatedPtrField<IndirectChunkListProto>;
using RepeatedStringProto = google::protobuf::RepeatedPtrField<std::string>;

namespace proto {

inline bool operator==(const Asset& a, const Asset& b) {
  return a.SerializeAsString() == b.SerializeAsString();
}

inline bool operator!=(const Asset& a, const Asset& b) { return !(a == b); }

}  // namespace proto
}  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_PROTO_DEFS_H_
