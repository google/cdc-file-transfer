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

#include "data_store/data_store_writer.h"

#include "absl/strings/str_format.h"
#include "common/status.h"
#include "manifest/content_id.h"

namespace cdc_ft {

bool DataStoreWriter::Contains(const ContentIdProto& content_id) {
  Buffer buffer;
  return Get(content_id, &buffer).ok();
}

absl::Status DataStoreWriter::PutProto(
    const google::protobuf::MessageLite& proto, ContentIdProto* content_id,
    size_t* proto_size) {
  // Serialize the proto.
  std::string out;
  if (!proto.SerializeToString(&out)) {
    return absl::InternalError(
        absl::StrFormat("Failed to serialize %s.", proto.GetTypeName()));
  }
  // Calculate the proto's content ID.
  *content_id = ContentId::FromDataString(out);
  if (proto_size) *proto_size = out.size();
  // Write manifest chunk to storage.
  return Put(*content_id, out.c_str(), out.size());
}

}  // namespace cdc_ft
