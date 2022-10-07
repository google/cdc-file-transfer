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

#include "manifest/asset_builder.h"

#include "absl/strings/str_cat.h"
#include "common/path.h"

namespace cdc_ft {

AssetBuilder::AssetBuilder() = default;

AssetBuilder::AssetBuilder(AssetProto* proto, const std::string& rel_path)
    : proto_(proto), rel_path_(path::ToUnix(rel_path)) {}

AssetBuilder::~AssetBuilder() = default;

std::string AssetBuilder::RelativeFilePath() const {
  if (!proto_) return std::string();
  return path::JoinUnix(rel_path_, proto_->name());
}

void AssetBuilder::AppendChunk(const ContentIdProto& content_id, size_t len) {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::FILE);
  // TODO: Handle indirect chunks.
  assert(proto_->file_indirect_chunks_size() == 0);
  ChunkRefProto* chunk_ref = proto_->add_file_chunks();
  chunk_ref->set_offset(proto_->file_size());
  chunk_ref->mutable_chunk_id()->CopyFrom(content_id);
  proto_->set_file_size(proto_->file_size() + len);
}

void AssetBuilder::TruncateChunks() {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::FILE);
  proto_->mutable_file_chunks()->Clear();
  proto_->mutable_file_indirect_chunks()->Clear();
  proto_->set_file_size(0);
}

void AssetBuilder::SetChunks(const RepeatedChunkRefProto& chunks,
                             uint64_t file_size) {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::FILE);
  proto_->mutable_file_chunks()->Clear();
  proto_->mutable_file_chunks()->CopyFrom(chunks);
  proto_->mutable_file_indirect_chunks()->Clear();
  proto_->set_file_size(file_size);
}

void AssetBuilder::SwapChunks(RepeatedChunkRefProto* chunks,
                              uint64_t file_size) {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::FILE);
  proto_->mutable_file_chunks()->Swap(chunks);
  proto_->mutable_file_indirect_chunks()->Clear();
  proto_->set_file_size(file_size);
}

void AssetBuilder::SetFileSize(uint64_t file_size) {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::FILE);
  proto_->set_file_size(file_size);
}

AssetBuilder AssetBuilder::AppendAsset(const std::string& name,
                                       AssetProto::Type type) {
  assert(proto_ != nullptr);
  assert(proto_->type() == AssetProto::DIRECTORY);
  AssetProto* child = proto_->add_dir_assets();
  child->set_type(type);
  child->set_name(name);
  return AssetBuilder(child, RelativeFilePath());
}

bool AssetBuilder::InProgress() const {
  if (!proto_) return false;
  return proto_->in_progress();
}

void AssetBuilder::SetInProgress(bool in_progress) {
  assert(proto_ != nullptr);
  proto_->set_in_progress(in_progress);
}

void AssetBuilder::SetProto(AssetProto* proto, const std::string& rel_path) {
  Clear();
  proto_ = proto;
  absl::StrAppend(&rel_path_, path::ToUnix(rel_path));
}

void AssetBuilder::Clear() {
  proto_ = nullptr;
  rel_path_.resize(0);
}

AssetBuilder& AssetBuilder::operator=(const AssetBuilder& other) {
  proto_ = other.proto_;
  rel_path_ = other.rel_path_;
  return *this;
}

}  // namespace cdc_ft
