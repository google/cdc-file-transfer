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

#include "cdc_stream/grpc_asset_stream_server.h"

#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "common/grpc_status.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "data_store/data_store_reader.h"
#include "grpcpp/grpcpp.h"
#include "manifest/file_chunk_map.h"
#include "proto/asset_stream_service.grpc.pb.h"

namespace cdc_ft {
namespace {

using GetContentRequest = proto::GetContentRequest;
using GetContentResponse = proto::GetContentResponse;
using SendCachedContentIdsRequest = proto::SendCachedContentIdsRequest;
using SendCachedContentIdsResponse = proto::SendCachedContentIdsResponse;
using AssetStreamService = proto::AssetStreamService;

using GetManifestIdRequest = proto::GetManifestIdRequest;
using GetManifestIdResponse = proto::GetManifestIdResponse;
using AckManifestIdReceivedRequest = proto::AckManifestIdReceivedRequest;
using AckManifestIdReceivedResponse = proto::AckManifestIdReceivedResponse;
using ConfigStreamService = proto::ConfigStreamService;
using ProcessAssetsRequest = proto::ProcessAssetsRequest;
using ProcessAssetsResponse = proto::ProcessAssetsResponse;

}  // namespace

class AssetStreamServiceImpl final : public AssetStreamService::Service {
 public:
  AssetStreamServiceImpl(std::string src_dir,
                         DataStoreReader* data_store_reader,
                         FileChunkMap* file_chunks, InstanceIdMap* instance_ids,
                         ContentSentHandler content_sent)
      : src_dir_(std::move(src_dir)),
        data_store_reader_(data_store_reader),
        file_chunks_(file_chunks),
        started_(absl::Now()),
        instance_ids_(instance_ids),
        content_sent_(content_sent) {}

  grpc::Status GetContent(grpc::ServerContext* context,
                          const GetContentRequest* request,
                          GetContentResponse* response) override {
    // See if this is a data chunk first. The hash lookup is faster than the
    // file lookup from the data store.
    std::string rel_path;
    uint64_t offset;
    size_t size;
    std::string instance_id = instance_ids_->Get(context->peer());

    for (const ContentIdProto& id : request->id()) {
      uint32_t uint32_size;
      if (file_chunks_->Lookup(id, &rel_path, &offset, &uint32_size)) {
        size = uint32_size;
        // File data chunk.
        RETURN_GRPC_IF_ERROR(ReadFromFile(id, rel_path, offset, uint32_size,
                                          response->add_data()));
        file_chunks_->RecordStreamedChunk(id, request->thread_id());
      } else {
        // Manifest chunk.
        RETURN_GRPC_IF_ERROR(
            ReadFromDataStore(id, response->add_data(), &size));
      }
      if (content_sent_ != nullptr) {
        content_sent_(size, 1, instance_id);
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status SendCachedContentIds(
      grpc::ServerContext* context, const SendCachedContentIdsRequest* request,
      SendCachedContentIdsResponse* response) override {
    for (const ContentIdProto& id : request->id())
      file_chunks_->RecordCachedChunk(id);
    return grpc::Status::OK;
  }

 private:
  absl::Status ReadFromFile(const ContentIdProto& id,
                            const std::string& rel_path, uint64_t offset,
                            uint32_t size, std::string* data) {
    std::string path = path::Join(src_dir_, rel_path);
    path::FixPathSeparators(&path);
    data->resize(size);
    size_t read_size;
    ASSIGN_OR_RETURN(
        read_size,
        path::ReadFile(path, const_cast<char*>(data->data()), offset, size),
        "Failed to read chunk '%s', file '%s', offset %d, size %d",
        ContentId::ToHexString(id), path, offset, size);

    absl::Time now = absl::Now();
    LOG_VERBOSE("'%s', %d, '%s', '%s', %u, %u",
                absl::FormatTime("%H:%M:%S", now, absl::UTCTimeZone()),
                absl::ToInt64Milliseconds(now - started_),
                ContentId::ToHexString(id), path, offset, size);

    return absl::OkStatus();
  }

  absl::Status ReadFromDataStore(const ContentIdProto& id, std::string* data,
                                 size_t* size) {
    Buffer buf;
    RETURN_IF_ERROR(data_store_reader_->Get(id, &buf),
                    "Failed to read chunk '%s'", ContentId::ToHexString(id));

    // TODO: Get rid of copy after the Buffer uses std::string.
    *data = std::string(buf.data(), buf.size());
    *size = buf.size();
    absl::Time now = absl::Now();
    LOG_VERBOSE("'%s', %d, '%s', %d",
                absl::FormatTime("%H:%M:%S", now, absl::UTCTimeZone()),
                absl::ToInt64Milliseconds(now - started_),
                ContentId::ToHexString(id), buf.size());

    return absl::OkStatus();
  }

  const std::string src_dir_;
  DataStoreReader* const data_store_reader_;
  FileChunkMap* const file_chunks_;
  const absl::Time started_;
  InstanceIdMap* instance_ids_;
  ContentSentHandler content_sent_;
};

class ConfigStreamServiceImpl final : public ConfigStreamService::Service {
 public:
  ConfigStreamServiceImpl(InstanceIdMap* instance_ids,
                          PrioritizeAssetsHandler prio_handler)
      : instance_ids_(instance_ids), prio_handler_(std::move(prio_handler)) {}
  ~ConfigStreamServiceImpl() { Shutdown(); }

  grpc::Status GetManifestId(
      grpc::ServerContext* context, const GetManifestIdRequest* request,
      ::grpc::ServerWriter<GetManifestIdResponse>* stream) override {
    ContentIdProto local_id;
    bool running = true;
    do {
      // Shutdown happened.
      if (!WaitForUpdate(local_id)) {
        break;
      }
      LOG_INFO("Sending updated manifest id '%s' to the gamelet",
               ContentId::ToHexString(local_id));
      GetManifestIdResponse response;
      *response.mutable_id() = local_id;
      bool success = stream->Write(response);
      if (!success) {
        LOG_WARNING("Failed to send updated manifest id '%s'",
                    ContentId::ToHexString(local_id));
      }
      absl::ReaderMutexLock lock(&mutex_);
      running = running_;
    } while (running);
    return grpc::Status::OK;
  }

  grpc::Status AckManifestIdReceived(
      grpc::ServerContext* context, const AckManifestIdReceivedRequest* request,
      AckManifestIdReceivedResponse* response) override {
    // Associate the peer with the gamelet ID.
    instance_ids_->Set(context->peer(), request->gamelet_id());
    absl::MutexLock lock(&mutex_);
    acked_manifest_ids_[request->gamelet_id()] = request->manifest_id();
    return grpc::Status::OK;
  }

  grpc::Status ProcessAssets(grpc::ServerContext* context,
                             const ProcessAssetsRequest* request,
                             ProcessAssetsResponse* response) override {
    if (!prio_handler_) return grpc::Status::OK;

    std::vector<std::string> rel_paths;
    rel_paths.reserve(request->relative_paths().size());
    for (const std::string& rel_path : request->relative_paths()) {
      rel_paths.push_back(rel_path);
    }
    prio_handler_(std::move(rel_paths));
    return grpc::Status::OK;
  }

  void SetManifestId(const ContentIdProto& id) ABSL_LOCKS_EXCLUDED(mutex_) {
    LOG_INFO("Updating manifest id '%s' in configuration service",
             ContentId::ToHexString(id));
    absl::MutexLock lock(&mutex_);
    id_ = id;
  }

  absl::Status WaitForManifestAck(const std::string& instance,
                                  absl::Duration timeout) {
    absl::MutexLock lock(&mutex_);
    auto cond = [this, &instance]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
      AckedManifestIdsMap::iterator iter = acked_manifest_ids_.find(instance);
      return iter != acked_manifest_ids_.end() && id_ == iter->second;
    };

    if (!mutex_.AwaitWithTimeout(absl::Condition(&cond), timeout)) {
      return absl::DeadlineExceededError(absl::StrFormat(
          "Instance '%s' did not acknowledge reception of manifest", instance));
    }

    return absl::OkStatus();
  }

  void Shutdown() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    if (running_) {
      LOG_INFO("Shutting down configuration service");
      running_ = false;
    }
  }

  ContentIdProto GetStoredManifestId() const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return id_;
  }

  void SetPrioritizeAssetsHandler(PrioritizeAssetsHandler handler) {
    prio_handler_ = handler;
  }

 private:
  // Returns false if the update process was cancelled.
  bool WaitForUpdate(ContentIdProto& local_id) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    auto cond = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
      return !running_ || local_id != id_;
    };
    mutex_.Await(absl::Condition(&cond));
    local_id = id_;
    return running_;
  }

  mutable absl::Mutex mutex_;
  ContentIdProto id_ ABSL_GUARDED_BY(mutex_);
  bool running_ ABSL_GUARDED_BY(mutex_) = true;
  InstanceIdMap* instance_ids_ = nullptr;
  PrioritizeAssetsHandler prio_handler_;

  // Maps instance ids to the last acknowledged manifest id.
  using AckedManifestIdsMap = std::unordered_map<std::string, ContentIdProto>;
  AckedManifestIdsMap acked_manifest_ids_ ABSL_GUARDED_BY(mutex_);
};

GrpcAssetStreamServer::GrpcAssetStreamServer(
    std::string src_dir, DataStoreReader* data_store_reader,
    FileChunkMap* file_chunks, ContentSentHandler content_sent,
    PrioritizeAssetsHandler prio_assets)
    : AssetStreamServer(src_dir, data_store_reader, file_chunks),
      asset_stream_service_(std::make_unique<AssetStreamServiceImpl>(
          std::move(src_dir), data_store_reader, file_chunks, &instance_ids_,
          content_sent)),
      config_stream_service_(std::make_unique<ConfigStreamServiceImpl>(
          &instance_ids_, std::move(prio_assets))) {}

GrpcAssetStreamServer::~GrpcAssetStreamServer() = default;

absl::Status GrpcAssetStreamServer::Start(int port) {
  assert(!server_);

  std::string server_address = absl::StrFormat("localhost:%i", port);
  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(asset_stream_service_.get());
  builder.RegisterService(config_stream_service_.get());
  server_ = builder.BuildAndStart();
  if (selected_port != port) {
    return MakeStatus(
        "Failed to start streaming server: Could not listen on port %i. Is the "
        "port in use?",
        port);
  }
  if (!server_) return MakeStatus("Failed to start streaming server");
  LOG_INFO("Streaming server listening on '%s'", server_address);
  return absl::OkStatus();
}

void GrpcAssetStreamServer::SetManifestId(const ContentIdProto& manifest_id) {
  LOG_INFO("Setting manifest id '%s'", ContentId::ToHexString(manifest_id));
  assert(config_stream_service_);
  config_stream_service_->SetManifestId(manifest_id);
}

absl::Status GrpcAssetStreamServer::WaitForManifestAck(
    const std::string& instance, absl::Duration timeout) {
  assert(config_stream_service_);
  return config_stream_service_->WaitForManifestAck(instance, timeout);
}

void GrpcAssetStreamServer::Shutdown() {
  assert(config_stream_service_);
  config_stream_service_->Shutdown();
  if (server_) {
    server_->Shutdown();
    server_->Wait();
  }
}

ContentIdProto GrpcAssetStreamServer::GetManifestId() const {
  assert(config_stream_service_);
  return config_stream_service_->GetStoredManifestId();
}

}  // namespace cdc_ft
