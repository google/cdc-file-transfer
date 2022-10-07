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

#include "cdc_fuse_fs/config_stream_client.h"

#include <thread>

#include "common/grpc_status.h"
#include "common/log.h"
#include "manifest/content_id.h"

namespace cdc_ft {

using GetManifestIdRequest = proto::GetManifestIdRequest;
using GetManifestIdResponse = proto::GetManifestIdResponse;
using AckManifestIdReceivedRequest = proto::AckManifestIdReceivedRequest;
using AckManifestIdReceivedResponse = proto::AckManifestIdReceivedResponse;
using ConfigStreamService = proto::ConfigStreamService;

// Asynchronous gRPC streaming client for streaming configuration changes to
// gamelets. The client runs inside the CDC FUSE and requests updated manifest
// from the workstation.
class ManifestIdReader {
 public:
  ManifestIdReader(ConfigStreamService::Stub* stub) : stub_(stub) {}

  // Starts a GetManifestId() request and listens to the stream of manifest ids
  // sent from the workstation. Calls |callback| on every manifest id received.
  absl::Status StartListeningToManifestUpdates(
      std::function<absl::Status(const ContentIdProto&)> callback) {
    callback_ = callback;

    GetManifestIdRequest request;
    assert(!reader_);
    reader_ = stub_->GetManifestId(&context_, request);
    if (!reader_)
      return absl::UnavailableError("Failed to create manifest id reader");

    reader_thread_ =
        std::make_unique<std::thread>([this]() { ReadThreadMain(); });
    return absl::OkStatus();
  }

  // Thread that reads manifest ids from the GetManifestId() response stream.
  void ReadThreadMain() {
    GetManifestIdResponse response;
    LOG_INFO("Started manifest id reader thread")
    for (;;) {
      LOG_INFO("Waiting for manifest id update")
      if (!reader_->Read(&response)) break;

      LOG_INFO("Received new manifest id '%s'",
               ContentId::ToHexString(response.id()));
      absl::Status status = callback_(response.id());
      if (!status.ok()) {
        LOG_ERROR("Failed to execute callback for manifest update '%s': '%s'",
                  ContentId::ToHexString(response.id()), status.message());
      }
    }
    // This should happen if the server shuts down.
    LOG_INFO("Stopped manifest id reader thread")
  }

  void Shutdown() {
    if (!reader_thread_) return;

    context_.TryCancel();
    if (reader_thread_->joinable()) reader_thread_->join();
    reader_thread_.reset();
  }

 private:
  ConfigStreamService::Stub* stub_;
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientReader<GetManifestIdResponse>> reader_;
  std::function<absl::Status(const ContentIdProto&)> callback_;
  std::unique_ptr<std::thread> reader_thread_;
};

ConfigStreamClient::ConfigStreamClient(std::string instance,
                                       std::shared_ptr<grpc::Channel> channel)
    : instance_(std::move(instance)),
      stub_(ConfigStreamService::NewStub(std::move(channel))),
      read_client_(std::make_unique<ManifestIdReader>(stub_.get())) {}

ConfigStreamClient::~ConfigStreamClient() = default;

absl::Status ConfigStreamClient::StartListeningToManifestUpdates(
    std::function<absl::Status(const ContentIdProto&)> callback) {
  LOG_INFO("Starting to listen to manifest updates");
  return read_client_->StartListeningToManifestUpdates(callback);
}

absl::Status ConfigStreamClient::SendManifestAck(ContentIdProto manifest_id) {
  AckManifestIdReceivedRequest request;
  request.set_gamelet_id(instance_);
  *request.mutable_manifest_id() = std::move(manifest_id);

  grpc::ClientContext context_;
  AckManifestIdReceivedResponse response;
  RETURN_ABSL_IF_ERROR(
      stub_->AckManifestIdReceived(&context_, request, &response));
  return absl::OkStatus();
}

void ConfigStreamClient::Shutdown() {
  LOG_INFO("Stopping to listen to manifest updates");
  read_client_->Shutdown();
}

}  // namespace cdc_ft
