#include "dev/file_transfer/cdc_fuse_fs/mock_config_stream_client.h"

namespace cdc_ft {

absl::Status MockConfigStreamClient::StartListeningToManifestUpdates(
    std::function<absl::Status(const ContentIdProto&)> callback) {
  return absl::OkStatus();
}

absl::Status MockConfigStreamClient::SendManifestAck(
    ContentIdProto manifest_id) {
  return absl::OkStatus();
}

absl::Status MockConfigStreamClient::ProcessAssets(
    std::vector<std::string> assets) {
  return absl::OkStatus();
}

void MockConfigStreamClient::Shutdown() {
  // Do nothing.
}

}  // namespace cdc_ft
