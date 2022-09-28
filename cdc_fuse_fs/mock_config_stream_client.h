#ifndef CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_
#define CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_

#include "cdc_fuse_fs/config_stream_client.h"

namespace cdc_ft {

// Mock ConfigStreamClient implementation, used for testing only.
class MockConfigStreamClient : public ConfigStreamClient {
 public:
  // Returns the list of relative file paths to assets that have been
  // prioritized via ProcessAssets().
  const std::vector<std::string>& PrioritizedAssets() const;

  // Clears the list prioritized assets.
  void ClearPrioritizedAssets();

  // ConfigStreamClient

  absl::Status StartListeningToManifestUpdates(
      std::function<absl::Status(const ContentIdProto&)> callback) override;
  absl::Status SendManifestAck(ContentIdProto manifest_id) override;
  absl::Status ProcessAssets(std::vector<std::string> assets) override;
  void Shutdown() override;
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_MOCK_CONFIG_STREAM_CLIENT_H_
