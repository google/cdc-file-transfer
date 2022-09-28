#ifndef MANIFEST_PENDING_ASSETS_QUEUE_H
#define MANIFEST_PENDING_ASSETS_QUEUE_H

#include "absl/time/time.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

class ManifestBuilder;

// Holds an asset that was requested to be prioritized at a given point in time.
struct PriorityAsset {
  // Relative Unix file path.
  std::string rel_file_path;
  // Timestamp when this request was received.
  absl::Time received;
};

// Represents an asset that has not been fully processed yet.
struct PendingAsset {
  PendingAsset() {}
  PendingAsset(AssetProto::Type type, std::string relative_path,
               std::string filename, absl::Time deadline)
      : type(type),
        relative_path(std::move(relative_path)),
        filename(std::move(filename)),
        deadline(deadline) {}

  // The asset type (either FILE or DIRECTORY).
  AssetProto::Type type = AssetProto::UNKNOWN;

  // Relative unix path of the directory containing this asset.
  std::string relative_path;

  // File name of the asset that still needs processing.
  std::string filename;

  // If this asset was explicitly prioritized, this field is set to true,
  // otherwise false.
  bool prioritized = false;

  // If a deadline is set, it means that this asset was prioritized
  // (implicitly or explicitly) and should be processed by this deadline. Once
  // this asset has been processed, the manifest should be flushed if the
  // deadline has expired. Otherwise, additional related assets can be queued
  // and processed (implicit prioritization).
  absl::Time deadline;
};

// Queues assets that still need to be processed before they are completed.
class PendingAssetsQueue {
 public:
  // Signature for a callback function to accept items to dequeue.
  using AcceptFunc = std::function<bool(const PendingAsset&)>;

  // The |min_processing_time| is used to calculate the deadline by which a
  // pending asset should be returned to the requesting instance.
  PendingAssetsQueue(absl::Duration min_processing_time);

  // Adds the given asset |pending| to the queue of assets to complete.
  // PendingAssets without a deadline will be queued at the end, while those
  // with a given deadline will be inserted after other assets having a
  // deadline.
  void Add(PendingAsset pending);

  // Removes a PendingAsset from the queue and stores it in |pending|. If
  // |accept| is given, then only items for which |accept| returns true are
  // considered. Returns true if an item was stored in |pending|, otherwise
  // false is returned.
  bool Dequeue(PendingAsset* pending, AcceptFunc accept = nullptr);

  // Returns true if the queue is empty, otherwise returns false.
  bool Empty() const { return queue_.empty(); }

  // Modifies the list of queued assets to prioritize the assets given in
  // |prio_assets|. Returns the deadline by which the processed assets should be
  // returned to the requested instance.
  absl::Time Prioritize(const std::vector<PriorityAsset>& prio_assets,
                        ManifestBuilder* manifest_builder);

 private:
  const absl::Duration min_processing_time_;
  std::list<PendingAsset> queue_;
};

}  // namespace cdc_ft

#endif  // MANIFEST_PENDING_ASSETS_QUEUE_H
