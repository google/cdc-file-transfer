#include "manifest/pending_assets_queue.h"

#include "common/log.h"
#include "manifest/manifest_builder.h"

namespace cdc_ft {

PendingAssetsQueue::PendingAssetsQueue(absl::Duration min_processing_time)
    : min_processing_time_(min_processing_time) {}

void PendingAssetsQueue::Add(PendingAsset pending) {
  if (pending.deadline == absl::InfiniteFuture()) {
    queue_.push_back(std::move(pending));
    return;
  }

  // Pending assets with a deadline will be added at the end of other
  // prioritized assets.
  auto it =
      std::find_if(queue_.begin(), queue_.end(), [](const PendingAsset& pa) {
        return pa.deadline == absl::InfiniteFuture();
      });
  queue_.insert(it, std::move(pending));
}

bool PendingAssetsQueue::Dequeue(PendingAsset* pending, AcceptFunc accept) {
  auto it = queue_.begin();
  while (it != queue_.end() && accept && !accept(*it)) ++it;
  if (it == queue_.end()) return false;
  *pending = std::move(*it);
  queue_.erase(it);
  return true;
}

absl::Time PendingAssetsQueue::Prioritize(
    const std::vector<PriorityAsset>& prio_assets,
    ManifestBuilder* manifest_builder) {
  absl::Time min_received = absl::InfiniteFuture();

  for (const PriorityAsset& prio_asset : prio_assets) {
    if (prio_asset.received < min_received) min_received = prio_asset.received;

    // Check if this asset is still in progress.
    absl::StatusOr<AssetBuilder> asset = manifest_builder->GetOrCreateAsset(
        prio_asset.rel_file_path, AssetProto::UNKNOWN);
    if (!asset.ok()) {
      LOG_ERROR("Failed to prioritize asset '%s': %s", prio_asset.rel_file_path,
                asset.status().ToString());
      continue;
    }
    if (!asset->InProgress()) continue;

    // Find the queued task for this asset.
    auto prio_end = queue_.end();
    for (auto it = queue_.begin(); it != queue_.end(); ++it) {
      // Remember the first task that is not prioritized so that we can insert
      // new prioritized tasks just before.
      if (prio_end == queue_.end() && it->deadline == absl::InfiniteFuture()) {
        prio_end = it;
      }

      if (it->relative_path == asset->RelativePath() &&
          it->filename == asset->Name()) {
        // If this asset is not yet prioritized, |prio_end| will be set
        // accordingly and we move |*it| to the end of the prioritized tasks.
        if (it->deadline == absl::InfiniteFuture()) {
          it->deadline = prio_asset.received + min_processing_time_;
          it->prioritized = true;  // Expliciy prioritization.
          queue_.insert(prio_end, std::move(*it));
          queue_.erase(it);
        }
        break;
      }
    }
  }

  return min_received + min_processing_time_;
}

}  // namespace cdc_ft
