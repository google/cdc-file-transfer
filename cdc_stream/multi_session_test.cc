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

#include "cdc_stream/multi_session.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "absl/strings/match.h"
#include "cdc_stream/testing_asset_stream_server.h"
#include "common/path.h"
#include "common/platform.h"
#include "common/process.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"
#include "manifest/manifest_test_base.h"

namespace cdc_ft {
namespace {
constexpr char kTestDir[] = "multisession_test_dir";
constexpr char kData[] = {10, 20, 30, 40, 50, 60, 70, 80, 90};
constexpr size_t kDataSize = sizeof(kData);
constexpr char kInstance[] = "test_instance";
constexpr int kPort = 44444;
constexpr absl::Duration kTimeout = absl::Milliseconds(5);
constexpr char kVeryLongPath[] =
    "C:\\this\\is\\some\\really\\really\\really\\really\\really\\really\\really"
    "\\really\\really\\really\\really\\really\\really\\really\\really\\really"
    "\\really\\really\\really\\really\\really\\really\\really\\really\\really"
    "\\really\\really\\really\\really\\really\\really\\really\\really\\really"
    "\\really\\long\\path";
constexpr uint32_t kNumThreads = 1;

struct MetricsRecord {
  MetricsRecord(metrics::DeveloperLogEvent evt, metrics::EventType code)
      : evt(std::move(evt)), code(code) {}
  metrics::DeveloperLogEvent evt;
  metrics::EventType code;
};

class MetricsServiceForTest : public MultiSessionMetricsRecorder {
 public:
  MetricsServiceForTest() : MultiSessionMetricsRecorder(nullptr) {}

  virtual ~MetricsServiceForTest() = default;

  void RecordEvent(metrics::DeveloperLogEvent event,
                   metrics::EventType code) const override
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    metrics_records_.push_back(MetricsRecord(std::move(event), code));
  }

  // Waits until |num_events| events of type |type| have been recorded, or until
  // the function times out. Returns true if the condition was met and false if
  // in case of a timeout.
  bool WaitForEvents(metrics::EventType type, int num_events = 1,
                     absl::Duration timeout = absl::Seconds(1)) {
    absl::MutexLock lock(&mutex_);
    auto cond = [this, type, num_events]() {
      return std::count_if(metrics_records_.begin(), metrics_records_.end(),
                           [type](const MetricsRecord& mr) {
                             return mr.code == type;
                           }) >= num_events;
    };
    return mutex_.AwaitWithTimeout(absl::Condition(&cond), timeout);
  }

  std::vector<MetricsRecord> GetEventsAndClear(metrics::EventType type)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    std::vector<MetricsRecord> events;
    std::vector<MetricsRecord> remaining;
    absl::MutexLock lock(&mutex_);
    for (size_t i = 0; i < metrics_records_.size(); ++i) {
      if (metrics_records_[i].code == type) {
        events.push_back(std::move(metrics_records_[i]));
      } else {
        remaining.push_back(std::move(metrics_records_[i]));
      }
    }
    metrics_records_ = std::move(remaining);
    return events;
  }

 private:
  mutable absl::Mutex mutex_;
  mutable std::vector<MetricsRecord> metrics_records_;
};

class MultiSessionTest : public ManifestTestBase {
 public:
  MultiSessionTest() : ManifestTestBase(GetTestDataDir("multi_session")) {
    Log::Initialize(std::make_unique<ConsoleLog>(LogLevel::kInfo));
  }
  ~MultiSessionTest() { Log::Shutdown(); }

  void SetUp() override {
    // Use a temporary directory to be able to test empty directories (git does
    // not index empty directories) and creation/deletion of files.
    EXPECT_OK(path::RemoveDirRec(test_dir_path_));
    EXPECT_OK(path::CreateDirRec(test_dir_path_));
    metrics_service_ = new MetricsServiceForTest();
  }

  void TearDown() override {
    EXPECT_OK(path::RemoveDirRec(test_dir_path_));
    delete metrics_service_;
  }

 protected:
  // Callback if the manifest was updated == a new manifest is set.
  void OnManifestUpdated() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    ++num_manifest_updates_;
  }

  // Waits until the manifest is fully computed: the manifest id is not changed
  // anymore.
  bool WaitForManifestUpdated(uint32_t exp_num_manifest_updates,
                              absl::Duration timeout = absl::Seconds(5)) {
    absl::MutexLock lock(&mutex_);
    auto cond = [&]() {
      return exp_num_manifest_updates == num_manifest_updates_;
    };
    mutex_.AwaitWithTimeout(absl::Condition(&cond), timeout);
    return exp_num_manifest_updates == num_manifest_updates_;
  }

  void CheckMultiSessionStartNotRecorded() {
    std::vector<MetricsRecord> events = metrics_service_->GetEventsAndClear(
        metrics::EventType::kMultiSessionStart);
    EXPECT_EQ(events.size(), 0);
  }

  void CheckMultiSessionStartRecorded(uint64_t byte_count, uint64_t chunk_count,
                                      uint32_t file_count) {
    std::vector<MetricsRecord> events = metrics_service_->GetEventsAndClear(
        metrics::EventType::kMultiSessionStart);
    ASSERT_EQ(events.size(), 1);
    metrics::MultiSessionStartData* data =
        events[0].evt.as_manager_data->multi_session_start_data.get();
    EXPECT_EQ(data->byte_count, byte_count);
    EXPECT_EQ(data->chunk_count, chunk_count);
    EXPECT_EQ(data->file_count, file_count);
    EXPECT_EQ(data->min_chunk_size, 128 << 10);
    EXPECT_EQ(data->avg_chunk_size, 256 << 10);
    EXPECT_EQ(data->max_chunk_size, 1024 << 10);
  }

  metrics::ManifestUpdateData GetManifestUpdateData(
      metrics::UpdateTrigger trigger, absl::StatusCode status,
      size_t total_assets_added_or_updated, size_t total_assets_deleted,
      size_t total_chunks, size_t total_files_added_or_updated,
      size_t total_files_failed, size_t total_processed_bytes) {
    metrics::ManifestUpdateData manifest_upd;
    manifest_upd.trigger = trigger;
    manifest_upd.status = status;
    manifest_upd.total_assets_added_or_updated = total_assets_added_or_updated;
    manifest_upd.total_assets_deleted = total_assets_deleted;
    manifest_upd.total_chunks = total_chunks;
    manifest_upd.total_files_added_or_updated = total_files_added_or_updated;
    manifest_upd.total_files_failed = total_files_failed;
    manifest_upd.total_processed_bytes = total_processed_bytes;
    return manifest_upd;
  }

  void CheckManifestUpdateRecorded(
      std::vector<metrics::ManifestUpdateData> manifests) {
    std::vector<MetricsRecord> events = metrics_service_->GetEventsAndClear(
        metrics::EventType::kManifestUpdated);
    ASSERT_EQ(events.size(), manifests.size());
    for (size_t i = 0; i < manifests.size(); ++i) {
      metrics::ManifestUpdateData* data =
          events[i].evt.as_manager_data->manifest_update_data.get();
      EXPECT_LT(data->local_duration_ms, 60000ull);
      EXPECT_EQ(data->status, manifests[i].status);
      EXPECT_EQ(data->total_assets_added_or_updated,
                manifests[i].total_assets_added_or_updated);
      EXPECT_EQ(data->total_assets_deleted, manifests[i].total_assets_deleted);
      EXPECT_EQ(data->total_chunks, manifests[i].total_chunks);
      EXPECT_EQ(data->total_files_added_or_updated,
                manifests[i].total_files_added_or_updated);
      EXPECT_EQ(data->total_processed_bytes,
                manifests[i].total_processed_bytes);
      EXPECT_EQ(data->trigger, manifests[i].trigger);
    }
  }

  const std::string test_dir_path_ = path::Join(path::GetTempDir(), kTestDir);
  WinProcessFactory process_factory_;
  absl::Mutex mutex_;
  uint32_t num_manifest_updates_ ABSL_GUARDED_BY(mutex_) = 0;
  MetricsServiceForTest* metrics_service_;
};

constexpr char kCacheDir[] = "c__path_to_dir_ee54bbbc";

TEST_F(MultiSessionTest, GetCacheDir_IgnoresTrailingPathSeparators) {
  EXPECT_EQ(MultiSession::GetCacheDir("C:\\path\\to\\dir"), kCacheDir);
  EXPECT_EQ(MultiSession::GetCacheDir("C:\\path\\to\\dir\\"), kCacheDir);
}

TEST_F(MultiSessionTest, GetCacheDir_WorksWithForwardSlashes) {
  EXPECT_EQ(MultiSession::GetCacheDir("C:/path/to/dir"), kCacheDir);
  EXPECT_EQ(MultiSession::GetCacheDir("C:/path/to/dir/"), kCacheDir);
}

TEST_F(MultiSessionTest, GetCacheDir_ReplacesInvalidCharacters) {
  EXPECT_EQ(MultiSession::GetCacheDir("C:\\<>:\"/\\|?*"),
            "c___________ae188efd");
}

TEST_F(MultiSessionTest, GetCacheDir_UsesFullPath) {
  EXPECT_EQ(MultiSession::GetCacheDir("foo/bar"),
            MultiSession::GetCacheDir(path::GetFullPath("foo/bar")));
}

#if PLATFORM_WINDOWS
TEST_F(MultiSessionTest, GetCacheDir_IgnoresCaseOnWindows) {
  EXPECT_EQ(MultiSession::GetCacheDir("C:\\PATH\\TO\\DIR"), kCacheDir);
}
#endif

TEST_F(MultiSessionTest, GetCachePath_ContainsExpectedParts) {
  absl::StatusOr<std::string> cache_path =
      MultiSession::GetCachePath("C:\\path\\to\\dir");
  ASSERT_OK(cache_path);
  EXPECT_TRUE(absl::EndsWith(*cache_path, kCacheDir)) << *cache_path;
  EXPECT_TRUE(
      absl::StrContains(*cache_path, path::Join("cdc-file-transfer", "chunks")))
      << *cache_path;
}

TEST_F(MultiSessionTest, GetCachePath_ShortensLongPaths) {
  EXPECT_GT(strlen(kVeryLongPath), MultiSession::kDefaultMaxCachePathLen);
  std::string cache_dir = MultiSession::GetCacheDir(kVeryLongPath);
  absl::StatusOr<std::string> cache_path =
      MultiSession::GetCachePath(kVeryLongPath);
  ASSERT_OK(cache_path);
  EXPECT_EQ(cache_path->size(), MultiSession::kDefaultMaxCachePathLen);
  EXPECT_TRUE(
      absl::StrContains(*cache_path, path::Join("cdc-file-transfer", "chunks")))
      << *cache_path;
  // The hash in the end of the path is kept and not shortened.
  EXPECT_EQ(cache_dir.substr(cache_dir.size() - MultiSession::kDirHashLen),
            cache_path->substr(cache_path->size() - MultiSession::kDirHashLen));
}

TEST_F(MultiSessionTest, GetCachePath_DoesNotSplitUtfCodePoints) {
  // Find out the length of the %APPDATA%\cdc-file-transfer\chunks\" + hash
  // part.
  absl::StatusOr<std::string> cache_path = MultiSession::GetCachePath("");
  ASSERT_OK(cache_path);
  size_t base_len = cache_path->size();

  // Path has are two 2-byte characters. They should not be split in the middle.
  cache_path = MultiSession::GetCachePath(u8"\u0200\u0200", base_len);
  ASSERT_OK(cache_path);
  EXPECT_EQ(cache_path->size(), base_len);

  // %APPDATA%\cdc-file-transfer\chunks\abcdefg
  cache_path = MultiSession::GetCachePath(u8"\u0200\u0200", base_len + 1);
  ASSERT_OK(cache_path);
  EXPECT_EQ(cache_path->size(), base_len);

  // %APPDATA%\cdc-file-transfer\chunks\\u0200abcdefg
  cache_path = MultiSession::GetCachePath(u8"\u0200\u0200", base_len + 2);
  ASSERT_OK(cache_path);
  EXPECT_EQ(cache_path->size(), base_len + 2);

  // %APPDATA%\cdc-file-transfer\chunks\\u0200abcdefg
  cache_path = MultiSession::GetCachePath(u8"\u0200\u0200", base_len + 3);
  ASSERT_OK(cache_path);
  EXPECT_EQ(cache_path->size(), base_len + 2);
}

// Calculate manifest for an empty directory.
TEST_F(MultiSessionTest, MultiSessionRunnerOnEmpty) {
  cfg_.src_dir = test_dir_path_;
  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));
  EXPECT_TRUE(WaitForManifestUpdated(2));
  ASSERT_TRUE(
      metrics_service_->WaitForEvents(metrics::EventType::kMultiSessionStart));
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
  CheckMultiSessionStartRecorded(0, 0, 0);
  CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{
      GetManifestUpdateData(metrics::UpdateTrigger::kInitUpdateAll,
                            absl::StatusCode::kOk, 0, 0, 0, 0, 0, 0)});

  EXPECT_OK(runner.Status());
  EXPECT_OK(runner.Shutdown());
}

// Calculate manifest for a non-empty directory.
TEST_F(MultiSessionTest, MultiSessionRunnerNonEmptySucceeds) {
  // Contains a.txt, subdir/b.txt, subdir/c.txt, subdir/d.txt.
  cfg_.src_dir = path::Join(base_dir_, "non_empty");
  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));
  EXPECT_TRUE(WaitForManifestUpdated(2));
  ASSERT_TRUE(
      metrics_service_->WaitForEvents(metrics::EventType::kMultiSessionStart));
  CheckMultiSessionStartRecorded(46, 4, 4);
  ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals(
      {"a.txt", "subdir", "subdir/b.txt", "subdir/c.txt", "subdir/d.txt"},
      runner.ManifestId()));
  EXPECT_OK(runner.Status());
  EXPECT_OK(runner.Shutdown());
}

// Update manifest on adding a file.
TEST_F(MultiSessionTest, MultiSessionRunnerAddFileSucceeds) {
  cfg_.src_dir = test_dir_path_;
  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });

  {
    SCOPED_TRACE("Initialize.");

    EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));
    // 1 file was added, 1 intermediate + 1 final manifest is pushed.
    EXPECT_TRUE(WaitForManifestUpdated(2));
    EXPECT_OK(runner.WaitForManifestAck(kInstance, kTimeout));
    EXPECT_TRUE(metrics_service_->WaitForEvents(
        metrics::EventType::kMultiSessionStart));
    ASSERT_OK(runner.Status());
  }

  {
    SCOPED_TRACE("Created base manifest for the test directory.");

    CheckMultiSessionStartRecorded(0, 0, 0);
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
    CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{
        GetManifestUpdateData(metrics::UpdateTrigger::kInitUpdateAll,
                              absl::StatusCode::kOk, 0, 0, 0, 0, 0, 0)});
  }

  {
    SCOPED_TRACE("Added file.txt.");

    uint32_t prev_updates = num_manifest_updates_;
    const std::string file_path = path::Join(test_dir_path_, "file.txt");
    EXPECT_OK(path::WriteFile(file_path, kData, kDataSize));
    // 1 file was added, 1 intermediate + 1 final manifest is pushed.
    EXPECT_TRUE(WaitForManifestUpdated(prev_updates + 2));
    EXPECT_TRUE(
        metrics_service_->WaitForEvents(metrics::EventType::kManifestUpdated));
    ASSERT_NO_FATAL_FAILURE(
        ExpectManifestEquals({"file.txt"}, runner.ManifestId()));
    CheckMultiSessionStartNotRecorded();
    CheckManifestUpdateRecorded(
        std::vector<metrics::ManifestUpdateData>{GetManifestUpdateData(
            metrics::UpdateTrigger::kRegularUpdate, absl::StatusCode::kOk, 1, 0,
            1, 1, 0, kDataSize)});
  }
  EXPECT_OK(runner.Status());
  EXPECT_OK(runner.Shutdown());
}

// Fail if the directory does not exist as the watching could not be started.
// At this moment we expect that the directory exists.
TEST_F(MultiSessionTest, MultiSessionRunnerNoDirFails) {
  cfg_.src_dir = path::Join(base_dir_, "non_existing");
  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));

  ASSERT_FALSE(
      absl::IsNotFound(runner.WaitForManifestAck(kInstance, kTimeout)));
  ASSERT_FALSE(WaitForManifestUpdated(1, absl::Milliseconds(10)));
  CheckMultiSessionStartNotRecorded();
  CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{});
  EXPECT_NOT_OK(runner.Shutdown());
  EXPECT_TRUE(absl::StrContains(runner.Status().ToString(),
                                "Could not start watching"));
}

// Do not break if the directory is recreated.
TEST_F(MultiSessionTest, MultiSessionRunnerDirRecreatedSucceeds) {
  cfg_.src_dir = test_dir_path_;
  EXPECT_OK(path::WriteFile(path::Join(test_dir_path_, "file.txt"), kData,
                            kDataSize));

  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));

  {
    SCOPED_TRACE("Originally, only the streamed directory contains file.txt.");
    EXPECT_TRUE(WaitForManifestUpdated(2));
    ASSERT_TRUE(metrics_service_->WaitForEvents(
        metrics::EventType::kMultiSessionStart));
    CheckMultiSessionStartRecorded((uint64_t)kDataSize, 1, 1);
    ASSERT_NO_FATAL_FAILURE(
        ExpectManifestEquals({"file.txt"}, runner.ManifestId()));
    CheckManifestUpdateRecorded(
        std::vector<metrics::ManifestUpdateData>{GetManifestUpdateData(
            metrics::UpdateTrigger::kInitUpdateAll, absl::StatusCode::kOk, 1, 0,
            1, 1, 0, kDataSize)});
  }

  {
    SCOPED_TRACE(
        "Remove the streamed directory, the manifest should become empty.");
    uint32_t prev_updates = num_manifest_updates_;
    EXPECT_OK(path::RemoveDirRec(test_dir_path_));
    ASSERT_TRUE(WaitForManifestUpdated(prev_updates + 1));
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
    CheckManifestUpdateRecorded(
        std::vector<metrics::ManifestUpdateData>{GetManifestUpdateData(
            metrics::UpdateTrigger::kRunningUpdateAll,
            absl::StatusCode::kNotFound, 1, 0, 1, 1, 0, kDataSize)});
  }

  {
    SCOPED_TRACE(
        "Create the watched directory -> an empty manifest should be "
        "streamed.");
    uint32_t prev_updates = num_manifest_updates_;
    EXPECT_OK(path::CreateDirRec(test_dir_path_));
    // The first update is always the empty manifest, wait for the second one.
    EXPECT_TRUE(WaitForManifestUpdated(prev_updates + 2));
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
    EXPECT_TRUE(
        metrics_service_->WaitForEvents(metrics::EventType::kManifestUpdated));
    CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{
        GetManifestUpdateData(metrics::UpdateTrigger::kRunningUpdateAll,
                              absl::StatusCode::kOk, 0, 0, 0, 0, 0, 0)});
  }

  {
    SCOPED_TRACE("Create 'new_file.txt' -> new manifest should be created.");
    uint32_t prev_updates = num_manifest_updates_;
    EXPECT_OK(path::WriteFile(path::Join(test_dir_path_, "new_file.txt"), kData,
                              kDataSize));
    // The first update doesn't have the chunks for new_file.txt, wait for the
    // second one.
    ASSERT_TRUE(WaitForManifestUpdated(prev_updates + 2));
    ASSERT_NO_FATAL_FAILURE(
        ExpectManifestEquals({"new_file.txt"}, runner.ManifestId()));
    EXPECT_TRUE(
        metrics_service_->WaitForEvents(metrics::EventType::kManifestUpdated));
    CheckManifestUpdateRecorded(
        std::vector<metrics::ManifestUpdateData>{GetManifestUpdateData(
            metrics::UpdateTrigger::kRegularUpdate, absl::StatusCode::kOk, 1, 0,
            1, 1, 0, kDataSize)});
    CheckMultiSessionStartNotRecorded();
  }

  EXPECT_OK(runner.Status());
  EXPECT_OK(runner.Shutdown());
}

// Fail if the streamed source is a file.
TEST_F(MultiSessionTest, MultiSessionRunnerFileAsStreamedDirFails) {
  cfg_.src_dir = path::Join(test_dir_path_, "file.txt");
  EXPECT_OK(path::WriteFile(cfg_.src_dir, kData, kDataSize));

  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));
  ASSERT_FALSE(WaitForManifestUpdated(1, absl::Milliseconds(100)));
  CheckMultiSessionStartNotRecorded();
  CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{});
  EXPECT_NOT_OK(runner.Shutdown());
  EXPECT_TRUE(absl::StrContains(runner.Status().ToString(),
                                "Failed to update manifest"))
      << runner.Status().ToString();
}

// Stream an empty manifest if the streamed directory was re-created as a file.
TEST_F(MultiSessionTest,
       MultiSessionRunnerDirRecreatedAsFileSucceedsWithEmptyManifest) {
  cfg_.src_dir = path::Join(test_dir_path_, "file");
  EXPECT_OK(path::CreateDirRec(cfg_.src_dir));

  MultiSessionRunner runner(cfg_.src_dir, &data_store_, &process_factory_,
                            /*enable_stats=*/false, kTimeout, kNumThreads,
                            metrics_service_,
                            [this]() { OnManifestUpdated(); });
  {
    SCOPED_TRACE("Initialize manifest in test directory.");

    EXPECT_OK(runner.Initialize(kPort, AssetStreamServerType::kTest));
    ASSERT_TRUE(WaitForManifestUpdated(2));
    ASSERT_TRUE(metrics_service_->WaitForEvents(
        metrics::EventType::kMultiSessionStart));
    CheckMultiSessionStartRecorded(0, 0, 0);
    CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{
        GetManifestUpdateData(metrics::UpdateTrigger::kInitUpdateAll,
                              absl::StatusCode::kOk, 0, 0, 0, 0, 0, 0)});
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
  }

  {
    SCOPED_TRACE("Remove the streamed directory, the manifest becomes empty.");

    uint32_t prev_updates = num_manifest_updates_;
    EXPECT_OK(path::RemoveDirRec(cfg_.src_dir));
    ASSERT_TRUE(WaitForManifestUpdated(prev_updates + 1));
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
    CheckManifestUpdateRecorded(std::vector<metrics::ManifestUpdateData>{
        GetManifestUpdateData(metrics::UpdateTrigger::kRunningUpdateAll,
                              absl::StatusCode::kNotFound, 0, 0, 0, 0, 0, 0)});
  }

  {
    SCOPED_TRACE("Create a file in place of the directory");

    uint32_t prev_updates = num_manifest_updates_;
    EXPECT_OK(path::WriteFile(cfg_.src_dir, kData, kDataSize));
    ASSERT_TRUE(WaitForManifestUpdated(prev_updates + 2));
    ASSERT_NO_FATAL_FAILURE(ExpectManifestEquals({}, runner.ManifestId()));
    metrics::ManifestUpdateData update_data = GetManifestUpdateData(
        metrics::UpdateTrigger::kRunningUpdateAll,
        absl::StatusCode::kFailedPrecondition, 0, 0, 0, 0, 0, 0);
    CheckManifestUpdateRecorded(
        std::vector<metrics::ManifestUpdateData>{update_data, update_data});
    CheckMultiSessionStartNotRecorded();
  }

  EXPECT_OK(runner.Status());
  EXPECT_OK(runner.Shutdown());
}

}  // namespace
}  // namespace cdc_ft
