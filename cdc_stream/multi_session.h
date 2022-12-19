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

#ifndef CDC_STREAM_MULTI_SESSION_H_
#define CDC_STREAM_MULTI_SESSION_H_

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cdc_stream/asset_stream_server.h"
#include "cdc_stream/metrics_recorder.h"
#include "cdc_stream/session_config.h"
#include "common/stopwatch.h"
#include "data_store/data_store_writer.h"
#include "manifest/file_chunk_map.h"
#include "manifest/manifest_updater.h"

namespace cdc_ft {

class ProcessFactory;
class Session;
struct SessionTarget;
using ManifestUpdatedCb = std::function<void()>;

// Updates the manifest and runs a file watcher in a background thread.
class MultiSessionRunner {
 public:
  // |src_dir| is the source directory on the workstation to stream.
  // |data_store| can be passed for tests to override the default store used.
  // |process_factory| abstracts process creation.
  // |enable_stats| shows whether statistics should be derived.
  // |wait_duration| is the waiting time for changes in the streamed directory.
  // |num_updater_threads| is the thread count for the manifest updater.
  // |manifest_updated_cb| is the callback executed when a new manifest is set.
  MultiSessionRunner(
      std::string src_dir, DataStoreWriter* data_store,
      ProcessFactory* process_factory, bool enable_stats,
      absl::Duration wait_duration, uint32_t num_updater_threads,
      MultiSessionMetricsRecorder const* metrics_recorder,
      ManifestUpdatedCb manifest_updated_cb = ManifestUpdatedCb());

  ~MultiSessionRunner() = default;

  // Starts |server_| of |type| on |port|.
  absl::Status Initialize(
      int port, AssetStreamServerType type,
      ContentSentHandler content_sent = ContentSentHandler());

  // Stops updating the manifest and |server_|.
  absl::Status Shutdown() ABSL_LOCKS_EXCLUDED(mutex_);

  // Waits until a manifest is ready and the session for |instance_id| has
  // acknowledged the reception of the currently set manifest id. |fuse_timeout|
  // is the timeout for waiting for the FUSE manifest ack. The time required to
  // generate the manifest is not part of this timeout as this could take a
  // longer time for a directory with many files.
  absl::Status WaitForManifestAck(const std::string& instance_id,
                                  absl::Duration fuse_timeout);

  absl::Status Status() ABSL_LOCKS_EXCLUDED(mutex_);

  // Returns the current manifest id used by |server_|.
  ContentIdProto ManifestId() const;

 private:
  // Updates manifest if the content of the watched directory changes and
  // distributes it to subscribed gamelets.
  void Run();

  // Record MultiSessionStart event.
  void RecordMultiSessionStart(const ManifestUpdater& manifest_updater);

  // Record ManifestUpdate event.
  void RecordManifestUpdate(const ManifestUpdater& manifest_updater,
                            absl::Duration duration,
                            metrics::UpdateTrigger trigger,
                            absl::Status status);

  void SetStatus(absl::Status status) ABSL_LOCKS_EXCLUDED(mutex_);

  // Files changed callback called from FileWatcherWin.
  void OnFilesChanged() ABSL_LOCKS_EXCLUDED(mutex_);

  // Directory recreated callback called from FileWatcherWin.
  void OnDirRecreated() ABSL_LOCKS_EXCLUDED(mutex_);

  // Called during manifest update when the intermediate manifest or the final
  // manifest is available. Pushes the manifest to connected FUSEs.
  void SetManifest(const ContentIdProto& manifest_id);

  const std::string src_dir_;
  DataStoreWriter* const data_store_;
  ProcessFactory* const process_factory_;
  FileChunkMap file_chunks_;
  const absl::Duration wait_duration_;
  const uint32_t num_updater_threads_;
  const ManifestUpdatedCb manifest_updated_cb_;
  std::unique_ptr<AssetStreamServer> server_;
  std::unique_ptr<ManifestUpdater> manifest_updater_;

  // Modifications (shutdown, file changes).
  absl::Mutex mutex_;
  bool shutdown_ ABSL_GUARDED_BY(mutex_) = false;
  bool files_changed_ ABSL_GUARDED_BY(mutex_) = false;
  bool dir_recreated_ ABSL_GUARDED_BY(mutex_) = false;
  bool manifest_set_ ABSL_GUARDED_BY(mutex_) = false;
  Stopwatch files_changed_timer_ ABSL_GUARDED_BY(mutex_);
  absl::Status status_ ABSL_GUARDED_BY(mutex_);

  // Background thread that watches files and updates the manifest.
  std::unique_ptr<std::thread> thread_;

  MultiSessionMetricsRecorder const* metrics_recorder_;
};

// Manages an asset streaming session from a fixed directory on the workstation
// to an arbitrary number of gamelets.
class MultiSession {
 public:
  // Ports used by the asset streaming service for local port forwarding on
  // workstation and gamelet.
  static constexpr int kDefaultForwardPortFirst = 44433;
  static constexpr int kDefaultForwardPortLast = 44442;

  // Maximum length of cache path. We must be able to write content hashes into
  // this path:
  // <cache path>\01234567890123456789<null terminator> = 260 characters.
  static constexpr size_t kDefaultMaxCachePathLen =
      260 - 1 - ContentId::kHashSize * 2 - 1;

  // Length of the hash appended to the cache directory, exposed for testing.
  static constexpr size_t kDirHashLen = 8;

  // |src_dir| is the source directory on the workstation to stream.
  // |cfg| contains generic configuration parameters for each session.
  // |process_factory| abstracts process creation.
  // |data_store| can be passed for tests to override the default store used.
  // By default, the class uses a DiskDataStore that writes to
  // %APPDATA%\cdc-file-transfer\chunks\<dir_derived_from_src_dir> on Windows.
  MultiSession(std::string src_dir, SessionConfig cfg,
               ProcessFactory* process_factory,
               MultiSessionMetricsRecorder const* metrics_recorder,
               std::unique_ptr<DataStoreWriter> data_store = nullptr);
  ~MultiSession();

  // Initializes the data store if not overridden in the constructor and starts
  // a background thread for updating the manifest and watching file changes.
  // Does not wait for the initial manifest update to finish. Use IsRunning()
  // to determine whether it is finished.
  // Not thread-safe.
  absl::Status Initialize();

  // Stops all sessions and shuts down the server.
  // Not thread-safe.
  absl::Status Shutdown() ABSL_LOCKS_EXCLUDED(shutdownMu_);

  // Returns the |src_dir| streaming directory passed to the constructor.
  const std::string& src_dir() const { return src_dir_; }

  // Returns the status of the background thread.
  // Not thread-safe.
  absl::Status Status();

  // Starts a new streaming session to the instance described by |target| and
  // waits until the FUSE has received the initial manifest id.
  // Returns an error if a session for that instance already exists.
  // |instance_id| is a unique id for the remote instance and mount directory,
  // e.g. user@host:mount_dir.
  // |target| identifies the remote target and how to connect to it.
  // |project_id| is the project that owns the instance. Stadia only.
  // |organization_id| is organization that contains the instance. Stadia only.
  // Thread-safe.
  absl::Status StartSession(const std::string& instance_id,
                            const SessionTarget& target,
                            const std::string& project_id,
                            const std::string& organization_id)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Stops the session for the given |instance_id|.
  // Returns a NotFound error if a session for that instance does not exists.
  // Thread-safe.
  absl::Status StopSession(const std::string& instance_id)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Returns all instance ids that match the given filter. The filter may
  // contain Windows-style wildcards, e.g. *, foo* or f?o.
  // Matches are case sensitive.
  std::vector<std::string> MatchSessions(const std::string& instance_id_filter);

  // Returns true if there is an existing session for |instance_id|.
  bool HasSession(const std::string& instance_id)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Returns true if the FUSE process is up and running for an existing session
  // with ID |instance_id|.
  bool IsSessionHealthy(const std::string& instance_id)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Returns true if the MultiSession does not have any active sessions.
  bool Empty() ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // Returns the number of avtive sessions.
  uint32_t GetSessionCount() ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  // For a given source directory |dir|, e.g. "C:\path\to\game", returns a
  // sanitized version of |dir| plus a hash of |dir|, e.g.
  // "c__path_to_game_abcdef01".
  static std::string GetCacheDir(std::string dir);

  // Returns the directory where manifest chunks are cached, e.g.
  // "%APPDATA%\cdc-file-transfer\chunks\c__path_to_game_abcdef01" for
  // "C:\path\to\game".
  // The returned path is shortened to |max_len| by removing UTF8 code points
  // from the beginning of the actual cache directory (c__path...) if necessary.
  static absl::StatusOr<std::string> GetCachePath(
      const std::string& src_dir, size_t max_len = kDefaultMaxCachePathLen);

  // Record an event associated with the multi-session.
  void RecordMultiSessionEvent(metrics::DeveloperLogEvent event,
                               metrics::EventType code);

  // Record an event for a session associated with the |instance_id|.
  void RecordSessionEvent(metrics::DeveloperLogEvent event,
                          metrics::EventType code,
                          const std::string& instance_id);

 private:
  std::string src_dir_;
  SessionConfig cfg_;
  ProcessFactory* const process_factory_;
  std::unique_ptr<DataStoreWriter> data_store_;
  std::thread heartbeat_watcher_;
  absl::Mutex shutdownMu_;
  bool shutdown_ ABSL_GUARDED_BY(shutdownMu_) = false;

  // Background thread for watching file changes and updating the manifest.
  std::unique_ptr<MultiSessionRunner> runner_;

  // Local forwarding port for the asset stream service.
  int local_asset_stream_port_ = 0;

  // Maps instance id to sessions.
  std::unordered_map<std::string, std::unique_ptr<Session>> sessions_
      ABSL_GUARDED_BY(sessions_mutex_);
  absl::Mutex sessions_mutex_;

  MultiSessionMetricsRecorder const* metrics_recorder_;

  Session* FindSession(const std::string& instance_id)
      ABSL_LOCKS_EXCLUDED(sessions_mutex_);

  void OnContentSent(size_t byte_count, size_t chunck_count,
                     std::string instance_id);

  void StartHeartBeatCheck();
};

}  // namespace cdc_ft

#endif  // CDC_STREAM_MULTI_SESSION_H_
