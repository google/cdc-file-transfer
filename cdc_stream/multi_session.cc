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

#include "cdc_stream/session.h"
#include "common/file_watcher_win.h"
#include "common/log.h"
#include "common/path.h"
#include "common/path_filter.h"
#include "common/platform.h"
#include "common/port_manager.h"
#include "common/process.h"
#include "common/util.h"
#include "data_store/disk_data_store.h"
#include "manifest/content_id.h"
#include "manifest/manifest_iterator.h"
#include "manifest/manifest_printer.h"
#include "manifest/manifest_proto_defs.h"
#include "metrics/enums.h"
#include "metrics/messages.h"

namespace cdc_ft {
namespace {

// Stats output period (if enabled).
constexpr double kStatsPrintDelaySec = 0.1f;

ManifestUpdater::Operator FileWatcherActionToOperation(
    FileWatcherWin::FileAction action) {
  switch (action) {
    case FileWatcherWin::FileAction::kAdded:
      return ManifestUpdater::Operator::kAdd;
    case FileWatcherWin::FileAction::kModified:
      return ManifestUpdater::Operator::kUpdate;
    case FileWatcherWin::FileAction::kDeleted:
      return ManifestUpdater::Operator::kDelete;
  }
  // The switch must cover all actions.
  LOG_ERROR("Unhandled action: %d", static_cast<int>(action));
  assert(false);
  return ManifestUpdater::Operator::kAdd;
}

// Converts |modified_files| (as returned from the file watcher) into an
// OperationList (as required by the manifest updater).
ManifestUpdater::OperationList GetFileOperations(
    const FileWatcherWin::FileMap& modified_files) {
  AssetInfo ai;
  ManifestUpdater::OperationList ops;
  ops.reserve(modified_files.size());
  for (const auto& [path, info] : modified_files) {
    ai.path = path;
    ai.type = info.is_dir ? AssetProto::DIRECTORY : AssetProto::FILE;
    ai.size = info.size;
    ai.mtime = info.mtime;
    ops.emplace_back(FileWatcherActionToOperation(info.action), std::move(ai));
  }
  return ops;
}

}  // namespace

MultiSessionRunner::MultiSessionRunner(
    std::string src_dir, DataStoreWriter* data_store,
    ProcessFactory* process_factory, bool enable_stats,
    absl::Duration wait_duration, uint32_t num_updater_threads,
    MultiSessionMetricsRecorder const* metrics_recorder,
    ManifestUpdatedCb manifest_updated_cb)
    : src_dir_(std::move(src_dir)),
      data_store_(data_store),
      process_factory_(process_factory),
      file_chunks_(enable_stats),
      wait_duration_(wait_duration),
      num_updater_threads_(num_updater_threads),
      manifest_updated_cb_(std::move(manifest_updated_cb)),
      metrics_recorder_(metrics_recorder) {
  assert(metrics_recorder_);
}

absl::Status MultiSessionRunner::Initialize(int port,
                                            AssetStreamServerType type,
                                            ContentSentHandler content_sent) {
  // Create the manifest updater.
  UpdaterConfig cfg;
  cfg.num_threads = num_updater_threads_;
  cfg.src_dir = src_dir_;

  assert(!manifest_updater_);
  manifest_updater_ =
      std::make_unique<ManifestUpdater>(data_store_, std::move(cfg));

  // Let the manifest updater handle requests to prioritize certain assets.
  PrioritizeAssetsHandler prio_assets =
      std::bind(&ManifestUpdater::AddPriorityAssets, manifest_updater_.get(),
                std::placeholders::_1);

  // Start the server.
  assert(!server_);
  server_ = AssetStreamServer::Create(type, src_dir_, data_store_,
                                      &file_chunks_, std::move(content_sent),
                                      std::move(prio_assets));
  assert(server_);
  RETURN_IF_ERROR(server_->Start(port),
                  "Failed to start asset stream server for '%s'", src_dir_);

  assert(!thread_);
  thread_ = std::make_unique<std::thread>([this]() { Run(); });

  return absl::OkStatus();
}

absl::Status MultiSessionRunner::Shutdown() {
  // Send shutdown signal.
  {
    absl::MutexLock lock(&mutex_);
    shutdown_ = true;
  }
  if (thread_) {
    if (thread_->joinable()) thread_->join();
    thread_.reset();
  }

  // Shut down asset stream server.
  if (server_) {
    server_->Shutdown();
    server_.reset();
  }

  return status_;
}

absl::Status MultiSessionRunner::WaitForManifestAck(
    const std::string& instance_id, absl::Duration fuse_timeout) {
  {
    absl::MutexLock lock(&mutex_);

    LOG_INFO("Waiting for manifest to be available");
    auto cond = [this]() { return manifest_set_ || !status_.ok(); };
    mutex_.Await(absl::Condition(&cond));

    if (!status_.ok())
      return WrapStatus(status_, "Failed to set up streaming session for '%s'",
                        src_dir_);
  }

  LOG_INFO("Waiting for FUSE ack");
  assert(server_);
  RETURN_IF_ERROR(server_->WaitForManifestAck(instance_id, fuse_timeout));

  return absl::OkStatus();
}

absl::Status MultiSessionRunner::Status() {
  absl::MutexLock lock(&mutex_);
  return status_;
}

ContentIdProto MultiSessionRunner::ManifestId() const {
  assert(server_);
  return server_->GetManifestId();
}

void MultiSessionRunner::Run() {
  // Set up file watcher.
  // The streamed path should be a directory and exist at the beginning.
  FileWatcherWin watcher(src_dir_);
  absl::Status status = watcher.StartWatching([this]() { OnFilesChanged(); },
                                              [this]() { OnDirRecreated(); });
  if (!status.ok()) {
    SetStatus(
        WrapStatus(status, "Failed to update manifest for '%s'", src_dir_));
    return;
  }

  // Push the intermediate manifest(s) and the final version with this handler.
  auto push_handler = [this](const ContentIdProto& manifest_id) {
    SetManifest(manifest_id);
  };

  // Bring the manifest up to date.
  LOG_INFO("Updating manifest for '%s'...", src_dir_);
  Stopwatch sw;
  status = manifest_updater_->UpdateAll(&file_chunks_, push_handler);
  RecordManifestUpdate(*manifest_updater_, sw.Elapsed(),
                       metrics::UpdateTrigger::kInitUpdateAll, status);
  if (!status.ok()) {
    SetStatus(
        WrapStatus(status, "Failed to update manifest for '%s'", src_dir_));
    return;
  }
  RecordMultiSessionStart(*manifest_updater_);
  LOG_INFO("Manifest for '%s' updated in %0.3f seconds", src_dir_,
           sw.ElapsedSeconds());

  while (!shutdown_) {
    FileWatcherWin::FileMap modified_files;
    bool clean_manifest = false;
    {
      // Wait for changes.
      absl::MutexLock lock(&mutex_);

      bool prev_files_changed = files_changed_;
      absl::Duration timeout =
          absl::Seconds(file_chunks_.HasStats() ? kStatsPrintDelaySec : 3600.0);
      if (files_changed_) {
        timeout = std::max(wait_duration_ - files_changed_timer_.Elapsed(),
                           absl::Milliseconds(1));
      } else {
        files_changed_timer_.Reset();
      }
      auto cond = [this]() {
        return shutdown_ || files_changed_ || dir_recreated_;
      };
      mutex_.AwaitWithTimeout(absl::Condition(&cond), timeout);

      // If |files_changed_| became true, wait some more time before updating
      // the manifest.
      if (!prev_files_changed && files_changed_) files_changed_timer_.Reset();

      // Shut down.
      if (shutdown_) {
        LOG_INFO("MultiSession('%s'): Shutting down", src_dir_);
        break;
      }

      // Pick up modified files.
      if (!dir_recreated_ && files_changed_ &&
          files_changed_timer_.Elapsed() > wait_duration_) {
        modified_files = watcher.GetModifiedFiles();
        files_changed_ = false;
        files_changed_timer_.Reset();
      }

      if (dir_recreated_) {
        clean_manifest = true;
        dir_recreated_ = false;
      }
    }  // mutex_ lock

    if (clean_manifest) {
      LOG_DEBUG(
          "Streamed directory '%s' was possibly re-created or not all changes "
          "were detected, re-building the manifest",
          src_dir_);
      modified_files.clear();
      sw.Reset();
      status = manifest_updater_->UpdateAll(&file_chunks_, push_handler);
      RecordManifestUpdate(*manifest_updater_, sw.Elapsed(),
                           metrics::UpdateTrigger::kRunningUpdateAll, status);
      if (!status.ok()) {
        LOG_WARNING(
            "Updating manifest for '%s' after re-creating directory failed: "
            "'%s'",
            src_dir_, status.ToString());
        SetManifest(manifest_updater_->DefaultManifestId());
      }
    } else if (!modified_files.empty()) {
      ManifestUpdater::OperationList ops = GetFileOperations(modified_files);
      sw.Reset();
      status = manifest_updater_->Update(&ops, &file_chunks_, push_handler);
      RecordManifestUpdate(*manifest_updater_, sw.Elapsed(),
                           metrics::UpdateTrigger::kRegularUpdate, status);
      if (!status.ok()) {
        LOG_WARNING("Updating manifest for '%s' failed: %s", src_dir_,
                    status.ToString());
        SetManifest(manifest_updater_->DefaultManifestId());
      }
    }

    // Update stats output.
    file_chunks_.PrintStats();
  }
}

void MultiSessionRunner::RecordManifestUpdate(
    const ManifestUpdater& manifest_updater, absl::Duration duration,
    metrics::UpdateTrigger trigger, absl::Status status) {
  metrics::DeveloperLogEvent evt;
  evt.as_manager_data = std::make_unique<metrics::AssetStreamingManagerData>();
  evt.as_manager_data->manifest_update_data =
      std::make_unique<metrics::ManifestUpdateData>();
  evt.as_manager_data->manifest_update_data->local_duration_ms =
      absl::ToInt64Milliseconds(duration);
  evt.as_manager_data->manifest_update_data->status = status.code();
  evt.as_manager_data->manifest_update_data->trigger = trigger;
  const UpdaterStats& stats = manifest_updater.Stats();
  evt.as_manager_data->manifest_update_data->total_assets_added_or_updated =
      stats.total_assets_added_or_updated;
  evt.as_manager_data->manifest_update_data->total_assets_deleted =
      stats.total_assets_deleted;
  evt.as_manager_data->manifest_update_data->total_chunks = stats.total_chunks;
  evt.as_manager_data->manifest_update_data->total_files_added_or_updated =
      stats.total_files_added_or_updated;
  evt.as_manager_data->manifest_update_data->total_files_failed =
      stats.total_files_failed;
  evt.as_manager_data->manifest_update_data->total_processed_bytes =
      stats.total_processed_bytes;
  metrics_recorder_->RecordEvent(std::move(evt),
                                 metrics::EventType::kManifestUpdated);
}

void MultiSessionRunner::RecordMultiSessionStart(
    const ManifestUpdater& manifest_updater) {
  metrics::DeveloperLogEvent evt;
  evt.as_manager_data = std::make_unique<metrics::AssetStreamingManagerData>();
  evt.as_manager_data->multi_session_start_data =
      std::make_unique<metrics::MultiSessionStartData>();
  ManifestIterator manifest_iter(data_store_);
  absl::Status status = manifest_iter.Open(manifest_updater.ManifestId());
  if (status.ok()) {
    const AssetProto* entry = &manifest_iter.Manifest().root_dir();
    uint32_t file_count = 0;
    uint64_t total_chunks = 0;
    uint64_t total_processed_bytes = 0;
    do {
      if (entry->type() == AssetProto::FILE) {
        ++file_count;
        total_chunks += entry->file_chunks_size();
        total_processed_bytes += entry->file_size();
        for (const IndirectChunkListProto& icl :
             entry->file_indirect_chunks()) {
          ChunkListProto list;
          status = data_store_->GetProto(icl.chunk_list_id(), &list);
          if (status.ok()) {
            total_chunks += list.chunks_size();
          } else {
            LOG_WARNING("Could not get proto by id: '%s'. %s",
                        ContentId::ToHexString(icl.chunk_list_id()),
                        status.ToString());
          }
        }
      }
    } while ((entry = manifest_iter.NextEntry()) != nullptr);
    evt.as_manager_data->multi_session_start_data->file_count = file_count;
    evt.as_manager_data->multi_session_start_data->chunk_count = total_chunks;
    evt.as_manager_data->multi_session_start_data->byte_count =
        total_processed_bytes;
  } else {
    LOG_WARNING("Could not open manifest by id: '%s'. %s",
                ContentId::ToHexString(manifest_updater.ManifestId()),
                status.ToString());
  }
  evt.as_manager_data->multi_session_start_data->min_chunk_size =
      static_cast<uint64_t>(manifest_updater.Config().min_chunk_size);
  evt.as_manager_data->multi_session_start_data->avg_chunk_size =
      static_cast<uint64_t>(manifest_updater.Config().avg_chunk_size);
  evt.as_manager_data->multi_session_start_data->max_chunk_size =
      static_cast<uint64_t>(manifest_updater.Config().max_chunk_size);
  metrics_recorder_->RecordEvent(std::move(evt),
                                 metrics::EventType::kMultiSessionStart);
}

void MultiSessionRunner::SetStatus(absl::Status status)
    ABSL_LOCKS_EXCLUDED(mutex_) {
  absl::MutexLock lock(&mutex_);
  status_ = std::move(status);
}

void MultiSessionRunner::OnFilesChanged() {
  absl::MutexLock lock(&mutex_);
  files_changed_ = true;
}

void MultiSessionRunner::OnDirRecreated() {
  absl::MutexLock lock(&mutex_);
  dir_recreated_ = true;
}

void MultiSessionRunner::SetManifest(const ContentIdProto& manifest_id) {
  server_->SetManifestId(manifest_id);
  if (Log::Instance()->GetLogLevel() <= LogLevel::kVerbose) {
    ManifestPrinter printer;
    ManifestProto manifest_proto;
    absl::Status status = data_store_->GetProto(manifest_id, &manifest_proto);
    std::string manifest_text;
    printer.PrintToString(manifest_proto, &manifest_text);
    if (status.ok()) {
      LOG_DEBUG("Set manifest '%s'\n'%s'", ContentId::ToHexString(manifest_id),
                manifest_text);
    } else {
      LOG_WARNING("Could not retrieve manifest from the data store '%s'",
                  ContentId::ToHexString(manifest_id));
    }
  }

  // Notify thread that starts the streaming session that a manifest has been
  // set.
  absl::MutexLock lock(&mutex_);
  manifest_set_ = true;
  if (manifest_updated_cb_) {
    manifest_updated_cb_();
  }
}

MultiSession::MultiSession(std::string src_dir, SessionConfig cfg,
                           ProcessFactory* process_factory,
                           MultiSessionMetricsRecorder const* metrics_recorder,
                           std::unique_ptr<DataStoreWriter> data_store)
    : src_dir_(src_dir),
      cfg_(cfg),
      process_factory_(process_factory),
      data_store_(std::move(data_store)),
      metrics_recorder_(metrics_recorder) {
  assert(metrics_recorder_);
}

MultiSession::~MultiSession() {
  absl::Status status = Shutdown();
  if (!status.ok()) {
    LOG_ERROR("Shutdown streaming from '%s' failed: %s", src_dir_,
              status.ToString());
  }
}

absl::Status MultiSession::Initialize() {
  // |data_store_| is not set in production, but it can be overridden for tests.
  if (!data_store_) {
    std::string cache_path;
    ASSIGN_OR_RETURN(cache_path, GetCachePath(src_dir_));
    ASSIGN_OR_RETURN(data_store_,
                     DiskDataStore::Create(/*depth=*/0, cache_path,
                                           /*create_dirs=*/true),
                     "Failed to create data store for '%s'", cache_path);
  }

  // Find an available local port.
  local_asset_stream_port_ = cfg_.forward_port_first;
  if (cfg_.forward_port_first < cfg_.forward_port_last) {
    std::unordered_set<int> ports;
    ASSIGN_OR_RETURN(
        ports,
        PortManager::FindAvailableLocalPorts(
            cfg_.forward_port_first, cfg_.forward_port_last,
            ArchType::kWindows_x86_64, process_factory_),
        "Failed to find an available local port in the range [%d, %d]",
        cfg_.forward_port_first, cfg_.forward_port_last);
    assert(!ports.empty());
    local_asset_stream_port_ = *ports.begin();
  }

  assert(!runner_);
  runner_ = std::make_unique<MultiSessionRunner>(
      src_dir_, data_store_.get(), process_factory_, cfg_.stats,
      absl::Milliseconds(cfg_.file_change_wait_duration_ms),
      cfg_.manifest_updater_threads, metrics_recorder_);
  RETURN_IF_ERROR(runner_->Initialize(
                      local_asset_stream_port_, AssetStreamServerType::kGrpc,
                      [this](uint64_t bc, uint64_t cc, std::string id) {
                        this->OnContentSent(bc, cc, id);
                      }),
                  "Failed to initialize session runner");
  StartHeartBeatCheck();
  return absl::OkStatus();
}

absl::Status MultiSession::Shutdown() {
  // Stop all sessions.
  // TODO: Record error on multi-session end.
  metrics_recorder_->RecordEvent(metrics::DeveloperLogEvent(),
                                 metrics::EventType::kMultiSessionEnd);
  {
    absl::WriterMutexLock lock(&shutdownMu_);
    shutdown_ = true;
  }
  while (!sessions_.empty()) {
    std::string instance_id = sessions_.begin()->first;
    RETURN_IF_ERROR(StopSession(instance_id),
                    "Failed to stop session for instance id '%s'", instance_id);
    sessions_.erase(instance_id);
  }

  absl::Status status;
  if (runner_) {
    status = runner_->Shutdown();
  }

  if (heartbeat_watcher_.joinable()) {
    heartbeat_watcher_.join();
  }

  return status;
}

absl::Status MultiSession::Status() {
  return runner_ ? runner_->Status() : absl::OkStatus();
}

absl::Status MultiSession::StartSession(const std::string& instance_id,
                                        const SessionTarget& target,
                                        const std::string& project_id,
                                        const std::string& organization_id) {
  absl::MutexLock lock(&sessions_mutex_);

  if (sessions_.find(instance_id) != sessions_.end()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Session for instance id '%s' already exists", instance_id));
  }

  if (!runner_)
    return absl::FailedPreconditionError("MultiSession not started");

  absl::Status runner_status = runner_->Status();
  if (!runner_status.ok()) {
    return WrapStatus(runner_status,
                      "Failed to set up streaming session for '%s'", src_dir_);
  }

  auto metrics_recorder = std::make_unique<SessionMetricsRecorder>(
      metrics_recorder_->GetMetricsService(),
      metrics_recorder_->MultiSessionId(), project_id, organization_id);

  auto session = std::make_unique<Session>(
      instance_id, target, cfg_, process_factory_, std::move(metrics_recorder));
  RETURN_IF_ERROR(session->Start(local_asset_stream_port_,
                                 cfg_.forward_port_first,
                                 cfg_.forward_port_last));

  // Wait for the FUSE to receive the first intermediate manifest.
  RETURN_IF_ERROR(runner_->WaitForManifestAck(instance_id, absl::Seconds(5)));

  sessions_[instance_id] = std::move(session);
  return absl::OkStatus();
}

absl::Status MultiSession::StopSession(const std::string& instance_id) {
  absl::MutexLock lock(&sessions_mutex_);

  if (sessions_.find(instance_id) == sessions_.end()) {
    return absl::NotFoundError(
        absl::StrFormat("No session for instance id '%s' found", instance_id));
  }

  if (!runner_)
    return absl::FailedPreconditionError("MultiSession not started");

  RETURN_IF_ERROR(sessions_[instance_id]->Stop());
  sessions_.erase(instance_id);
  return absl::OkStatus();
}

bool MultiSession::HasSession(const std::string& instance_id) {
  absl::ReaderMutexLock lock(&sessions_mutex_);
  return sessions_.find(instance_id) != sessions_.end();
}

std::vector<std::string> MultiSession::MatchSessions(
    const std::string& instance_id_filter) {
  PathFilter filter;
  filter.AddRule(PathFilter::Rule::Type::kInclude, instance_id_filter);
  filter.AddRule(PathFilter::Rule::Type::kExclude, "*");

  std::vector<std::string> matches;
  for (const auto& [instance_id, session] : sessions_) {
    if (filter.IsMatch(instance_id)) {
      matches.push_back(instance_id);
    }
  }
  return matches;
}

bool MultiSession::IsSessionHealthy(const std::string& instance_id) {
  absl::ReaderMutexLock lock(&sessions_mutex_);
  auto iter = sessions_.find(instance_id);
  if (iter == sessions_.end()) return false;
  Session* session = iter->second.get();
  assert(session);
  return session->IsHealthy();
}

bool MultiSession::Empty() {
  absl::ReaderMutexLock lock(&sessions_mutex_);
  return sessions_.empty();
}

uint32_t MultiSession::GetSessionCount() {
  absl::ReaderMutexLock lock(&sessions_mutex_);
  return static_cast<uint32_t>(sessions_.size());
}

// static
std::string MultiSession::GetCacheDir(std::string dir) {
  // Get full path, or else "..\foo" and "C:\foo" are treated differently, even
  // if they map to the same directory.
  dir = path::GetFullPath(dir);
#if PLATFORM_WINDOWS
  // On Windows, casing is ignored.
  std::for_each(dir.begin(), dir.end(), [](char& c) { c = tolower(c); });
#endif
  path::EnsureEndsWithPathSeparator(&dir);
  dir = path::ToUnix(std::move(dir));
  ContentIdProto id = ContentId::FromDataString(dir);

  // Replace invalid characters by _.
  std::for_each(dir.begin(), dir.end(), [](char& c) {
    static constexpr char invalid_chars[] = "<>:\"/\\|?*";
    if (strchr(invalid_chars, c)) c = '_';
  });

  return dir + ContentId::ToHexString(id).substr(0, kDirHashLen);
}

// static
absl::StatusOr<std::string> MultiSession::GetCachePath(
    const std::string& src_dir, size_t max_len) {
  std::string appdata_path;
#if PLATFORM_WINDOWS
  RETURN_IF_ERROR(
      path::GetKnownFolderPath(path::FolderId::kRoamingAppData, &appdata_path),
      "Failed to get roaming appdata path");
#elif PLATFORM_LINUX
  RETURN_IF_ERROR(path::GetEnv("HOME", &appdata_path));
  path::Append(&appdata_path, ".cache");
#endif

  std::string base_dir =
      path::Join(appdata_path, "cdc-file-transfer", "chunks");
  std::string cache_dir = GetCacheDir(src_dir);

  size_t total_size = base_dir.size() + 1 + cache_dir.size();
  if (total_size <= max_len) return path::Join(base_dir, cache_dir);

  // Path needs to be shortened. Remove |to_remove| many chars from the
  // beginning of |cache_dir|, but keep the hash (last kDirHashLen bytes).
  size_t to_remove = total_size - max_len;
  assert(cache_dir.size() >= kDirHashLen);
  if (to_remove > cache_dir.size() - kDirHashLen)
    to_remove = cache_dir.size() - kDirHashLen;

  // Remove UTF8 code points from the beginning.
  size_t start = 0;
  while (start < to_remove) {
    int codepoint_len = Util::Utf8CodePointLen(cache_dir.data() + start);
    // For invalid code points (codepoint_len == 0), just eat byte by byte.
    start += std::max(codepoint_len, 1);
  }

  assert(start + kDirHashLen <= cache_dir.size());
  return path::Join(base_dir, cache_dir.substr(start));
}

void MultiSession::RecordMultiSessionEvent(metrics::DeveloperLogEvent event,
                                           metrics::EventType code) {
  metrics_recorder_->RecordEvent(std::move(event), code);
}

void MultiSession::RecordSessionEvent(metrics::DeveloperLogEvent event,
                                      metrics::EventType code,
                                      const std::string& instance_id) {
  Session* session = FindSession(instance_id);
  if (session) {
    session->RecordEvent(std::move(event), code);
  }
}

Session* MultiSession::FindSession(const std::string& instance_id) {
  absl::ReaderMutexLock lock(&sessions_mutex_);
  auto session_it = sessions_.find(instance_id);
  if (session_it == sessions_.end()) {
    return nullptr;
  }
  return session_it->second.get();
}

void MultiSession::OnContentSent(size_t byte_count, size_t chunck_count,
                                 std::string instance_id) {
  if (instance_id.empty()) {
    // |instance_id| is empty only in case when manifest wasn't acknowledged by
    // the instance yet (ConfigStreamServiceImpl::AckManifestIdReceived was not
    // invoked). This means MultiSession::StartSession is still waiting for
    // manifest acknowledge and |sessions_mutex_| is currently locked. In this
    // case invoking MultiSession::FindSession and waiting for |sessions_mutex_|
    // to get unlocked will block the current thread, which is also responsible
    // for receiving a call at ConfigStreamServiceImpl::AckManifestIdReceived.
    // This causes a deadlock and leads to a DeadlineExceeded error.
    LOG_WARNING("Cannot record session content for an empty instance_id.");
    return;
  }
  Session* session = FindSession(instance_id);
  if (session == nullptr) {
    LOG_WARNING("Failed to find active session by instance id '%s'",
                instance_id);
    return;
  }
  session->OnContentSent(byte_count, chunck_count);
}

void MultiSession::StartHeartBeatCheck() {
  heartbeat_watcher_ = std::thread([this]() ABSL_LOCKS_EXCLUDED(shutdownMu_) {
    auto cond = [this]() { return shutdown_; };
    while (!shutdownMu_.LockWhenWithTimeout(absl::Condition(&cond),
                                            absl::Minutes(5))) {
      absl::ReaderMutexLock lock(&sessions_mutex_);
      for (auto it = sessions_.begin(); it != sessions_.end(); ++it) {
        it->second->RecordHeartBeatIfChanged();
      }
      shutdownMu_.Unlock();
    }
    shutdownMu_.Unlock();
  });
}

}  // namespace cdc_ft
