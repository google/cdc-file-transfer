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

#include "cdc_fuse_fs/cdc_fuse_fs.h"

#include <assert.h>
#include <fcntl.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <unordered_map>

#include "cdc_fuse_fs/asset.h"
#include "common/buffer.h"
#include "common/log.h"
#include "common/path.h"
#include "common/platform.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/threadpool.h"
#include "common/util.h"
#include "data_store/data_store_reader.h"
#include "json/json.h"
#include "manifest/content_id.h"
#include "manifest/manifest_proto_defs.h"

#ifndef USE_MOCK_LIBFUSE
#include "include/fuse.h"
#include "include/fuse_lowlevel.h"
#else
// This code is tested using a fake testing FUSE layer.
#include "cdc_fuse_fs/mock_libfuse.h"
#endif

namespace cdc_ft {
namespace {

enum class InodeState {
  kInitialized,
  kUpdatedProto,  // inode's proto was changed, but the content is the same.
  kUpdated,       // inode was updated and its file should be reopened.
  kInvalid        // the corresponding file was removed.
};

const char* InodeStateToString(const InodeState& state) {
  switch (state) {
    case InodeState::kInitialized:
      return "INITIALIZED";
    case InodeState::kUpdatedProto:
      return "UPDATED_PROTO";
    case InodeState::kUpdated:
      return "UPDATED";
    case InodeState::kInvalid:
      return "INVALID";
    default:
      return "UNKNOWN";
  }
}

struct Inode;

fuse_ino_t GetIno(const Inode& inode);

struct Inode {
  Asset asset;

  // Inode nlookup: how many times the file was accessed. It is reduced by
  // forget(). The inode is removed if nlookup = 0 and children_nlookup = 0.
  std::atomic_uint64_t nlookup{0};

  // The number of accessed children (used for directories), whose nlookup > 0.
  std::atomic_uint64_t children_nlookup{0};

  // Shows if this inode is a FUSE root inode.
  bool is_root = false;

  // The state during manifest swap.
  std::atomic<InodeState> state{InodeState::kInitialized};

  Inode() = default;

  // Delete copy/move constructor and assignments. We don't need any.
  Inode(const Inode&) = delete;
  Inode(Inode&& inode) = delete;
  Inode& operator=(Inode&& inode) = delete;
  Inode& operator=(const Inode&) = delete;

  bool IsInitialized() const { return state == InodeState::kInitialized; }
  bool IsUpdated() const { return state == InodeState::kUpdated; }
  bool IsUpdatedProto() const { return state == InodeState::kUpdatedProto; }
  bool IsValid() const { return state != InodeState::kInvalid; }

  Json::Value ToJson(bool with_proto) const {
    Json::Value value;
    value["ino"] = GetIno(*this);
    value["parent_ino"] = asset.parent_ino();
    value["nlookup"] = nlookup.load();
    value["children_nlookup"] = children_nlookup.load();
    value["state"] = InodeStateToString(state);
    value["proto"] = asset.proto();
    if (with_proto) {
      if (asset.proto()) {
        value["name"] = asset.proto()->name();
        value["type"] = asset.proto()->type();
      } else {
        value["message"] = "Proto message is not set";
      }
    }
    return value;
  }
};

// Asset proto -> inode map.
using InodeMap = std::unordered_map<const AssetProto*, std::shared_ptr<Inode>>;

// Queued request that cannot be processed yet and should be processed once the
// manifest is updated.
struct QueuedRequest {
  // The request type that was blocked.
  enum class Type { kOpen, kOpenDir, kLookup };
  Type type;
  std::string rel_path;

  union {
    // Only valid for type == kOpen or type == kOpenDir.
    struct Open {
      fuse_req_t req;
      fuse_ino_t ino;
      struct fuse_file_info fi;
    } open;
    // Only valid for type == kLookup.
    struct Lookup {
      fuse_req_t req;
      fuse_ino_t parent_ino;
      const char* name;
    } lookup;
  } u;
};

// Global context. Fuse is based on loose callbacks, so this holds the fs state.
struct CdcFuseFsContext {
#ifndef USE_MOCK_LIBFUSE
  // Fuse state.
  fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  fuse_chan* channel = nullptr;
  char* mountpoint = nullptr;
  fuse_session* session = nullptr;
  bool signal_handlers_set = false;
  int multithreaded = 1;
#endif
  bool initialized = false;

  // Interface for loading chunks (assets, data etc.).
  DataStoreReader* data_store_reader = nullptr;

  // Mutex to protect manifest update process.
  absl::Mutex manifest_mutex ABSL_ACQUIRED_BEFORE(inodes_mutex);

  // Loaded manifest.
  std::unique_ptr<ManifestProto> manifest ABSL_GUARDED_BY(manifest_mutex) =
      std::make_unique<ManifestProto>();

  // Root inode (points to manifest->root_dir()).
  std::shared_ptr<Inode> root ABSL_GUARDED_BY(manifest_mutex) =
      std::make_shared<Inode>();

  // Mutex to protect inodes.
  absl::Mutex inodes_mutex ABSL_ACQUIRED_AFTER(manifest_mutex);

  // Maps asset protos to Inodes, which contains the proto + metadata.
  InodeMap inodes ABSL_GUARDED_BY(inodes_mutex);

  // One buffer per thread to serve read, readdir etc. requests.
  static thread_local Buffer buffer;

  // Configuration client to get configuration updates from the workstation.
  std::unique_ptr<ConfigStreamClient> config_stream_client;

  // Queue for requests to open files or directories that have not been
  // processed yet.
  absl::Mutex queued_requests_mutex;
  std::vector<QueuedRequest> queued_requests
      ABSL_GUARDED_BY(queued_requests_mutex);

  // Identifies whether FUSE consistency should be inspected after manifest
  // update.
  bool consistency_check = false;

  // Contains invalid inodes, which should be deleted after they are forgotten.
  std::unordered_map<fuse_ino_t, std::shared_ptr<Inode>> invalid_inodes
      ABSL_GUARDED_BY(inodes_mutex);
};

thread_local Buffer CdcFuseFsContext::buffer;

// Global context for the (static!) Fuse callbacks.
CdcFuseFsContext* ctx;

// Inode IDs (fuse_ino_t) are just the Inode pointer addresses.
// That allows quick lock-free access to inodes.
static_assert(sizeof(Inode*) == sizeof(fuse_ino_t), "Size mismatch!");

#ifndef USE_MOCK_LIBFUSE
// Sanity check for correct compiler options.
// Note: There doesn't seem to be a way to make this 64 bit on Windows in a way
//       that doesn't cause havoc (but that's for testing only, anyway).
static_assert(sizeof(off_t) == 8, "off_t must be 64 bit");
static_assert(sizeof(ino_t) == 8, "ino_t must be 64 bit");
static_assert(sizeof(stat::st_ino) == 8, "st_ino must be 64 bit");
#endif

// Converts Inode to fuse_ino_t (cheap typecast).
fuse_ino_t GetIno(const Inode& inode) {
  if (inode.is_root) {
    return FUSE_ROOT_ID;
  }
  return reinterpret_cast<fuse_ino_t>(&inode);
}

// Converts fuse_ino_t to Inode (root inode for FUSE_ROOT_ID, otherwise cheap
// typecast).
Inode& GetInode(fuse_ino_t ino)
    ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex) {
  if (ino == FUSE_ROOT_ID) {
    return *ctx->root;
  }

  // |ino| is just the inode pointer.
  return *reinterpret_cast<Inode*>(ino);
}

// Converts asset.permissions() to a file mode by OR'ing the file type flag.
uint32_t GetMode(const AssetProto& asset) {
  switch (asset.type()) {
    case AssetProto::FILE:
      return asset.permissions() | path::MODE_IFREG;
    case AssetProto::DIRECTORY:
      return asset.permissions() | path::MODE_IFDIR;
    default:
      return asset.permissions();
  }
}

// Fills |stbuf| with data from the asset pointed to by |ino|.
void FillStatBuffer(fuse_ino_t ino, struct stat* stbuf)
    ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex) {
  assert(stbuf);
  const AssetProto& asset = *GetInode(ino).asset.proto();
  stbuf->st_ino = ino;
  stbuf->st_mode = GetMode(asset);
  // For directories, this is going to be 0 (does that matter?).
  stbuf->st_size = asset.file_size();
  // Number of hard links to the file (number of directories with entries for
  // this file). Should always be 1 for this read-only filesystem.
  stbuf->st_nlink = internal::kCdcFuseDefaultNLink;
#ifndef USE_MOCK_LIBFUSE
  stbuf->st_mtim.tv_sec = asset.mtime_seconds();
#else
  stbuf->st_mtime = asset.mtime_seconds();
#endif
  stbuf->st_uid = internal::kCdcFuseCloudcastUid;
  stbuf->st_gid = internal::kCdcFuseCloudcastGid;

  LOG_DEBUG("FillStatBuffer, ino=%u, size=%u, mode=%u, time=%u", ino,
            stbuf->st_size, stbuf->st_mode, asset.mtime_seconds());
}

// Gets or creates an inode for |proto|.
Inode* GetOrCreateInode(Inode& parent, const AssetProto* proto)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->inodes_mutex) {
  std::shared_ptr<Inode>& inode = ctx->inodes[proto];
  if (inode) {
    assert(inode->asset.proto());
    // Found existing inode.
    ++inode->nlookup;
  } else {
    // A new inode was created.
    // Note: No other thread can access this node right now.
    inode = std::make_shared<Inode>();
    inode->asset.Initialize(GetIno(parent), ctx->data_store_reader, proto);
    inode->nlookup = 1;
    ++parent.children_nlookup;
  }
  return inode.get();
}

// Adds an entry with given |name| and stat info from the asset at the given
// |ino|. Usually, |name| matches the asset name, except for the "." and ".."
// directories. Stores the entry in some Fuse-internal format in |buffer|.
void AddDirectoryEntry(fuse_req_t req, Buffer* buffer, const char* name,
                       fuse_ino_t ino)
    ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex) {
  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));

  // Note: fuse_add_direntry() only uses those two entries.
  stbuf.st_ino = ino;
  stbuf.st_mode = GetMode(*GetInode(ino).asset.proto());

  // Call fuse_add_direntry with null args to get the size of the entry.
  size_t old_size = buffer->size();
  size_t entry_size = fuse_add_direntry(req, NULL, 0, name, NULL, 0);

  // Append the new entry at the end of the buffer.
  buffer->resize(old_size + entry_size);
  fuse_add_direntry(req, buffer->data() + old_size, buffer->size() - old_size,
                    name, &stbuf, static_cast<off_t>(buffer->size()));
}

void ForgetChild(fuse_ino_t ino) ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->inodes_mutex) {
  Inode& inode = GetInode(ino);
  assert(inode.children_nlookup > 0);
  --inode.children_nlookup;

  // Maintain children_nlookup on the root, but never remove it.
  if (ino == FUSE_ROOT_ID) {
    return;
  }
  if (inode.nlookup == 0 && inode.children_nlookup == 0) {
    const AssetProto* proto = inode.asset.proto();
    ForgetChild(inode.asset.parent_ino());
    ctx->inodes.erase(proto);
  }
}

void ForgetOne(fuse_ino_t ino, uint64_t nlookup)
    ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex)
        ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->inodes_mutex) {
  // Supports forgetting outdated inodes - do not need to check validity.
  Inode& inode = GetInode(ino);
  LOG_DEBUG("Current nlookup %u to reduce by %u", inode.nlookup.load(),
            nlookup);
  inode.nlookup = inode.nlookup > nlookup ? inode.nlookup - nlookup : 0;
  // Maintain nlookup on the root, but never remove it.
  if (ino == FUSE_ROOT_ID) {
    return;
  }
  if (inode.nlookup == 0 && inode.children_nlookup == 0) {
    const AssetProto* proto = inode.asset.proto();
    ForgetChild(inode.asset.parent_ino());
    size_t count = 0;
    if (!proto) {
      count = ctx->invalid_inodes.erase(ino);
      LOG_DEBUG("Erased invalid inode");
    } else {
      count = ctx->inodes.erase(proto);
      LOG_DEBUG("Erased inode");
    }
    assert(count);
    (void)count;
  }
}

// Returns inos of previously accessed children inodes for |asset|.
std::vector<fuse_ino_t> CollectLoadedChildInos(const Asset& asset)
    ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  std::vector<const AssetProto*> protos = asset.GetLoadedChildProtos();
  std::vector<fuse_ino_t> children;
  absl::ReaderMutexLock inode_lock(&ctx->inodes_mutex);
  for (const AssetProto* proto : protos) {
    InodeMap::iterator it = ctx->inodes.find(proto);
    if (it != ctx->inodes.end()) {
      children.push_back(GetIno(*it->second.get()));
    }
  }
  return children;
}

// Returns true if |inode| with |ino| is valid (it was not changed by any
// manifest update).
bool ValidateInode(fuse_req_t req, Inode& inode, fuse_ino_t ino) {
  if (!inode.IsValid()) {
    LOG_WARNING("Ino %u was outdated after the manifest update", ino);
    fuse_reply_err(req, ENOENT);
    return false;
  }
  return true;
}

// Returns the full relative file path for the given |inode|.
std::string GetRelativePath(const Inode& inode) {
  if (inode.asset.parent_ino() == FUSE_ROOT_ID)
    return inode.asset.proto()->name();
  std::string rel_path = GetRelativePath(GetInode(inode.asset.parent_ino()));
  absl::StrAppend(&rel_path, "/", inode.asset.proto()->name());
  return rel_path;
}

// Asks the server to prioritize the asset at |rel_file_path|.
void PrioritizeAssetOnServer(const std::string& rel_file_path) {
  std::vector<std::string> assets{rel_file_path};
  LOG_INFO("Requesing server to prioritize asset '%s'", rel_file_path);
  absl::Status status =
      ctx->config_stream_client->ProcessAssets(std::move(assets));
  // An error is not critical, but we should log it.
  if (!status.ok()) {
    LOG_ERROR(
        "Failed to request prioritization for asset '%s' from the server: %s",
        rel_file_path, status.ToString());
  }
}

// Queues a CdcFuseOpen request in the list of pending requests. Thread-safe.
void QueueOpenRequest(const std::string& rel_path, fuse_req_t req,
                      fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->queued_requests_mutex) {
  QueuedRequest qr{QueuedRequest::Type::kOpen, rel_path};
  qr.u.open.req = req;
  qr.u.open.ino = ino;
  qr.u.open.fi = *fi;
  {
    absl::MutexLock lock(&ctx->queued_requests_mutex);
    ctx->queued_requests.emplace_back(std::move(qr));
  }
  PrioritizeAssetOnServer(rel_path);
}

// Queues a CdcFuseOpenDir request in the list of pending requests. Thread-safe.
void QueueOpenDirRequest(const std::string& rel_path, fuse_req_t req,
                         fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->queued_requests_mutex) {
  QueuedRequest qr{QueuedRequest::Type::kOpenDir, rel_path};
  qr.u.open.req = req;
  qr.u.open.ino = ino;
  qr.u.open.fi = *fi;
  {
    absl::MutexLock lock(&ctx->queued_requests_mutex);
    ctx->queued_requests.emplace_back(std::move(qr));
  }
  PrioritizeAssetOnServer(rel_path);
}

// Queues a CdcFuseLookup reuquest in the list of pending requests. Thread-safe.
void QueueLookupRequest(const std::string& rel_path, fuse_req_t req,
                        fuse_ino_t parent_ino, const char* name)
    ABSL_LOCKS_EXCLUDED(ctx->queued_requests_mutex) {
  QueuedRequest qr{QueuedRequest::Type::kLookup, rel_path};
  qr.u.lookup.req = req;
  qr.u.lookup.parent_ino = parent_ino;
  qr.u.lookup.name = name;
  {
    absl::MutexLock lock(&ctx->queued_requests_mutex);
    ctx->queued_requests.emplace_back(std::move(qr));
  }
  PrioritizeAssetOnServer(rel_path);
}

}  // namespace

// Implementation of the Fuse lookup() method.
// See include/fuse_lowlevel.h.
void CdcFuseLookup(fuse_req_t req, fuse_ino_t parent_ino, const char* name)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex, ctx->inodes_mutex) {
  LOG_DEBUG("CdcFuseLookup, parent_ino=%u, name='%s'", parent_ino, name);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& parent = GetInode(parent_ino);
  if (!ValidateInode(req, parent, parent_ino)) {
    return;
  }

  if (parent.asset.proto()->in_progress()) {
    // This directory has not been processed yet. Queue up the request and block
    // until an updated manifest is available.
    std::string rel_path = GetRelativePath(parent);
    LOG_INFO("Request to open ino %u queued (file '%s' not ready)", parent_ino,
             rel_path);
    QueueLookupRequest(rel_path, req, parent_ino, name);
    return;
  }

  absl::StatusOr<const AssetProto*> proto = parent.asset.Lookup(name);
  if (!proto.ok()) {
    LOG_ERROR("Lookup of '%s' in ino %u failed: '%s'", name, parent_ino,
              proto.status().ToString().c_str());
    fuse_reply_err(req, ENOENT);
    return;
  }
  if (!*proto) {
    fuse_reply_err(req, ENOENT);
    return;
  }

  Inode* inode;
  {
    absl::MutexLock inode_lock(&ctx->inodes_mutex);
    inode = GetOrCreateInode(parent, *proto);
  }
  if (!ValidateInode(req, *inode, GetIno(*inode))) {
    return;
  }

  fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  e.attr_timeout = internal::kCdcFuseInodeTimeoutSec;
  e.entry_timeout = internal::kCdcFuseInodeTimeoutSec;
  e.ino = GetIno(*inode);
  FillStatBuffer(e.ino, &e.attr);
  fuse_reply_entry(req, &e);
}

// Implementation of the Fuse getattr() method.
// See include/fuse_lowlevel.h.
void CdcFuseGetAttr(fuse_req_t req, fuse_ino_t ino,
                    struct fuse_file_info* /*fi*/)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseGetAttr, ino=%u", ino);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  if (!ValidateInode(req, GetInode(ino), ino)) {
    return;
  }

  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));
  FillStatBuffer(ino, &stbuf);
  fuse_reply_attr(req, &stbuf, internal::kCdcFuseInodeTimeoutSec);
}

void CdcFuseSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                    int to_set, struct fuse_file_info* fi) {
  LOG_DEBUG("CdcFuseSetAttr, ino=%u to_set=%04x mode=%04o", ino, to_set,
            attr->st_mode);
  // TODO: Verify that the bits are already set or store the new permissions in
  // a separate variable.
  CdcFuseGetAttr(req, ino, fi);
}

// Implementation of the FUSE open() method.
// See include/fuse_lowlevel.h.
void CdcFuseOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseOpen, ino=%u, flags=%u", ino, fi->flags);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);
  if (!ValidateInode(req, inode, ino)) {
    return;
  }

  const AssetProto* proto = inode.asset.proto();
  if (proto->type() == AssetProto::DIRECTORY) {
    fuse_reply_err(req, EISDIR);
    return;
  }

  // TODO: Handle links.
  if (proto->type() != AssetProto::FILE) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  if ((fi->flags & 3) != O_RDONLY) {
    fuse_reply_err(req, EACCES);
    return;
  }

  if (proto->file_size() > 0 && proto->in_progress()) {
    // This file has not been processed yet. Queue up the request Block until an
    // updated manifest is available.
    LOG_DEBUG("Request to open ino %u queued (file not ready)", ino);
    QueueOpenRequest(GetRelativePath(inode), req, ino, fi);
    return;
  }

  if (fi->flags & O_DIRECT) {
    fi->keep_cache = 0;
    fi->direct_io = 1;
  } else {
    fi->keep_cache = 1;
    fi->direct_io = 0;
  }

  // If the manifest was changed, open files "from scratch" to be able to get
  // the updated data.
  if (inode.IsUpdated()) {
    fi->keep_cache = 0;
    inode.state = InodeState::kInitialized;
  }
  fuse_reply_open(req, fi);
}

// Implementation of the FUSE read() method.
// See include/fuse_lowlevel.h.
void CdcFuseRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info* /*fi*/)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseRead, ino=%u, size=%u, off=%u", ino, size, off);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);
  if (!ValidateInode(req, inode, ino)) {
    return;
  }
  if (inode.IsUpdated()) {
    LOG_ERROR("Manifest has been updated, the file '%s' should be reopened",
              inode.asset.proto()->name());
    fuse_reply_err(req, EIO);
    return;
  }
  ctx->buffer.resize(size);
  absl::StatusOr<uint64_t> bytes_read =
      inode.asset.Read(off, ctx->buffer.data(), size);
  if (!bytes_read.ok()) {
    LOG_ERROR("Reading %u bytes from offset %u of asset '%s' failed: '%s'",
              size, off, inode.asset.proto()->name().c_str(),
              bytes_read.status().ToString().c_str());
    fuse_reply_err(req, EIO);
    return;
  }
  fuse_reply_buf(req, ctx->buffer.data(), *bytes_read);
}

// Implementation of the FUSE release() method.
// See include/fuse_lowlevel.h.
void CdcFuseRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseRelease, ino=%u", ino);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);
  if (!ValidateInode(req, inode, ino)) {
    return;
  }

  const AssetProto* proto = inode.asset.proto();
  if (proto->type() == AssetProto::DIRECTORY) {
    fuse_reply_err(req, EISDIR);
    return;
  }

  if (proto->type() != AssetProto::FILE) {
    fuse_reply_err(req, EINVAL);
    return;
  }
  fuse_reply_err(req, 0);
}

// Implementation of the FUSE opendir() method.
// See include/fuse_lowlevel.h.
void CdcFuseOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseOpenDir, ino=%u", ino);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);

  if (!ValidateInode(req, inode, ino)) {
    return;
  }

  const AssetProto* proto = inode.asset.proto();
  if (proto->type() != AssetProto::DIRECTORY) {
    fuse_reply_err(req, ENOTDIR);
    return;
  }
  if (proto->in_progress()) {
    // This directory has not been processed yet. Queue up the request until an
    // updated manifest is available.
    LOG_DEBUG("Request to open directory '%s' ino %u queued (dir not ready)",
              proto->name(), ino);
    QueueOpenDirRequest(GetRelativePath(inode), req, ino, fi);
    return;
  }

  fuse_reply_open(req, fi);
}

// Implementation of the FUSE readdir() method.
// See include/fuse_lowlevel.h.
void CdcFuseReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                    fuse_file_info* /*fi*/)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex, ctx->inodes_mutex) {
  LOG_DEBUG("CdcFuseReadDir, ino=%u, size=%u, off=%u", ino, size, off);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);
  if (!ValidateInode(req, inode, ino) ||
      !ValidateInode(req, GetInode(inode.asset.parent_ino()),
                     inode.asset.parent_ino())) {
    return;
  }

  if (inode.asset.proto()->type() != AssetProto::DIRECTORY) {
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  // TODO: This is called at least twice for each ls call. Cache buffer or
  // similar.
  Buffer buffer;
  AddDirectoryEntry(req, &buffer, ".", ino);
  AddDirectoryEntry(req, &buffer, "..", inode.asset.parent_ino());

  {
    absl::StatusOr<std::vector<const AssetProto*>> protos =
        inode.asset.GetAllChildProtos();
    if (!protos.ok()) {
      LOG_ERROR("ReadDir of ino %u failed: '%s'", ino,
                protos.status().ToString().c_str());
      fuse_reply_err(req, EBADF);
      return;
    }
    absl::MutexLock inode_lock(&ctx->inodes_mutex);
    for (const AssetProto* child_proto : *protos) {
      const Inode& child_inode = *GetOrCreateInode(inode, child_proto);
      if (!child_inode.IsValid()) continue;
      AddDirectoryEntry(req, &buffer, child_proto->name().c_str(),
                        GetIno(child_inode));
    }
  }

  if (off >= static_cast<off_t>(buffer.size())) {
    // Out of bounds read.
    fuse_reply_buf(req, nullptr, 0);
  } else {
    // Return the part that the caller asks for.
    fuse_reply_buf(req, buffer.data() + off,
                   std::min(buffer.size() - off, size));
  }
}

void CdcFuseReleaseDir(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseReleaseDir, ino=%u", ino);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  Inode& inode = GetInode(ino);
  if (!ValidateInode(req, inode, ino)) {
    return;
  }

  if (inode.asset.proto()->type() != AssetProto::DIRECTORY) {
    fuse_reply_err(req, ENOTDIR);
    return;
  }
  fuse_reply_err(req, 0);
}

// Implementation of the FUSE forget() method.
// See include/fuse_lowlevel.h.
void CdcFuseForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex, ctx->inodes_mutex) {
  LOG_DEBUG("CdcFuseForget, ino=%u, nlookup=%u", ino, nlookup);
  assert(ctx && ctx->initialized);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  absl::MutexLock ctx_lock(&ctx->inodes_mutex);
  ForgetOne(ino, nlookup);
  fuse_reply_none(req);
}

// Implementation of the FUSE forget_multi() method.
// See include/fuse_lowlevel.h.
void CdcFuseForgetMulti(fuse_req_t req, size_t count,
                        struct fuse_forget_data* forgets)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex, ctx->inodes_mutex) {
  LOG_DEBUG("CdcFuseForgetMulti, count=%u", count);
  assert(forgets);
  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  absl::MutexLock ctx_lock(&ctx->inodes_mutex);
  for (size_t i = 0; i < count; ++i) {
    ForgetOne(forgets[i].ino, forgets[i].nlookup);
  }
  fuse_reply_none(req);
}

// Implementation of the FUSE access() method.
// See include/fuse_lowlevel.h.
void CdcFuseAccess(fuse_req_t req, fuse_ino_t ino, int mask)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_DEBUG("CdcFuseAccess, ino=%u, mask=%u", ino, mask);

  absl::ReaderMutexLock manifest_lock(&ctx->manifest_mutex);
  struct fuse_context* context = fuse_get_context();
  // Root always has access rights.
  if (context->uid == internal::kCdcFuseRootUid ||
      context->gid == internal::kCdcFuseRootGid) {
    fuse_reply_err(req, 0);
    return;
  }
  if (!ValidateInode(req, GetInode(ino), ino)) {
    return;
  }

  struct stat stbuf;
  memset(&stbuf, 0, sizeof(stbuf));
  FillStatBuffer(ino, &stbuf);

  int process_permission = stbuf.st_mode & 0x7;  // world
  if (stbuf.st_gid == static_cast<uint32_t>(context->gid)) {
    process_permission |= stbuf.st_mode >> 3 & 0x7;  // group
  }
  if (stbuf.st_uid == static_cast<uint32_t>(context->uid)) {
    process_permission |= stbuf.st_mode >> 6 & 0x7;  // user
  }

  if ((process_permission & mask) != mask) {
    fuse_reply_err(req, EACCES);
    return;
  }
  fuse_reply_err(req, 0);
}

// Not-implemented functions for read-only FUSE.
void CdcFuseReadLink(fuse_req_t req, fuse_ino_t ino) {
  LOG_WARNING("CdcFuseReadLink not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseFlush not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseFSync(fuse_req_t req, fuse_ino_t ino, int datasync,
                  struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseFSync not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseFSyncDir(fuse_req_t req, fuse_ino_t ino, int datasync,
                     struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseFSyncDir not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseStatFS(fuse_req_t req, fuse_ino_t ino)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseStatFS not implemented, ino=%u", ino);
  // Mimic the default behavior of the FUSE library.
  struct statvfs buf;
  buf.f_bsize = 512;
  buf.f_namemax = 255;
  fuse_reply_statfs(req, &buf);
}

void CdcFuseSetXAttr(fuse_req_t req, fuse_ino_t ino, const char* name,
                     const char* value, size_t size, int flags)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseSetXAttr not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseGetXAttr(fuse_req_t req, fuse_ino_t ino, const char* name,
                     size_t size) ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseGetXAttr not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseListXAttr(fuse_req_t req, fuse_ino_t ino, size_t size)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseListXAttr not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseGetLk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                  struct flock* lock) ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseGetLk not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseSetLk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                  struct flock* lock, int sleep)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseSetLk not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseBMap(fuse_req_t req, fuse_ino_t ino, size_t blocksize, uint64_t idx)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseBMap not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseIoctl(fuse_req_t req, fuse_ino_t ino, int cmd, void* arg,
                  struct fuse_file_info* fi, unsigned flags, const void* in_buf,
                  size_t in_bufsz, size_t out_bufsz)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseIoctl not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFusePoll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                 struct fuse_pollhandle* ph)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFusePoll not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseRetrieveReply(fuse_req_t req, void* cookie, fuse_ino_t ino,
                          off_t offset, struct fuse_bufvec* bufv)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseRetrieveReply not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseFLock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi,
                  int op) ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseFLock not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

void CdcFuseFAllocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
                      off_t length, struct fuse_file_info* fi)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex) {
  LOG_WARNING("CdcFuseFAllocate not implemented, ino=%u", ino);
  fuse_reply_err(req, ENOSYS);
}

size_t CdcFuseGetInodeCountForTesting() ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  assert(ctx);
  absl::MutexLock lock(&ctx->inodes_mutex);
  return ctx->inodes.size();
}

size_t CdcFuseGetInvalidInodeCountForTesting()
    ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  assert(ctx);
  absl::MutexLock lock(&ctx->inodes_mutex);
  return ctx->invalid_inodes.size();
}

namespace cdc_fuse_fs {

absl::Status Initialize(int argc, char** argv) {
  assert(!ctx);
  ctx = new CdcFuseFsContext();

#ifndef USE_MOCK_LIBFUSE
  // Parse command line args.
  ctx->args = FUSE_ARGS_INIT(argc, argv);
  if (fuse_parse_cmdline(&ctx->args, &ctx->mountpoint, &ctx->multithreaded,
                         /*foreground=*/nullptr) == -1) {
    Shutdown();
    return MakeStatus("fuse_parse_cmdline() failed");
  }

  // Initialize channel.
  ctx->channel = fuse_mount(ctx->mountpoint, &ctx->args);
  if (!ctx->channel) {
    Shutdown();
    return MakeStatus("fuse_mount() failed");
  }

  // Initialize session.
  fuse_lowlevel_ops fs_operations = {.lookup = CdcFuseLookup,
                                     .forget = CdcFuseForget,
                                     .getattr = CdcFuseGetAttr,
                                     .setattr = CdcFuseSetAttr,
                                     .readlink = CdcFuseReadLink,
                                     // .mknod // Read-only file system
                                     // .mkdir // Read-only file system
                                     // .unlink // Read-only file system
                                     // .rmdir // Read-only file system
                                     // .symlink // Read-only file system
                                     // .rename // Read-only file system
                                     // .link // Read-only file system
                                     .open = CdcFuseOpen,
                                     .read = CdcFuseRead,
                                     // .write // Read-only file system
                                     .flush = CdcFuseFlush,
                                     .release = CdcFuseRelease,
                                     .fsync = CdcFuseFSync,
                                     .opendir = CdcFuseOpenDir,
                                     .readdir = CdcFuseReadDir,
                                     .releasedir = CdcFuseReleaseDir,
                                     .fsyncdir = CdcFuseFSyncDir,
                                     .statfs = CdcFuseStatFS,
                                     .setxattr = CdcFuseSetXAttr,
                                     .getxattr = CdcFuseGetXAttr,
                                     .listxattr = CdcFuseListXAttr,
                                     // .removexattr // Read-only file system
                                     .access = CdcFuseAccess,
                                     // .create // Read-only file system
                                     .getlk = CdcFuseGetLk,
                                     .setlk = CdcFuseSetLk,
                                     .bmap = CdcFuseBMap,
                                     .ioctl = CdcFuseIoctl,
                                     .poll = CdcFusePoll,
                                     // .write_buf // Read-only file system
                                     .retrieve_reply = CdcFuseRetrieveReply,
                                     .forget_multi = CdcFuseForgetMulti,
                                     .flock = CdcFuseFLock,
                                     .fallocate = CdcFuseFAllocate};
  ctx->session = fuse_lowlevel_new(&ctx->args, &fs_operations,
                                   sizeof(fs_operations), nullptr);
  if (!ctx->session) {
    Shutdown();
    return MakeStatus("fuse_lowlevel_new() failed");
  }

  // Set signal handlers.
  if (fuse_set_signal_handlers(ctx->session) == -1) {
    Shutdown();
    return MakeStatus("fuse_set_signal_handlers() failed");
  }
  ctx->signal_handlers_set = true;

  fuse_session_add_chan(ctx->session, ctx->channel);

#else
  // This code is not unit tested.
#endif

  ctx->initialized = true;
  return absl::OkStatus();
}

void Shutdown() {
  assert(ctx);

#ifndef USE_MOCK_LIBFUSE
  // Exact opposite of Create().
  if (ctx->signal_handlers_set) {
    ctx->signal_handlers_set = false;
    fuse_session_remove_chan(ctx->channel);
    fuse_remove_signal_handlers(ctx->session);
  }

  if (ctx->session) {
    fuse_session_destroy(ctx->session);
    ctx->session = nullptr;
  }

  if (ctx->channel) {
    fuse_unmount(ctx->mountpoint, ctx->channel);
    ctx->channel = nullptr;
  }

  if (ctx->mountpoint) {
    free(ctx->mountpoint);
    ctx->mountpoint = nullptr;
  }

  fuse_opt_free_args(&ctx->args);
#else
  // This code is not unit tested.
#endif

  ctx->initialized = false;
  delete ctx;
  ctx = nullptr;
}

// Adds a warning message to |warnings| if |inode| does not point to
// |context_proto|.
void CheckProtoMismatch(const std::shared_ptr<Inode>& inode,
                        const AssetProto* context_proto,
                        Json::Value& warnings) {
  if (context_proto != inode->asset.proto()) {
    LOG_WARNING("Proto mismatch %u", GetIno(*inode.get()));
    Json::Value value;
    value["ino"] = GetIno(*inode.get());
    value["state"] = InodeStateToString(inode->state);
    value["context_proto"] = context_proto;
    value["actual_proto"] = inode->asset.proto();
    warnings.append(value);
  }
}

// Adds a warning message to |warnings| if the proto of |inode| is not nullptr.
// This check is relevant for invalidated inodes (corresponding files and
// directories were removed from the manifest).
void CheckProtoNotNull(const std::shared_ptr<Inode>& inode,
                       Json::Value& warnings) {
  if (inode->asset.proto()) {
    LOG_WARNING("Proto for invalidated inode is not NULL %u",
                GetIno(*inode.get()));
    Json::Value value;
    value["ino"] = GetIno(*inode.get());
    warnings.append(value);
  }
}

Json::Value CreateWarningMessage(const Inode* inode, std::string&& message) {
  Json::Value warning;
  warning["ino"] = GetIno(*inode);
  warning["name"] = inode->asset.proto()->name();
  warning["message"] = message;
  return warning;
}

// Adds a set of warning messages to |warnings| if inodes have wrong
// properties, for example: a non-directory asset has directory assets.
void CheckConsistencyIndividualInodes(const std::vector<const Inode*>& inodes,
                                      Json::Value& warnings) {
  LOG_DEBUG("Checking consistency of individual inodes");
  Json::Value inodes_wrong_properties;
  for (const Inode* inode : inodes) {
    std::string asset_check;
    if (!inode->asset.IsConsistent(&asset_check)) {
      inodes_wrong_properties.append(
          CreateWarningMessage(inode, std::move(asset_check)));
    }

    // Inode should be referenced.
    if (inode->nlookup + inode->children_nlookup == 0) {
      inodes_wrong_properties.append(
          CreateWarningMessage(inode, "Inode is not referenced"));
    }

    if (!inodes_wrong_properties.empty()) {
      warnings["inodes_wrong_properties"] = inodes_wrong_properties;
    }
  }
}

// Adds a set of warning messages to |warnings| if inodes have invalid parents
// and thus cannot be reached from the updated manifest. It checks the
// consistency of tree directory structure.
void CheckConsistencyInodesHierarchy(const std::vector<const Inode*>& inodes,
                                     Json::Value& warnings)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->manifest_mutex) {
  LOG_DEBUG("Checking consistency of inodes hierarchy");
  std::deque<const Inode*> inodes_queue;
  inodes_queue.insert(inodes_queue.end(), inodes.begin(), inodes.end());
  Json::Value inodes_wrong_parent;
  std::unordered_set<const Inode*> visited;
  while (!inodes_queue.empty()) {
    const Inode* inode = inodes_queue.front();
    inodes_queue.pop_front();
    if (visited.find(inode) != visited.end()) {
      continue;
    }
    visited.insert(inode);
    Inode& parent = GetInode(inode->asset.parent_ino());
    // Only valid inodes can be on the list.
    if (!parent.IsValid()) {
      Json::Value message;
      message["ino"] = GetIno(*inode);
      message["parent"] = inode->asset.parent_ino();
      message["name"] = inode->asset.proto()->name();
      message["message"] = "Invalid parent";
      inodes_wrong_parent.append(message);
      continue;
    }
    // Add the parent to the deque, as |inodes| includes only kUpdatedProto and
    // kUpdated.
    if (visited.find(&parent) == visited.end()) {
      inodes_queue.push_back(&parent);
    }
  }
  if (!visited.empty() && visited.find(ctx->root.get()) == visited.end()) {
    Json::Value message;
    message["message"] =
        "Inode hierarchy is not consistent: the root node was not reached";
    inodes_wrong_parent.append(message);
  }
  if (!inodes_wrong_parent.empty()) {
    warnings["inodes_wrong_parent"] = inodes_wrong_parent;
  }
}

// Checks if the proto messages are reachable from ctx->manifest.
// Returns the set of inodes with unreachable protos.
std::set<const Inode*> CheckProtoReachability(Json::Value& warnings)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->manifest_mutex)
        ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  LOG_DEBUG("Checking proto reachability");
  Json::Value reachability_warning;
  std::set<const Inode*> unreachable_inodes;
  if (&ctx->manifest->root_dir() != ctx->root->asset.proto()) {
    Json::Value message;
    message["message"] = "Root inode does not point to the manifest proto";
    reachability_warning.append(message);
    unreachable_inodes.emplace(ctx->root.get());
  }

  absl::MutexLock lock(&ctx->inodes_mutex);
  std::vector<const AssetProto*> root_protos =
      ctx->root->asset.GetLoadedChildProtos();
  std::unordered_set<const AssetProto*> manifest_protos(root_protos.begin(),
                                                        root_protos.end());

  // Start with the root node and its children, add children protos on the
  // way.
  std::deque<const AssetProto*> collected_protos;
  collected_protos.insert(collected_protos.end(), manifest_protos.begin(),
                          manifest_protos.end());
  // Collect all protos reachable from the manifest.
  while (!collected_protos.empty()) {
    const AssetProto* proto = collected_protos.front();
    collected_protos.pop_front();
    InodeMap::iterator it = ctx->inodes.find(proto);
    // Collect child protos of all directories.
    if (it == ctx->inodes.end() ||
        it->second->asset.proto()->type() != AssetProto::DIRECTORY) {
      continue;
    }
    std::vector<const AssetProto*> subprotos =
        it->second->asset.GetLoadedChildProtos();
    collected_protos.insert(collected_protos.end(), subprotos.begin(),
                            subprotos.end());
    manifest_protos.insert(subprotos.begin(), subprotos.end());
  }

  for (const auto& [proto, inode] : ctx->inodes) {
    if (manifest_protos.find(proto) == manifest_protos.end()) {
      Json::Value message;
      message["message"] = absl::StrFormat(
          "Proto for inode %i is not reachable from the manifest",
          reinterpret_cast<fuse_ino_t>(&(*inode)));
      reachability_warning.append(message);
      unreachable_inodes.emplace(inode.get());
    }
  }
  if (!reachability_warning.empty()) {
    warnings["proto_reachability"] = reachability_warning;
  }
  return unreachable_inodes;
}

// Checks if the FUSE state is consistent after the manifest update. In case of
// any inconsistencies it prints out a pretty JSON string. |inodes_size|
// describes the number of inodes before the manifest was set.
void CheckFUSEConsistency(size_t inodes_size)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(ctx->manifest_mutex)
        ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  LOG_DEBUG("Starting FUSE consistency check");

  std::vector<const Inode*> inodes_to_check;
  Json::Value warnings;
  // Step I. Root consistency.
  LOG_DEBUG("Checking the root");
  if (!ctx->root || ctx->root->asset.parent_ino() != FUSE_ROOT_ID ||
      !ctx->root->IsValid() || ctx->root->IsInitialized()) {
    Json::Value warning_root = ctx->root->ToJson(true);
    warning_root["message"] = "The root inode is inconsistent";
    warnings.append(warning_root);
  }

  // Step II. The total amount of inodes should not change.
  Json::Value initialized_json;
  Json::Value wrong_protos_json;
  std::vector<const Inode*> invalid_inodes;
  size_t initialized_total = 0;
  size_t updated_proto_total = 0;
  size_t updated_total = 0;
  {
    LOG_DEBUG("Checking the number of inodes");
    absl::ReaderMutexLock lock(&ctx->inodes_mutex);
    if (inodes_size != ctx->inodes.size() + ctx->invalid_inodes.size()) {
      Json::Value warning_size;
      warning_size["message"] =
          absl::StrFormat("Inodes' size mismatch: expected: %u, actual: %u",
                          inodes_size, ctx->inodes.size());
      warnings.append(warning_size);
    }

    // Step III. Consistency of ctx->inodes: inodes should point to the
    // correct asset proto and asset protos should point to the right inodes.
    LOG_DEBUG("Checking inode state");
    for (const auto& [context_proto, inode] : ctx->inodes) {
      switch (inode->state) {
        case InodeState::kInitialized:
          // There must be no kInitialized inodes, all should be kUpdatedProto,
          // kUpdated, or kInvalid after manifest update.
          initialized_json.append(inode->ToJson(true));
          ++initialized_total;
          break;
        case InodeState::kUpdatedProto:
          CheckProtoMismatch(inode, context_proto, wrong_protos_json);
          inodes_to_check.push_back(inode.get());
          ++updated_proto_total;
          break;
        case InodeState::kUpdated:
          CheckProtoMismatch(inode, context_proto, wrong_protos_json);
          inodes_to_check.push_back(inode.get());
          ++updated_total;
          break;
        case InodeState::kInvalid:
          CheckProtoNotNull(inode, wrong_protos_json);
          invalid_inodes.push_back(inode.get());
          break;
      }
    }
  }
  LOG_DEBUG("Initialized=%u, updated_proto=%u, updated=%u, invalid=%u",
            initialized_total, updated_proto_total, updated_total,
            invalid_inodes.size());

  if (!initialized_json.empty()) {
    warnings["initialized_inodes"] = initialized_json;
  }

  if (!wrong_protos_json.empty()) {
    warnings["wrong_protos_inodes"] = wrong_protos_json;
  }

  // IV. Tree consistency.
  CheckConsistencyInodesHierarchy(inodes_to_check, warnings);

  // V. Check reachability of all AssetProtos.
  std::set<const Inode*> unreachable_inodes = CheckProtoReachability(warnings);
  inodes_to_check.push_back(ctx->root.get());
  if (!unreachable_inodes.empty()) {
    LOG_WARNING("Skipping %i inodes from the consistency check",
                unreachable_inodes.size());
    inodes_to_check.erase(
        std::remove_if(inodes_to_check.begin(), inodes_to_check.end(),
                       [&unreachable_inodes](const Inode* inode) {
                         return unreachable_inodes.find(inode) !=
                                unreachable_inodes.end();
                       }),
        inodes_to_check.end());
  }

  // VI. Consistency of individual reachable inodes.
  CheckConsistencyIndividualInodes(inodes_to_check, warnings);

  Json::Value output;
  if (!warnings.empty()) {
    Json::Value updated_proto_json;
    Json::Value updated_json;
    for (const Inode* inode : inodes_to_check) {
      if (inode->IsUpdated()) {
        updated_json.append(inode->ToJson(true));
      } else {
        assert(inode->IsUpdatedProto());
        updated_proto_json.append(inode->ToJson(true));
      }
    }
    Json::Value invalid_json;
    for (const Inode* inode : invalid_inodes) {
      invalid_json.append(inode->ToJson(false));
    }
    output["updated_proto_inodes"] = updated_proto_json;
    output["updated_inodes"] = updated_json;
    output["invalid_inodes"] = invalid_json;
    output["warnings"] = warnings;
  }

  if (output.empty()) {
    LOG_INFO("FUSE consistency check succeeded");
  } else {
    LOG_WARNING("FUSE consistency check: %s", output.toStyledString());
  }
}

// Recursive procedure to invalidate the inode subtree for |ino| including
// the root |ino| of the subtree. The elements cannot be directly removed as
// they might be still referenced.
void InvalidateTree(fuse_ino_t ino)
    ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex)
        ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  std::deque<fuse_ino_t> inos;
  inos.push_back(ino);
  while (!inos.empty()) {
    fuse_ino_t tmp_ino = inos.front();
    Inode& inode = GetInode(tmp_ino);
    if (!inode.IsValid()) {
      LOG_WARNING(
          "ino should be valid before invalidation. ino %u is already invalid",
          ino);
      return;
    }
    inode.state = InodeState::kInvalid;
    if (inode.asset.proto()->type() == AssetProto::DIRECTORY) {
      std::vector<fuse_ino_t> child_inos = CollectLoadedChildInos(inode.asset);
      inos.insert(inos.end(), child_inos.begin(), child_inos.end());
    }
    {
      absl::MutexLock inode_lock(&ctx->inodes_mutex);
      const AssetProto* outdated_proto = inode.asset.proto();
      ctx->invalid_inodes[tmp_ino] = ctx->inodes[outdated_proto];
      size_t count = ctx->inodes.erase(outdated_proto);
      assert(count);
      (void)count;
    }
    inode.asset.UpdateProto(nullptr);
    inos.pop_front();
  }
}

struct UpdateInode {
  std::shared_ptr<Inode> new_parent;
  fuse_ino_t old_ino;
};

// ThreadPool task that runs the update of inodes.
class UpdateInodeTask : public Task {
 public:
  UpdateInodeTask(UpdateInode* inode, std::vector<UpdateInode>* result)
      : update_inode_(inode), child_inodes_to_update_(result) {}

  // Task:
  void ThreadRun(IsCancelledPredicate is_cancelled) override
      ABSL_SHARED_LOCKS_REQUIRED(ctx->manifest_mutex)
          ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
    LOG_DEBUG("Updating inode %u", update_inode_->old_ino);
    assert((ctx->manifest_mutex.AssertHeld(), true));

    const std::shared_ptr<Inode>& new_parent = update_inode_->new_parent;
    Inode& old_inode = GetInode(update_inode_->old_ino);
    assert(old_inode.IsValid());

    const std::string& name = old_inode.asset.proto()->name();
    absl::StatusOr<const AssetProto*> new_proto =
        new_parent->asset.Lookup(name.c_str());

    // The asset does not exist anymore. It has to be removed from the parent's
    // set of children. If the node has its own children, they should be
    // invalidated as well. The final removal from the inode map can only be
    // done via forget() and forget_multi() calls.
    if (!new_proto.ok() || !*new_proto) {
      InvalidateTree(update_inode_->old_ino);
      return;
    }
    // Asset still exists in a new proto. Its inode id should be preserved. If a
    // new proto exists for the same name, but the asset has changed, an update
    // is necessary, the inode id remains stable.
    if (*(*new_proto) != *(old_inode.asset.proto())) {
      LOG_DEBUG("Inode %u is marked for update", update_inode_->old_ino);
      old_inode.state = InodeState::kUpdated;
    } else {
      old_inode.state = InodeState::kUpdatedProto;
    }
    const AssetProto* old_proto = old_inode.asset.proto();
    std::shared_ptr<Inode> new_inode;
    {
      absl::MutexLock inode_lock(&ctx->inodes_mutex);
      new_inode = ctx->inodes[*new_proto] = ctx->inodes[old_proto];
    }
    // As there is an updated valid entry for the same inode in the map,
    // the old one can be removed.
    proto_to_remove_ = old_proto;

    std::vector<fuse_ino_t> child_inos =
        CollectLoadedChildInos(old_inode.asset);
    for (fuse_ino_t child_ino : child_inos) {
      UpdateInode child_to_update;
      child_to_update.new_parent = new_inode;
      child_to_update.old_ino = child_ino;
      child_inodes_to_update_->emplace_back(std::move(child_to_update));
    }
    old_inode.asset.UpdateProto(*new_proto);
  }

  const AssetProto* ProtoToRemove() const { return proto_to_remove_; }

 private:
  const UpdateInode* const update_inode_;
  std::vector<UpdateInode>* child_inodes_to_update_;
  const AssetProto* proto_to_remove_ = nullptr;
};

// Recursive procedure to update the inodes contents on a level after a request
// to update the manifest id was received.
void ParallelUpdateProtosOnLevel(
    Threadpool& pool, std::vector<UpdateInode>& input_inodes,
    std::vector<std::vector<UpdateInode>>& result,
    std::vector<const AssetProto*>& outdated_protos) {
  LOG_DEBUG("Update asset protos in parallel on the same level");
  assert(input_inodes.size() == result.size());

  for (unsigned int idx = 0; idx < input_inodes.size(); ++idx) {
    pool.QueueTask(
        std::make_unique<UpdateInodeTask>(&input_inodes[idx], &result[idx]));
  }
  for (unsigned int idx = 0; idx < input_inodes.size(); ++idx) {
    std::unique_ptr<Task> task = pool.GetCompletedTask();
    UpdateInodeTask* update_task = static_cast<UpdateInodeTask*>(task.get());
    if (update_task->ProtoToRemove()) {
      outdated_protos.push_back(update_task->ProtoToRemove());
    }
  }
}

std::shared_ptr<Inode> UpdateProtosFromRoot(const AssetProto* new_root_proto)
    ABSL_LOCKS_EXCLUDED(ctx->inodes_mutex) {
  LOG_DEBUG("Updating inode hierarchy starting from the root");
  assert((ctx->manifest_mutex.AssertHeld(), true));

  // Create the new root. Make sure to preserve the lookup counts!
  std::shared_ptr<Inode> new_root = std::make_shared<Inode>();
  new_root->asset.Initialize(FUSE_ROOT_ID, ctx->data_store_reader,
                             new_root_proto);
  new_root->nlookup = ctx->root->nlookup.load();
  new_root->children_nlookup = ctx->root->children_nlookup.load();
  new_root->state = ctx->root->state.load();
  new_root->is_root = true;

  std::shared_ptr<Inode> old_root = ctx->root;
  std::vector<fuse_ino_t> children = CollectLoadedChildInos(old_root->asset);
  std::vector<UpdateInode> inos_to_update;
  inos_to_update.reserve(children.size());
  for (fuse_ino_t child : children) {
    UpdateInode to_update;
    to_update.new_parent = new_root;
    to_update.old_ino = child;
    inos_to_update.emplace_back(std::move(to_update));
  }

  // Outdated AssetProto(s) can be removed at the end, as they have a duplicated
  // updated entry in inodes. Only updated (not removed) inodes are included.
  std::vector<const AssetProto*> outdated_protos;
  Threadpool pool(std::thread::hardware_concurrency());
  while (!inos_to_update.empty()) {
    std::vector<std::vector<UpdateInode>> level_result(
        inos_to_update.size(), std::vector<UpdateInode>());
    ParallelUpdateProtosOnLevel(pool, inos_to_update, level_result,
                                outdated_protos);
    inos_to_update.clear();
    for (unsigned int idx = 0; idx < level_result.size(); ++idx) {
      for (unsigned int jdx = 0; jdx < level_result[idx].size(); ++jdx) {
        inos_to_update.push_back(level_result[idx][jdx]);
      }
    }
  }

  // Inodes should not be removed, just the map entries with old protos.
  absl::MutexLock inode_lock(&ctx->inodes_mutex);
  for (size_t idx = outdated_protos.size(); idx > 0; --idx) {
    assert(outdated_protos[idx - 1]);
    size_t count = ctx->inodes.erase(outdated_protos[idx - 1]);
    assert(count);
    (void)count;
  }

  return new_root;
}

absl::Status SetManifest(const ContentIdProto& manifest_id)
    ABSL_LOCKS_EXCLUDED(ctx->manifest_mutex, ctx->inodes_mutex) {
  LOG_DEBUG("Setting manifest '%s' in FUSE",
            ContentId::ToHexString(manifest_id));
  assert(ctx && ctx->initialized && ctx->data_store_reader);

  {
    absl::WriterMutexLock manifest_lock(&ctx->manifest_mutex);
    size_t old_inodes_size;
    {
      absl::MutexLock inodes_lock(&ctx->inodes_mutex);
      old_inodes_size = ctx->inodes.size() + ctx->invalid_inodes.size();
    }
    std::unique_ptr<ManifestProto> new_manifest =
        std::make_unique<ManifestProto>();
    absl::Status status =
        ctx->data_store_reader->GetProto(manifest_id, new_manifest.get());
    if (!status.ok()) {
      LOG_ERROR("Failed to get manifest '%s'",
                ContentId::ToHexString(manifest_id));
      return WrapStatus(status, "Failed to get manifest '%s'",
                        ContentId::ToHexString(manifest_id));
    }
    ctx->root = UpdateProtosFromRoot(&new_manifest->root_dir());
    if (ctx->manifest->root_dir() != new_manifest->root_dir()) {
      ctx->root->state = InodeState::kUpdated;
    } else {
      ctx->root->state = InodeState::kUpdatedProto;
    }
    ctx->manifest.swap(new_manifest);
    if (ctx->consistency_check) {
      CheckFUSEConsistency(old_inodes_size);
    }

    absl::MutexLock inodes_lock(&ctx->inodes_mutex);
    for (const auto& [proto, inode] : ctx->inodes) {
      // Reset kUpdatedProto to kInitialized. The state was only used for
      // validation. kUpdated is still needed for clearing kernel caches when
      // a file is opened.
      assert(inode->IsValid());
      if (inode->IsUpdatedProto() ||
          inode->asset.proto()->type() == AssetProto::DIRECTORY) {
        inode->state = InodeState::kInitialized;
      }
    }
    ctx->root->state = InodeState::kInitialized;
  }

  // Process outstanding open requests. Be sure to move the vector because
  // processing might requeue requests.
  std::vector<QueuedRequest> requests;
  {
    absl::MutexLock lock(&ctx->queued_requests_mutex);
    requests.swap(ctx->queued_requests);
  }
  for (QueuedRequest& qr : requests) {
    switch (qr.type) {
      case QueuedRequest::Type::kLookup:
        LOG_DEBUG("Resuming request to look up '%s' in '%s' (ino %u)",
                  qr.u.lookup.name, qr.rel_path, qr.u.lookup.parent_ino);
        CdcFuseLookup(qr.u.lookup.req, qr.u.lookup.parent_ino,
                      qr.u.lookup.name);
        break;
      case QueuedRequest::Type::kOpen:
        LOG_DEBUG("Resuming request to open file '%s' (ino %u)", qr.rel_path,
                  qr.u.open.ino);
        CdcFuseOpen(qr.u.open.req, qr.u.open.ino, &qr.u.open.fi);
        break;
      case QueuedRequest::Type::kOpenDir:
        LOG_DEBUG("Resuming request to open dir '%s' (ino %u)", qr.rel_path,
                  qr.u.open.ino);
        CdcFuseOpenDir(qr.u.open.req, qr.u.open.ino, &qr.u.open.fi);
        break;
    }
  }

#ifndef USE_MOCK_LIBFUSE
  // Acknowledge that the manifest id was received and FUSE was updated.
  absl::Status status = ctx->config_stream_client->SendManifestAck(manifest_id);
  if (!status.ok()) {
    LOG_ERROR("Failed to send ack for manifest '%s'",
              ContentId::ToHexString(manifest_id));
    return WrapStatus(status, "Failed to send ack for manifest '%s'",
                      ContentId::ToHexString(manifest_id));
  }
#endif

  return absl::OkStatus();
}

void SetConfigClient(
    std::unique_ptr<cdc_ft::ConfigStreamClient> config_client) {
  LOG_DEBUG("Starting configuration client");
  assert(ctx && ctx->initialized);
  if (ctx->config_stream_client) {
    ctx->config_stream_client.reset();
  }
  ctx->config_stream_client = std::move(config_client);
}

// Initializes FUSE with a manifest for an empty directory:
// The user will be able to check the empty folder before the first update
// of the manifest id is received.
void InitializeRootManifest() {
  absl::MutexLock lock(&ctx->manifest_mutex);
  assert(ctx && ctx->root);
  ctx->manifest->mutable_root_dir()->set_type(AssetProto::DIRECTORY);
  ctx->root->asset.Initialize(FUSE_ROOT_ID, ctx->data_store_reader,
                              &ctx->manifest->root_dir());
  ctx->root->is_root = true;
  ctx->root->nlookup = 1;
}

absl::Status Run(DataStoreReader* data_store_reader, bool consistency_check) {
  assert(ctx && ctx->initialized && data_store_reader);
  ctx->consistency_check = consistency_check;
  ctx->data_store_reader = data_store_reader;
  InitializeRootManifest();
#ifndef USE_MOCK_LIBFUSE
  RETURN_IF_ERROR(ctx->config_stream_client->StartListeningToManifestUpdates(
                      [](const ContentIdProto& id) { return SetManifest(id); }),
                  "Failed to listen to manifest updates");

  LOG_INFO("Starting session loop (mt = '%s')",
           ctx->multithreaded ? "true" : "false");
  int res = ctx->multithreaded ? fuse_session_loop_mt(ctx->session)
                               : fuse_session_loop(ctx->session);
  if (res == -1) return MakeStatus("Session loop failed");
  LOG_INFO("Session loop finished.");

  ctx->config_stream_client->Shutdown();
#else
  // This code is not unit tested.
#endif
  return absl::OkStatus();
}

}  // namespace cdc_fuse_fs
}  // namespace cdc_ft
