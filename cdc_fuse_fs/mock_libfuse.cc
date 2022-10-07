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

#include "cdc_fuse_fs/mock_libfuse.h"

#include <cassert>
#include <cstring>

namespace cdc_ft {
namespace {
MockLibFuse* g_fuse;
}

MockLibFuse::MockLibFuse() {
  assert(!g_fuse);
  g_fuse = this;
}

MockLibFuse::~MockLibFuse() {
  assert(g_fuse == this);
  g_fuse = nullptr;
}

void MockLibFuse::SetUid(int uid) {
  assert(g_fuse == this);
  g_fuse->context.uid = uid;
}

void MockLibFuse::SetGid(int gid) {
  assert(g_fuse == this);
  g_fuse->context.gid = gid;
}

size_t fuse_add_direntry(fuse_req_t req, char* buf, size_t bufsize,
                         const char* name, const struct stat* stbuf,
                         off_t off) {
  assert(g_fuse);
  if (bufsize >= sizeof(MockLibFuse::DirEntry)) {
    assert(stbuf);
    auto* entry = reinterpret_cast<MockLibFuse::DirEntry*>(buf);
    strncpy(entry->name, name, sizeof(entry->name));
    entry->name[sizeof(entry->name) - 1] = 0;
    entry->ino = stbuf->st_ino;
    entry->mode = stbuf->st_mode;
    entry->off = off;
  }
  return sizeof(MockLibFuse::DirEntry);
}

int fuse_reply_attr(fuse_req_t req, const struct stat* attr,
                    double attr_timeout) {
  assert(g_fuse);
  assert(attr);
  g_fuse->attrs.emplace_back(*attr, attr_timeout);
  return 0;
}

int fuse_reply_buf(fuse_req_t req, const char* buf, size_t size) {
  assert(g_fuse);
  std::vector<char> data;
  if (buf && size > 0) {
    data.insert(data.end(), buf, buf + size);
  }
  g_fuse->buffers.push_back(std::move(data));
  return 0;
}

int fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param* e) {
  assert(g_fuse);
  assert(e);
  g_fuse->entries.push_back(*e);
  return 0;
}

int fuse_reply_err(fuse_req_t req, int err) {
  assert(g_fuse);
  g_fuse->errors.push_back(err);
  return 0;
}

int fuse_reply_open(fuse_req_t req, const struct fuse_file_info* fi) {
  assert(g_fuse);
  assert(fi);
  g_fuse->open_files.push_back(*fi);
  return 0;
}

void fuse_reply_none(fuse_req_t req) {
  assert(g_fuse);
  ++g_fuse->none_counter;
}

int fuse_reply_statfs(fuse_req_t req, const struct statvfs* stbuf) { return 0; }

struct fuse_context* fuse_get_context() {
  assert(g_fuse);
  return &g_fuse->context;
}

}  // namespace cdc_ft
