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

#ifndef CDC_FUSE_FS_MOCK_LIBFUSE_H_
#define CDC_FUSE_FS_MOCK_LIBFUSE_H_

#include <fcntl.h>
#include <sys/stat.h>

#include <cstdint>
#include <vector>

namespace cdc_ft {

//
// The interface below mimics the part of the FUSE low level interface we need.
// See include/fuse_lowlevel.h for more information.
//

// Definitions.
using fuse_ino_t = uint64_t;
using fuse_req_t = void*;
using nlink_t = uint64_t;

constexpr fuse_ino_t FUSE_ROOT_ID = 1;
#ifndef O_DIRECT
constexpr uint32_t O_DIRECT = 040000;
#endif

struct fuse_entry_param {
  fuse_ino_t ino;
  struct stat attr;
  double attr_timeout;
  double entry_timeout;
};

struct fuse_file_info {
  int flags = O_RDONLY;
  unsigned int direct_io : 1;
  unsigned int keep_cache : 1;

  fuse_file_info() : direct_io(0), keep_cache(0) {}
  explicit fuse_file_info(int flags)
      : flags(flags), direct_io(0), keep_cache(0) {}
};

struct fuse_forget_data {
  uint64_t ino;
  uint64_t nlookup;
};

struct fuse_context {
  int uid;
  int gid;
};

struct statvfs {
  uint32_t f_bsize;
  uint32_t f_namemax;
};

// FUSE reply/action functions.
size_t fuse_add_direntry(fuse_req_t req, char* buf, size_t bufsize,
                         const char* name, const struct stat* stbuf, off_t off);
int fuse_reply_attr(fuse_req_t req, const struct stat* attr,
                    double attr_timeout);
int fuse_reply_buf(fuse_req_t req, const char* buf, size_t size);
int fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param* e);
int fuse_reply_err(fuse_req_t req, int err);
int fuse_reply_open(fuse_req_t req, const struct fuse_file_info* fi);
void fuse_reply_none(fuse_req_t req);
int fuse_reply_statfs(fuse_req_t req, const struct statvfs* stbuf);
struct fuse_context* fuse_get_context();

// FUSE mocking class. Basically just a recorder for the fuse_* callbacks above.
struct MockLibFuse {
 public:
  MockLibFuse();
  ~MockLibFuse();

  struct Attr {
    struct stat value;
    double timeout;
    Attr(struct stat value, double timeout)
        : value(std::move(value)), timeout(timeout) {}
  };
  void SetUid(int uid);
  void SetGid(int gid);

  // Struct stored in the buffer |buf| by fuse_add_direntry().
  // Uses a maximum name size for simplicity.
  struct DirEntry {
    fuse_ino_t ino;
    uint32_t mode;
    char name[32];
    off_t off;
  };

  std::vector<fuse_entry_param> entries;
  std::vector<Attr> attrs;
  std::vector<int> errors;
  std::vector<fuse_file_info> open_files;
  std::vector<std::vector<char>> buffers;
  unsigned int none_counter = 0;
  fuse_context context;
};

}  // namespace cdc_ft

#endif  // CDC_FUSE_FS_MOCK_LIBFUSE_H_
