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

#include "common/dir_iter.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cassert>
#include <list>

#if defined(_MSC_VER)
#include "dirent.h"
#else
#include <dirent.h>
#endif

#include <algorithm>
#include <memory>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "common/errno_mapping.h"
#include "common/path.h"

namespace cdc_ft {
namespace {
const std::string empty_string;
}

// Allows testing if a specific flag is set using the & operator.
inline int operator&(DirectorySearchFlags a, DirectorySearchFlags b) {
  using T = std::underlying_type_t<DirectorySearchFlags>;
  return static_cast<int>(static_cast<T>(a) & static_cast<T>(b));
}

class DirectoryEntry::Impl {
  friend class DirectoryEntry;

 public:
  Impl(const std::string& rel_path, const dirent* dent)
      : rel_path(rel_path),
        name(dent ? dent->d_name : ""),
        type(dent ? dent->d_type : 0) {}
  const std::string rel_path;
  const std::string name;
  const int type;
};

DirectoryEntry::DirectoryEntry() : impl_(nullptr) {}

DirectoryEntry::~DirectoryEntry() {
  if (impl_) delete impl_;
}

bool DirectoryEntry::Valid() const { return impl_ && impl_->type; }

bool DirectoryEntry::IsDir() const { return impl_ && impl_->type & DT_DIR; }

bool DirectoryEntry::IsRegularFile() const {
  return impl_ && impl_->type & DT_REG;
}

bool DirectoryEntry::IsSymlink() const { return impl_ && impl_->type & DT_LNK; }

const std::string& DirectoryEntry::Name() const {
  return impl_ ? impl_->name : empty_string;
}

const std::string& DirectoryEntry::RelPath() const {
  return impl_ ? impl_->rel_path : empty_string;
}

std::string DirectoryEntry::RelPathName() const {
  return impl_ ? path::Join(impl_->rel_path, impl_->name) : std::string();
}

void DirectoryEntry::Clear() { SetImpl(nullptr); }

void DirectoryEntry::SetImpl(Impl* impl) {
  if (impl_) delete impl_;
  impl_ = impl;
}

class DirectoryIterator::Impl {
 public:
  Impl(const std::string& path, DirectorySearchFlags results, bool recursive)
      : path_(path),
        flags_(results),
        recursive_(recursive),
        last_dir_(nullptr) {}

  ~Impl() {
    while (!DirsEmpty()) PopDir();
  }

  inline const std::string& Path() const { return path_; }

  inline bool ReturnDirs() const {
    return flags_ & DirectorySearchFlags::kDirectories;
  }

  inline bool ReturnFiles() const {
    return flags_ & DirectorySearchFlags::kFiles;
  }

  inline bool Recursive() const { return recursive_; }

  // Reads the next directory entry from the topmost opened directory. Returns
  // nullptr when all directory entries have been read or in case of an error.
  // Check errno to distinguish between those two cases.
  inline struct dirent* NextDirEntry() {
    assert(!dirs_.empty());
    // Update relative path if it changed.
    DIR* dir = dirs_.back().dir;
    if (last_dir_ != dir) {
      UpdateRelPath();
      last_dir_ = dir;
    }
    // Reset previous error so that we know if we reached the end the dir.
    errno = 0;
    return readdir(dir);
  }

  inline const std::string& RelPath() const { return rel_path_; }

  inline std::string DirsPath() { return path::Join(path_, rel_path_); }

  inline void PushDir(DIR* dir, const std::string& name) {
    assert(dir != nullptr);
    dirs_.push_back({dir, name});
  }

  inline void PopDir() {
    assert(!dirs_.empty());
    closedir(dirs_.back().dir);
    dirs_.pop_back();
  }

  inline bool DirsEmpty() const { return dirs_.empty(); }

  inline absl::Status Status() const { return status_; }
  inline void SetStatus(absl::Status status) { status_ = status; }

 private:
  // On Windows, the struct DIR::ent doesn't seem properly aligned, so the
  // DIR::ent::d_name field is unusable. Thus, we store the DIR pointer along
  // with its name in this struct.
  struct OpenedDir {
    DIR* dir;
    std::string name;
  };

  inline void UpdateRelPath() {
    rel_path_.clear();
    bool first = true;
    for (const OpenedDir& dir : dirs_) {
      if (first) {
        // The first component is already included in path_.
        first = false;
        continue;
      }
      path::Append(&rel_path_, dir.name);
    }
  }

  const std::string path_;
  const DirectorySearchFlags flags_;
  const bool recursive_;

  absl::Status status_;
  std::list<OpenedDir> dirs_;
  const DIR* last_dir_;
  std::string rel_path_;
};

DirectoryIterator::DirectoryIterator() : impl_(nullptr) {}

DirectoryIterator::DirectoryIterator(const std::string& path,
                                     DirectorySearchFlags results,
                                     bool recursive)
    : impl_(nullptr) {
  Open(path, results, recursive);
}

DirectoryIterator::~DirectoryIterator() {
  if (impl_) delete impl_;
}

absl::Status DirectoryIterator::Status() const {
  return impl_ ? impl_->Status() : absl::OkStatus();
}

bool DirectoryIterator::Valid() const {
  return impl_ && impl_->Status().ok() && !impl_->DirsEmpty();
}

const std::string& DirectoryIterator::Path() const {
  return impl_ ? impl_->Path() : empty_string;
}

bool DirectoryIterator::Open(const std::string& path,
                             DirectorySearchFlags results, bool recursive) {
  if (impl_) delete impl_;
  impl_ = new Impl(path, results, recursive);
  DIR* dir = opendir(path.c_str());
  if (dir) {
    impl_->PushDir(dir, path::BaseName(path));
    return true;
  }
  impl_->SetStatus(ErrnoToCanonicalStatus(
      errno, absl::StrFormat("Failed to open directory '%s'", path)));
  return false;
}

bool DirectoryIterator::NextEntry(DirectoryEntry* entry) {
  if (!impl_ || !impl_->Status().ok()) return false;

  // Reset last error to not report a previous one below.
  errno = 0;

  while (!impl_->DirsEmpty()) {
    struct dirent* dent = impl_->NextDirEntry();
    if (!dent) {
      if (!errno) {
        // We have reached the end of this directory.
        impl_->PopDir();
        continue;
      }
      break;
    }
    // Handle directories.
    if (dent->d_type & DT_DIR) {
      // Skip "." and ".." directory entries.
      if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, "..")) {
        continue;
      }
      // For recursive traversal, push new directory on top of the stack.
      if (impl_->Recursive()) {
        std::string dname(dent->d_name);
        std::string subdir = path::Join(impl_->DirsPath(), dname);
        DIR* dir = opendir(subdir.c_str());
        if (dir) {
          impl_->PushDir(dir, dname);
        } else if (errno == EACCES) {
          // Ignore access errors and proceed.
        } else {
          impl_->SetStatus(ErrnoToCanonicalStatus(
              errno, absl::StrFormat("Failed to open directory '%s'", subdir)));
          return false;
        }
      }
      if (impl_->ReturnDirs()) {
        entry->SetImpl(new DirectoryEntry::Impl(impl_->RelPath(), dent));
        return true;
      }
    } else if (dent->d_type & DT_REG) {
      // Handle regular files.
      if (impl_->ReturnFiles()) {
        entry->SetImpl(new DirectoryEntry::Impl(impl_->RelPath(), dent));
        return true;
      }
    }
  }

  if (errno) {
    impl_->SetStatus(ErrnoToCanonicalStatus(
        errno, absl::StrFormat("Failed to iterate over directory '%s'",
                               impl_->DirsPath())));
  }
  return false;
}

};  // namespace cdc_ft
