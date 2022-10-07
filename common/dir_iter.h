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

#ifndef COMMON_DIR_ITER_H_
#define COMMON_DIR_ITER_H_

#include <cstdio>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace cdc_ft {

// Flags for selecting the type of directory entries when iterating. This allows
// one to narrow down the results that a DirectoryIterator returns.
enum class DirectorySearchFlags {
  kNone = 0,
  kDirectories = (1 << 0),
  kFiles = (1 << 1),
  kFilesAndDirectories = kFiles | kDirectories,
};

class DirectoryIterator;

// A DirectoryEntry describes a file or a directory that was found when
// iterating through the file system.
class DirectoryEntry {
 public:
  DirectoryEntry();
  ~DirectoryEntry();

  // Copy and assignment are disabled due to the separate implementation class.
  DirectoryEntry(DirectoryEntry const&) = delete;
  DirectoryEntry& operator=(DirectoryEntry const&) = delete;

  // Returns true if the entry describes an existing item.
  bool Valid() const;

  // Returns true if the entry is valid and describes a directory.
  bool IsDir() const;

  // Returns true if the entry is valid and describes a regular file.
  bool IsRegularFile() const;

  // Returns true if the entry is valid and describes a symlink.
  bool IsSymlink() const;

  // Returns the name for this entry, without any path component.
  const std::string& Name() const;

  // Returns the relative path from the originating DirectoryIterator to the
  // directory that contains this entry (excluding this entry).
  const std::string& RelPath() const;

  // Returns the relative path and filename from the originating
  // DirectoryIterator to the this entry (including this entry).
  std::string RelPathName() const;

  // Frees all data associated with this entry. After calling Clear(), this
  // entry is no longer valid.
  void Clear();

 private:
  class Impl;
  friend class DirectoryIterator;
  void SetImpl(Impl* impl);
  Impl* impl_;
};

// This class allows recursive listing of directory contents.
class DirectoryIterator {
 public:
  // Default constructor.
  DirectoryIterator();

  // Contructs a new iterator and immediately calls Open() using the given
  // parameters.
  DirectoryIterator(
      const std::string& path,
      DirectorySearchFlags results = DirectorySearchFlags::kFilesAndDirectories,
      bool recursive = true);

  // Destructor
  ~DirectoryIterator();

  // Copy and assignment are disabled due to the separate implementation class.
  DirectoryIterator(DirectoryIterator const&) = delete;
  DirectoryIterator& operator=(DirectoryIterator const&) = delete;

  // Returns any error that might have occured so far. Note that the iterator
  // ignores any permission errors that might occur when recursing into
  // restricted sub-directories and just continues with the next directory.
  absl::Status Status() const;

  // Returns true as long as a directory has been opened, no error has occured,
  // and a call to NextEntry() has a chance to succeed.
  bool Valid() const;

  // Returns the path that was given to open the first directory.
  const std::string& Path() const;

  // Opens the given directory path for reading. If this method returns true, a
  // directory entry may be fetched by calling NextEntry(). The results
  // parameter can be used to control which types of directory entries a call to
  // NextEntry() might yield. The recustive parameter controls whether or not
  // the iterator recurses into sub-directories in a DFS manner.
  //
  // In case of an error (including permission errors), this function returns
  // false. Check Status() for the actual error in that case.
  bool Open(
      const std::string& path,
      DirectorySearchFlags results = DirectorySearchFlags::kFilesAndDirectories,
      bool recursive = true);

  // Yields the next directory entry that matches the DirectorySearchFlags that
  // were given in the call to Open(). Returns true if a new entry was found,
  // false in case of an error or if no more entries are available. Check
  // Status() to distinguish between those two cases.
  //
  // Note: The iterator ignores permission errors that occur in any
  // sub-directory and just continues with the next directory.
  bool NextEntry(DirectoryEntry* entry);

 private:
  class Impl;
  Impl* impl_;
};

};  // namespace cdc_ft

#endif  // COMMON_DIR_ITER_H_
