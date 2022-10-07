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

#ifndef MANIFEST_FAKE_MANIFEST_BUILDER_H_
#define MANIFEST_FAKE_MANIFEST_BUILDER_H_

#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

class MemDataStore;

// In-memory manifest builder. Useful for testing.
class FakeManifestBuilder {
 public:
  // Permissions assigned to the root directory.
  static constexpr uint32_t kRootDirPerms = 0755u;

  explicit FakeManifestBuilder(MemDataStore* store);
  ~FakeManifestBuilder();

  // Adds a new file of given the |name| to the directory |dir_asset| and sets
  // the modified time to |mtime_sec| and permissions to |permissions|. Also
  // generates data chunks from |data| using fastcdc (hardcoded chunk sizes).
  // Use builder.AddFile(builder.Root(), ...) to add a file to the root
  // directory.
  void AddFile(AssetProto* dir_asset, const char* name, int64_t mtime_sec,
               uint32_t permissions, const std::vector<char>& data);

  // Adds a new directory of the given |name| to the directory |dir_asset| and
  // sets the modified time to |mtime_sec| and permissions to |permissions|.
  // Returns a pointer to the new directory that can be used to further add
  // files or subdirectories.
  // Use builder.AddDirectory(builder.Root(), ...) to add a directory to the
  // root directory.
  AssetProto* AddDirectory(AssetProto* dir_asset, const char* name,
                           int64_t mtime_sec, uint32_t permissions);

  // Builds a fake directory structure with files and subdirectories suitable
  // for prototyping/testing.
  ContentIdProto BuildTestData();

  // Returns the built manifest.
  const ManifestProto* Manifest() const;

  // Shortcut to &Manifest()->root_dir().
  AssetProto* Root();

  // Updates the file |name| with new |permissions|, |mtime_sec|, and |data|.
  void ModifyFile(AssetProto* dir_asset, const char* name, int64_t mtime_sec,
                  uint32_t permissions, const std::vector<char>& data);

 private:
  MemDataStore* const store_;
  ManifestProto manifest_;
};

}  // namespace cdc_ft

#endif  // MANIFEST_FAKE_MANIFEST_BUILDER_H_
