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

#ifndef MANIFEST_MANIFEST_ITERATOR_H_
#define MANIFEST_MANIFEST_ITERATOR_H_

#include <list>

#include "data_store/data_store_reader.h"

namespace cdc_ft {

class ManifestIterator {
 public:
  // Constructs a new manifest iterator that can read a manifest proto from the
  // given |chunk_store|.
  explicit ManifestIterator(DataStoreReader* data_store);
  ~ManifestIterator();

  // Opens the manifest identified by |manifest_id| from the chunk store. If
  // this method returns an Ok() status, an AssetProto may be fetched by
  // calling NextEntry(). In case of an error, the value of Status() is
  // returned.
  absl::Status Open(const ContentIdProto& manifest_id);

  // Opens the manifest stored in the file path given as |manifest_file|.
  // Further chunks will be read from the chunk store, if needed. If this method
  // returns an Ok() status, an AssetProto may be fetched by calling
  // NextEntry(). In case of an error, the value of Status() is returned.
  absl::Status Open(const std::string& manifest_file);

  // Returns any error that might have occured so far.
  absl::Status Status() const { return status_; }

  // Returns true as long as a manifest has been opened, no error has occured,
  // and a call to NextEntry() has a chance to succeed.
  bool Valid() const;

  // Yields the next asset from the opened manifest. Returns nullptr in case of
  // an error or if no more assets are available. Check Status() to distinguish
  // between those two cases.
  //
  // Calling NextEntry() invalidates any references to objects returned by
  // previous calls to this function.
  const AssetProto* NextEntry();

  // Returns the current relative path. This corresponds to the directory path
  // in which the asset returned from the last call to NextEntry() is located,
  // relative to the manifest root.
  const std::string& RelativePath() const { return rel_path_; }

  // Returns a reference to the loaded manifest proto. Only valid after a
  // successful call to Open().
  const ManifestProto& Manifest() const { return manifest_; }

 private:
  struct OpenedDirectory;

  // Resets the iterator for a new Open() call.
  void Reset();

  // Returns the AssetProto at |index| from the given list |assets|. If the
  // AssetProto is of type DIRECTORY, it is pushed on top of the stack of open
  // directories. Does not check if |index| is out-of-bounds.
  AssetProto* MutableAsset(RepeatedAssetProto* assets, int index);

  // Updates the relative path according to the current stack of opened
  // directories.
  void UpdateRelPath(const OpenedDirectory* od);

  ManifestProto manifest_;
  std::list<OpenedDirectory> dirs_;
  const OpenedDirectory* last_opened_dir_;
  std::string rel_path_;
  absl::Status status_;
  DataStoreReader* data_store_;
};

}  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_ITERATOR_H_
