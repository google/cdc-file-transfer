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

#ifndef CDC_RSYNC_CDC_RSYNC_H_
#define CDC_RSYNC_CDC_RSYNC_H_

#ifdef COMPILING_DLL
#define CDC_RSYNC_API __declspec(dllexport)
#else
#define CDC_RSYNC_API __declspec(dllimport)
#endif

namespace cdc_ft {

#ifdef __cplusplus
extern "C" {
#endif

struct Options {
  const char* ip = nullptr;
  int port = 0;
  bool delete_ = false;
  bool recursive = false;
  int verbosity = 0;
  bool quiet = false;
  bool whole_file = false;
  bool relative = false;
  bool compress = false;
  bool checksum = false;
  bool dry_run = false;
  bool existing = false;
  bool json = false;
  const char* copy_dest = nullptr;
  int compress_level = 6;
  int connection_timeout_sec = 10;

  // Compression level 0 is invalid.
  static constexpr int kMinCompressLevel = -5;
  static constexpr int kMaxCompressLevel = 22;
};

// Rule for including/excluding files.
struct FilterRule {
  enum class Type {
    kInclude,
    kExclude,
  };

  Type type;
  const char* pattern;

  FilterRule(Type type, const char* pattern) : type(type), pattern(pattern) {}
};

enum class ReturnCode {
  // No error. Will match the tool's exit code, so OK must be 0.
  kOk = 0,

  // Generic error.
  kGenericError = 1,

  // Server connection timed out.
  kConnectionTimeout = 2,

  // Connection to the server was shut down unexpectedly.
  kConnectionLost = 3,

  // Binding to the forward port failed, probably because there's another
  // instance of cdc_rsync running.
  kAddressInUse = 4,

  // Server deployment failed. This should be rare, it means that the server
  // components were successfully copied, but the up-to-date check still fails.
  kDeployFailed = 5,

  // Gamelet selection asks for user input, but we are in quiet mode.
  kInstancePickerNotAvailableInQuietMode = 6,
};

// Calling Sync() a second time overwrites the data in |error_message|.
CDC_RSYNC_API ReturnCode Sync(const Options* options,
                              const FilterRule* filter_rules,
                              size_t filter_num_rules, const char* sources_dir,
                              const char* const* sources, size_t num_sources,
                              const char* destination,
                              const char** error_message);

#ifdef __cplusplus
}  // extern "C"
#endif

}  // namespace cdc_ft

#endif  // CDC_RSYNC_CDC_RSYNC_H_
