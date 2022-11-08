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

#include "common/path_filter.h"
#include "common/remote_util.h"

#ifndef CDC_RSYNC_CDC_RSYNC_H_
#define CDC_RSYNC_CDC_RSYNC_H_

namespace cdc_ft {

struct Options {
  int port = RemoteUtil::kDefaultSshPort;
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
  std::string copy_dest;
  int compress_level = 6;
  int connection_timeout_sec = 10;
  std::string ssh_command;
  std::string scp_command;
  std::string sources_dir;  // Base directory for files loaded for --files-from.
  PathFilter filter;

  // Compression level 0 is invalid.
  static constexpr int kMinCompressLevel = -5;
  static constexpr int kMaxCompressLevel = 22;
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
};

// Calling Sync() a second time overwrites the data in |error_message|.
ReturnCode Sync(const Options& options, const std::vector<std::string>& sources,
                const std::string& user_host, const std::string& destination,
                std::string* error_message);

}  // namespace cdc_ft

#endif  // CDC_RSYNC_CDC_RSYNC_H_
