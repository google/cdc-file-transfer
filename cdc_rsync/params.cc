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

#include "cdc_rsync/params.h"

#include <cassert>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "common/path.h"
#include "common/port_range_parser.h"
#include "common/remote_util.h"
#include "lib/zstd.h"

namespace cdc_ft {
namespace params {
namespace {

using Options = CdcRsyncClient::Options;

template <typename... Args>
void PrintError(const absl::FormatSpec<Args...>& format, Args... args) {
  std::cerr << "Error: " << absl::StrFormat(format, args...) << std::endl;
}

enum class OptionResult { kConsumedKey, kConsumedKeyValue, kError };

const char kHelpText[] =
    R"(Copy local files to a gamelet

Synchronizes local files and files on a gamelet. Matching files are skipped.
For partially matching files only the deltas are transferred.

Usage:
  cdc_rsync [options] source [source]... [user@]host:destination

Parameters:
  source                    Local file or directory to be copied
  user                      Remote SSH user name
  host                      Remote host or IP address
  destination               Remote destination directory

Options:
    --contimeout sec        Gamelet connection timeout in seconds (default: 10)
-q, --quiet                 Quiet mode, only print errors
-v, --verbose               Increase output verbosity
    --json                  Print JSON progress
-n, --dry-run               Perform a trial run with no changes made
-r, --recursive             Recurse into directories
    --delete                Delete extraneous files from destination directory
-z, --compress              Compress file data during the transfer
    --compress-level <num>  Explicitly set compression level (default: 6)
-c, --checksum              Skip files based on checksum, not mod-time & size
-W, --whole-file            Always copy files whole,
                            do not apply delta-transfer algorithm
    --exclude pattern       Exclude files matching pattern
    --exclude-from <file>   Read exclude patterns from file
    --include pattern       Don't exclude files matching pattern
    --include-from <file>   Read include patterns from file
    --files-from <file>     Read list of source files from file
-R, --relative              Use relative path names
    --existing              Skip creating new files on instance
    --copy-dest <dir>       Use files from dir as sync base if files are missing
    --ssh-command <cmd>     Path and arguments of ssh command to use, e.g.
                            "C:\path\to\ssh.exe -p 12345 -i id_rsa -oUserKnownHostsFile=known_hosts"
                            Can also be specified by the CDC_SSH_COMMAND environment variable.
    --sftp-command <cmd>    Path and arguments of sftp command to use, e.g.
                            "C:\path\to\sftp.exe -P 12345 -i id_rsa -oUserKnownHostsFile=known_hosts"
                            Can also be specified by the CDC_SFTP_COMMAND environment variable.
    --forward-port <port>   TCP port or range used for SSH port forwarding (default: 44450-44459).
                            If a range is specified, searches for available ports (slower).
-h  --help                  Help for cdc_rsync
)";

constexpr char kSshCommandEnvVar[] = "CDC_SSH_COMMAND";
constexpr char kScpCommandEnvVar[] = "CDC_SCP_COMMAND";
constexpr char kSftpCommandEnvVar[] = "CDC_SFTP_COMMAND";

// Populates some parameters from environment variables.
void PopulateFromEnvVars(Parameters* parameters) {
  path::GetEnv(kSshCommandEnvVar, &parameters->options.ssh_command)
      .IgnoreError();
  path::GetEnv(kScpCommandEnvVar, &parameters->options.deprecated_scp_command)
      .IgnoreError();
  path::GetEnv(kSftpCommandEnvVar, &parameters->options.sftp_command)
      .IgnoreError();
}

// Returns false and prints an error if |value| is null or empty.
bool ValidateValue(const std::string& option_name, const char* value) {
  if (!value) {
    PrintError("Option '%s' needs a value", option_name);
    return false;
  }
  return true;
}

// Handles the --exclude-from and --include-from options.
OptionResult HandleFilterRuleFile(const std::string& option_name,
                                  const char* path, PathFilter::Rule::Type type,
                                  Parameters* params) {
  assert(path);
  std::vector<std::string> patterns;
  absl::Status status = path::ReadAllLines(
      path, &patterns,
      path::ReadFlags::kRemoveEmpty | path::ReadFlags::kTrimWhitespace);
  if (!status.ok()) {
    PrintError("Failed to read file '%s' for %s option: %s", path, option_name,
               status.message());
    return OptionResult::kError;
  }

  for (std::string& pattern : patterns) {
    params->options.filter.AddRule(type, std::move(pattern));
  }
  return OptionResult::kConsumedKeyValue;
}

// Loads sources for --files-from option. |sources| must contain at most one
// path and that path must be an existing directory. This directory is returned
// in |sources_dir|. The method then loads all sources line-by-line from
// |sources_file| and stores them into |sources|.
bool LoadFilesFrom(const std::string& files_from,
                   std::vector<std::string>* sources,
                   std::string* sources_dir) {
  if (sources->size() > 1) {
    PrintError(
        "Expected at most 1 source for the --files-from option, but %u "
        "provided",
        sources->size());
    return false;
  }
  if (sources->size() == 1 && !path::DirExists(sources->at(0))) {
    PrintError(
        "The source '%s' must be an existing directory for the --files-from "
        "option",
        sources->at(0));
    return false;
  }
  *sources_dir = sources->empty() ? std::string() : sources->at(0);
  if (!sources_dir->empty()) {
    path::EnsureEndsWithPathSeparator(sources_dir);
  }

  sources->clear();
  absl::Status status = path::ReadAllLines(
      files_from, sources,
      path::ReadFlags::kRemoveEmpty | path::ReadFlags::kTrimWhitespace);
  if (!status.ok()) {
    PrintError("Failed to read sources file '%s' for files-from option: %s",
               files_from, status.message());
    return false;
  }

  if (sources->empty()) {
    PrintError("The file '%s' specified in the --files-from option is empty",
               files_from);
  }

  return true;
}

OptionResult HandleParameter(const std::string& key, const char* value,
                             Parameters* params, bool* help) {
  if (key == "delete") {
    params->options.delete_ = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "r" || key == "recursive") {
    params->options.recursive = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "v" || key == "verbosity") {
    params->options.verbosity++;
    return OptionResult::kConsumedKey;
  }

  if (key == "q" || key == "quiet") {
    params->options.quiet = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "W" || key == "whole-file") {
    params->options.whole_file = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "include") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.filter.AddRule(PathFilter::Rule::Type::kInclude, value);
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "include-from") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    return HandleFilterRuleFile(key, value, PathFilter::Rule::Type::kInclude,
                                params);
  }

  if (key == "exclude") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.filter.AddRule(PathFilter::Rule::Type::kExclude, value);
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "exclude-from") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    return HandleFilterRuleFile(key, value, PathFilter::Rule::Type::kExclude,
                                params);
  }

  if (key == "files-from") {
    // Implies -R.
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.relative = true;
    params->files_from = value;
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "R" || key == "relative") {
    params->options.relative = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "z" || key == "compress") {
    params->options.compress = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "compress-level") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.compress_level = atoi(value);
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "contimeout") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.connection_timeout_sec = atoi(value);
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "h" || key == "help") {
    *help = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "c" || key == "checksum") {
    params->options.checksum = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "n" || key == "dry-run") {
    params->options.dry_run = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "existing") {
    params->options.existing = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "copy-dest") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.copy_dest = value;
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "json") {
    params->options.json = true;
    return OptionResult::kConsumedKey;
  }

  if (key == "ssh-command") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.ssh_command = value;
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "scp-command") {
    // Backwards compatibility. Note that this flag is hidden from the help.
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.deprecated_scp_command = value;
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "sftp-command") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    params->options.sftp_command = value;
    return OptionResult::kConsumedKeyValue;
  }

  if (key == "forward-port") {
    if (!ValidateValue(key, value)) return OptionResult::kError;
    uint16_t first, last;
    if (!port_range::Parse(value, &first, &last)) {
      PrintError("Failed to parse %s=%s, expected <port> or <port1>-<port2>",
                 key, value);
      return OptionResult::kError;
    }
    params->options.forward_port_first = first;
    params->options.forward_port_last = last;
    return OptionResult::kConsumedKeyValue;
  }

  PrintError("Unknown option: '%s'", key);
  return OptionResult::kError;
}

bool ValidateParameters(const Parameters& params, bool help) {
  if (help) {
    std::cout << kHelpText;
    return false;
  }

  if (params.options.delete_ && !params.options.recursive) {
    PrintError("--delete does not work without --recursive (-r).");
    return false;
  }

  // Note: ZSTD_minCLevel() is ridiculously small (-131072), so use a
  // reasonable value.
  assert(ZSTD_minCLevel() <= Options::kMinCompressLevel);
  assert(ZSTD_maxCLevel() == Options::kMaxCompressLevel);
  static_assert(Options::kMinCompressLevel < 0);
  static_assert(Options::kMaxCompressLevel > 0);
  if (params.options.compress_level < Options::kMinCompressLevel ||
      params.options.compress_level > Options::kMaxCompressLevel ||
      params.options.compress_level == 0) {
    PrintError("--compress_level must be between %i..-1 or 1..%i",
               Options::kMinCompressLevel, Options::kMaxCompressLevel);
    return false;
  }

  // Warn that any include rules not followed by an exclude rule are pointless
  // as the files would be included, anyway.
  const std::vector<PathFilter::Rule>& rules = params.options.filter.GetRules();
  for (int n = static_cast<int>(rules.size()) - 1; n >= 0; --n) {
    const PathFilter::Rule& rule = rules[n];
    if (rule.type == PathFilter::Rule::Type::kExclude) {
      break;
    }
    std::cout << "Warning: Include pattern '" << rule.pattern
              << "' has no effect, not followed by exclude pattern"
              << std::endl;
  }

  if (params.sources.empty() && params.destination.empty()) {
    PrintError("Missing source and destination");
    return false;
  }

  if (params.destination.empty()) {
    PrintError("Missing destination");
    return false;
  }

  if (params.sources.empty()) {
    // If one arg was passed on the command line, it is not clear whether it
    // was supposed to be a source or destination.
    PrintError("Missing source or destination");
    return false;
  }

  if (params.user_host.empty()) {
    PrintError(
        "No remote host specified in destination '%s'. "
        "Expected [user@]host:destination.",
        params.destination);
    return false;
  }

  return true;
}

bool CheckOptionResult(OptionResult result, const std::string& name,
                       const char* value) {
  switch (result) {
    case OptionResult::kConsumedKey:
      return true;

    case OptionResult::kConsumedKeyValue:
      return ValidateValue(name, value);

    case OptionResult::kError:
      // Error message was already printed.
      return false;
  }

  return true;
}

// Removes the user/host part of |destination| and puts it into |user_host|,
// e.g. if |destination| is initially "user@foo.com:~/file", it is "~/file"
// afterward and |user_host| is |user@foo.com|. Does not touch Windows drives,
// e.g. C:\foo.
void PopUserHost(std::string* destination, std::string* user_host) {
  std::vector<std::string> parts =
      absl::StrSplit(*destination, absl::MaxSplits(':', 1));
  if (parts.size() < 2) return;

  // Don't mistake the C part of C:\foo as user/host.
  if (parts[0].size() == 1 && toupper(parts[0][0]) >= 'A' &&
      toupper(parts[0][0]) <= 'Z') {
    return;
  }

  *user_host = parts[0];
  *destination = parts[1];
}

}  // namespace

const char* HelpText() { return kHelpText; }

// Note that abseil has a flags library, but the C++ version doesn't support
// short names ("-q"), see https://abseil.io/docs/cpp/guides/flags. However, we
// aim to be roughly compatible with vanilla rsync, which does have short flag
// names like "-q".
bool Parse(int argc, const char* const* argv, Parameters* parameters) {
  if (argc <= 1) {
    std::cout << kHelpText;
    return false;
  }

  // Before applying args, populate parameters from env vars.
  PopulateFromEnvVars(parameters);

  bool help = false;
  for (int index = 1; index < argc; ++index) {
    // Handle '--key [value]' and '--key=value' options.
    bool equality_used = false;
    if (strncmp(argv[index], "--", 2) == 0) {
      std::string key(argv[index] + 2);
      const char* value = nullptr;
      size_t equality_pos = key.find("=");
      if (equality_pos != std::string::npos) {
        if (equality_pos + 1 < key.size()) {
          value = argv[index] + 2 + equality_pos + 1;
        }
        key = key.substr(0, equality_pos);
        equality_used = true;
      } else {
        value = index + 1 < argc && argv[index + 1][0] != '-' ? argv[index + 1]
                                                              : nullptr;
      }
      OptionResult result = HandleParameter(key, value, parameters, &help);
      if (!CheckOptionResult(result, key, value)) {
        return false;
      }
      if (!equality_used && result == OptionResult::kConsumedKeyValue) {
        ++index;
      }
      continue;
    }

    // Handle '-abc' options.
    if (strncmp(argv[index], "-", 1) == 0) {
      char key[] = "x";
      char name[] = "-x";
      for (const char* c = argv[index] + 1; *c != 0; ++c) {
        key[0] = *c;
        name[1] = *c;
        OptionResult result = HandleParameter(key, nullptr, parameters, &help);
        // These args shouldn't try to consume values.
        assert(result != OptionResult::kConsumedKeyValue);
        if (!CheckOptionResult(result, name, nullptr)) {
          return false;
        }
      }
      continue;
    }

    // The last added option is the destination. Move previously added options
    // to the sources.
    if (!parameters->destination.empty()) {
      parameters->sources.push_back(std::move(parameters->destination));
    }
    parameters->destination = argv[index];
  }

  // Load files-from file (can't do it when --files-from is handled since not
  // all sources might have been read at that point.
  if (!parameters->files_from.empty() &&
      !LoadFilesFrom(parameters->files_from, &parameters->sources,
                     &parameters->options.sources_dir)) {
    return false;
  }

  PopUserHost(&parameters->destination, &parameters->user_host);

  // Backwards compabitility after switching to sftp. Convert scp to sftp
  // command Note that this flag is hidden from the help.
  if (parameters->options.sftp_command.empty() &&
      !parameters->options.deprecated_scp_command.empty()) {
    LOG_WARNING(
        "The CDC_SCP_COMMAND environment variable and the --scp-command flag "
        "are deprecated. Please set CDC_SFTP_COMMAND or --sftp-command "
        "instead.");

    parameters->options.sftp_command = RemoteUtil::ScpToSftpCommand(
        parameters->options.deprecated_scp_command);
    if (!parameters->options.sftp_command.empty()) {
      LOG_WARNING("Converted scp command '%s' to sftp command '%s'.",
                  parameters->options.deprecated_scp_command,
                  parameters->options.sftp_command);
    } else {
      LOG_WARNING("Failed to convert scp command '%s' to sftp command.",
                  parameters->options.deprecated_scp_command);
    }
  }

  if (!ValidateParameters(*parameters, help)) {
    return false;
  }

  return true;
}

}  // namespace params
}  // namespace cdc_ft
