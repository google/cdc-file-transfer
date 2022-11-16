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

#include <memory>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/status/status.h"
#include "absl/strings/str_split.h"
#include "cdc_stream/local_assets_stream_manager_client.h"
#include "common/log.h"
#include "common/path.h"
#include "grpcpp/channel.h"
#include "grpcpp/security/credentials.h"

ABSL_FLAG(uint16_t, service_port, 44432,
          "Port to use while connecting to the local asset stream service.");
ABSL_FLAG(
    uint16_t, ssh_port, 22,
    "Port to use while connecting to the remote instance being streamed to.");
ABSL_FLAG(std::string, ssh_command, "",
          "Path and arguments of ssh command to use, e.g. "
          "\"C:\\path\\to\\ssh.exe -F config_file\". Can also be specified by "
          "the CDC_SSH_COMMAND environment variable.");
ABSL_FLAG(std::string, scp_command, "",
          "Path and arguments of scp command to use, e.g. "
          "\"C:\\path\\to\\scp.exe -F config_file\". Can also be specified by "
          "the CDC_SCP_COMMAND environment variable.");
ABSL_FLAG(int, verbosity, 2,
          "Verbosity of the log output. Increase to make logs more verbose.");

namespace {

constexpr char kHelpText[] = R"!(Stream files from a Windows to a Linux device

Usage: cdc_stream [flags] start windows_dir [user@]host:linux_dir
       Streams the Windows directory windows_dir to directory linux_dir on the
       Linux host using SSH username user.
 
       cdc_stream [flags] stop [user@]host:linux_dir
       Stops the streaming session to the given Linux target.

Type cdc_stream --helpfull for available flags.)!";

// Splits |user_host_dir| = [user@]host:dir up into [user@]host and dir.
// Does not touch Windows drives, e.g. C:\foo.
bool ParseUserHostDir(const std::string& user_host_dir, std::string* user_host,
                      std::string* dir) {
  std::vector<std::string> parts =
      absl::StrSplit(user_host_dir, absl::MaxSplits(':', 1));
  if (parts.size() < 2 ||
      (parts[0].size() == 1 && toupper(parts[0][0]) >= 'A' &&
       toupper(parts[0][0]) <= 'Z')) {
    LOG_ERROR(
        "Failed to parse '%s'. Make sure it is of the form "
        "[user@]host:linux_dir.",
        user_host_dir);
    return false;
  }

  *user_host = parts[0];
  *dir = parts[1];
  return true;
}

std::string GetFlagFromEnvOrArgs(const char* env,
                                 const absl::Flag<std::string>& flag) {
  std::string value = absl::GetFlag(flag);
  if (value.empty()) {
    cdc_ft::path::GetEnv(env, &value).IgnoreError();
  }
  return value;
}

}  // namespace

int main(int argc, char* argv[]) {
  absl::SetProgramUsageMessage(kHelpText);
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);

  uint16_t service_port = absl::GetFlag(FLAGS_service_port);
  int verbosity = absl::GetFlag(FLAGS_verbosity);
  std::string ssh_command =
      GetFlagFromEnvOrArgs("CDC_SSH_COMMAND", FLAGS_ssh_command);
  std::string scp_command =
      GetFlagFromEnvOrArgs("CDC_SCP_COMMAND", FLAGS_scp_command);
  uint16_t ssh_port = absl::GetFlag(FLAGS_ssh_port);

  cdc_ft::LogLevel level = cdc_ft::Log::VerbosityToLogLevel(verbosity);
  cdc_ft::Log::Initialize(std::make_unique<cdc_ft::ConsoleLog>(level));

  if (args.size() < 2) {
    LOG_INFO(kHelpText);
    return 0;
  }
  std::string command = args[1];
  if (command != "start" && command != "stop") {
    LOG_ERROR("Unknown command '%s'. Must be 'start' or 'stop'.", command);
    return 1;
  }

  // Start a gRpc client.
  std::string client_address = absl::StrFormat("localhost:%u", service_port);
  std::shared_ptr<grpc::Channel> grpc_channel = grpc::CreateCustomChannel(
      client_address, grpc::InsecureChannelCredentials(),
      grpc::ChannelArguments());

  cdc_ft::LocalAssetsStreamManagerClient client(grpc_channel);

  absl::Status status;
  if (command == "start") {
    if (args.size() < 4) {
      LOG_ERROR(
          "Command 'start' needs 2 arguments: the Windows directory to stream"
          " and the Linux target [user@]host:dir.");
      return 1;
    }

    std::string src_dir = args[2];
    std::string user_host, mount_dir;
    if (!ParseUserHostDir(args[3], &user_host, &mount_dir)) return 1;

    status = client.StartSession(src_dir, user_host, ssh_port, mount_dir,
                                 ssh_command, scp_command);
    if (status.ok()) {
      LOG_INFO("Started streaming directory '%s' to '%s:%s'", src_dir,
               user_host, mount_dir);
    }
  } else /* if (command == "stop") */ {
    if (args.size() < 3) {
      LOG_ERROR(
          "Command 'stop' needs 1 argument: the Linux target "
          "[user@]host:linux_dir.");
      return 1;
    }

    std::string user_host, mount_dir;
    if (!ParseUserHostDir(args[2], &user_host, &mount_dir)) return 1;

    status = client.StopSession(user_host, mount_dir);
    if (status.ok()) {
      LOG_INFO("Stopped streaming session to '%s:%s'", user_host, mount_dir);
    }
  }

  if (!status.ok()) {
    LOG_ERROR("Error: %s", status.ToString());
  }

  cdc_ft::Log::Shutdown();
  return static_cast<int>(status.code());
}
