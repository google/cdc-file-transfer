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

#include "absl/strings/match.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace params {
namespace {

using Options = CdcRsyncClient::Options;

constexpr char kSrc[] = "source";
constexpr char kUserHostDst[] = "user@host:destination";
constexpr char kUserHost[] = "user@host";
constexpr char kDst[] = "destination";

constexpr char kSshCommandEnvVar[] = "CDC_SSH_COMMAND";
constexpr char kScpCommandEnvVar[] = "CDC_SCP_COMMAND";
constexpr char kSftpCommandEnvVar[] = "CDC_SFTP_COMMAND";

class TestLog : public Log {
 public:
  explicit TestLog() : Log(LogLevel::kInfo) {}

 protected:
  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override {
    errors_ += message;
  }

 private:
  std::string errors_;
};

std::string NeedsValueError(const char* option_name) {
  return absl::StrFormat("Option '%s' needs a value", option_name);
}

class ParamsTest : public ::testing::Test {
 public:
  void SetUp() override {
    prev_stdout_ = std::cout.rdbuf(output_.rdbuf());
    prev_stderr_ = std::cerr.rdbuf(errors_.rdbuf());
  }

  void TearDown() override {
    std::cout.rdbuf(prev_stdout_);
    std::cerr.rdbuf(prev_stderr_);

    // Clear env. They seem to be sticky sometimes and leak into other tests.
    path::SetEnv(kSshCommandEnvVar, "");
    path::SetEnv(kScpCommandEnvVar, "");
    path::SetEnv(kSftpCommandEnvVar, "");
  }

 protected:
  void ExpectNoError() const {
    EXPECT_TRUE(errors_.str().empty())
        << "Expected empty stderr but got\n'" << errors_.str() << "'";
  }

  void ExpectOutput(const std::string& expected) const {
    EXPECT_TRUE(absl::StrContains(output_.str(), expected))
        << "Expected stdout to contain '" << expected << "' but got\n'"
        << output_.str() << "'";
  }

  void ExpectError(const std::string& expected) const {
    EXPECT_TRUE(absl::StrContains(errors_.str(), expected))
        << "Expected stderr to contain '" << expected << "' but got\n'"
        << errors_.str() << "'";
  }

  void ClearErrors() { errors_.str(std::string()); }

  std::string base_dir_ = GetTestDataDir("params");
  std::string sources_file_ = path::Join(base_dir_, "source_files.txt");
  std::string empty_sources_file_ =
      path::Join(base_dir_, "empty_source_files.txt");

  Parameters parameters_;
  std::stringstream output_;
  std::stringstream errors_;
  std::streambuf* prev_stdout_;
  std::streambuf* prev_stderr_;
};

TEST_F(ParamsTest, ParseSucceedsDefaults) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_FALSE(parameters_.options.delete_);
  EXPECT_FALSE(parameters_.options.recursive);
  EXPECT_EQ(0, parameters_.options.verbosity);
  EXPECT_FALSE(parameters_.options.quiet);
  EXPECT_FALSE(parameters_.options.whole_file);
  EXPECT_FALSE(parameters_.options.compress);
  EXPECT_FALSE(parameters_.options.checksum);
  EXPECT_FALSE(parameters_.options.dry_run);
  EXPECT_TRUE(parameters_.options.copy_dest.empty());
  EXPECT_EQ(6, parameters_.options.compress_level);
  EXPECT_EQ(10, parameters_.options.connection_timeout_sec);
  EXPECT_EQ(1, parameters_.sources.size());
  EXPECT_EQ(parameters_.sources[0], kSrc);
  EXPECT_EQ(parameters_.user_host, kUserHost);
  EXPECT_EQ(parameters_.destination, kDst);
  ExpectNoError();
}

TEST_F(ParamsTest, ParseSucceedsWithOptionFromTwoArguments) {
  const char* argv[] = {
      "cdc_rsync.exe", "--compress-level", "2", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.compress_level, 2);
  ExpectNoError();
}

TEST_F(ParamsTest,
       ParseSucceedsWithOptionFromOneArgumentWithEqualityWithValue) {
  const char* argv[] = {"cdc_rsync.exe", "--compress-level=2", kSrc,
                        kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ASSERT_EQ(parameters_.sources.size(), 1);
  EXPECT_EQ(parameters_.options.compress_level, 2);
  EXPECT_EQ(parameters_.sources[0], kSrc);
  EXPECT_EQ(parameters_.user_host, kUserHost);
  EXPECT_EQ(parameters_.destination, kDst);
  ExpectNoError();
}

TEST_F(ParamsTest, ParseFailsOnCompressLevelEqualsNoValue) {
  const char* argv[] = {"cdc_rsync.exe", "--compress-level=", kSrc,
                        kUserHostDst, NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("compress-level"));
}

TEST_F(ParamsTest, ParseFailsOnContimeoutEqualsNoValue) {
  const char* argv[] = {"cdc_rsync.exe", "--contimeout=", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("contimeout"));
}

TEST_F(ParamsTest, ParseSucceedsWithSshSftpCommands) {
  const char* argv[] = {
      "cdc_rsync.exe",          kSrc, kUserHostDst, "--ssh-command=sshcmd",
      "--sftp-command=sftpcmd", NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "sftpcmd");
  EXPECT_EQ(parameters_.options.ssh_command, "sshcmd");
}

TEST_F(ParamsTest, ParseSucceedsWithSshSftpCommandsByEnvVars) {
  EXPECT_OK(path::SetEnv(kSshCommandEnvVar, "sshcmd"));
  EXPECT_OK(path::SetEnv(kSftpCommandEnvVar, "sftpcmd"));
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "sftpcmd");
  EXPECT_EQ(parameters_.options.ssh_command, "sshcmd");
}

TEST_F(ParamsTest, ParseSucceedsWithScpCommandFallback) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst,
                        "--scp-command=C:\\scp.exe foo", NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "C:\\sftp.exe foo");
}

TEST_F(ParamsTest, ParseSucceedsWithScpCommandFallbackByEnvVar) {
  EXPECT_OK(path::SetEnv(kScpCommandEnvVar, "C:\\scp.exe foo"));
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "C:\\sftp.exe foo");
}

TEST_F(ParamsTest, ParseSucceedsWithSftpOverwritingScp) {
  const char* argv[] = {"cdc_rsync.exe",
                        kSrc,
                        kUserHostDst,
                        "--scp-command=C:\\scp.exe foo",
                        "--sftp-command=sftpcmd",
                        NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "sftpcmd");
}

TEST_F(ParamsTest, ParseSucceedsWithSftpEnvVarOverwritingScp) {
  EXPECT_OK(path::SetEnv(kSftpCommandEnvVar, "sftpcmd"));
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst,
                        "--scp-command=C:\\scp.exe foo", NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.sftp_command, "sftpcmd");
}

TEST_F(ParamsTest, ParseSucceedsWithNoSshCommand) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst,
                        "--ssh-command=", NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("ssh-command"));
}

TEST_F(ParamsTest, ParseSucceedsWithNoSftpCommand) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "--sftp-command",
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("sftp-command"));
}

TEST_F(ParamsTest, ParseFailsOnNoUserHost) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kDst, NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("No remote host specified");
}

TEST_F(ParamsTest, ParseDoesNotThinkCIsAHost) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, "C:\\foo", NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("No remote host specified");
}

TEST_F(ParamsTest, ParseWithoutParametersFailsOnMissingSourceAndDestination) {
  const char* argv[] = {"cdc_rsync.exe", NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectOutput("Usage:");
}

TEST_F(ParamsTest, ParseWithSingleParameterFailsOnMissingDestination) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("Missing source or destination");
}

TEST_F(ParamsTest, ParseSucceedsWithMultipleLetterKeyConsumed) {
  const char* argv[] = {"cdc_rsync.exe", "-rvqWRzcn", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_TRUE(parameters_.options.recursive);
  EXPECT_EQ(parameters_.options.verbosity, 1);
  EXPECT_TRUE(parameters_.options.quiet);
  EXPECT_TRUE(parameters_.options.whole_file);
  EXPECT_TRUE(parameters_.options.relative);
  EXPECT_TRUE(parameters_.options.compress);
  EXPECT_TRUE(parameters_.options.checksum);
  EXPECT_TRUE(parameters_.options.dry_run);
  ExpectNoError();
}

TEST_F(ParamsTest,
       ParseFailsOnMultipleLetterKeyConsumedOptionsWithUnsupportedOne) {
  const char* argv[] = {"cdc_rsync.exe", "-rvqaWRzcn", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("Unknown option: 'a'");
}

TEST_F(ParamsTest, ParseSucceedsWithMultipleLongKeyConsumedOptions) {
  const char* argv[] = {"cdc_rsync.exe",
                        "--recursive",
                        "--verbosity",
                        "--quiet",
                        "--whole-file",
                        "--compress",
                        "--relative",
                        "--delete",
                        "--checksum",
                        "--dry-run",
                        "--existing",
                        "--json",
                        kSrc,
                        kUserHostDst,
                        NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_TRUE(parameters_.options.recursive);
  EXPECT_EQ(parameters_.options.verbosity, 1);
  EXPECT_TRUE(parameters_.options.quiet);
  EXPECT_TRUE(parameters_.options.whole_file);
  EXPECT_TRUE(parameters_.options.relative);
  EXPECT_TRUE(parameters_.options.compress);
  EXPECT_TRUE(parameters_.options.delete_);
  EXPECT_TRUE(parameters_.options.checksum);
  EXPECT_TRUE(parameters_.options.dry_run);
  EXPECT_TRUE(parameters_.options.existing);
  EXPECT_TRUE(parameters_.options.json);
  ExpectNoError();
}

TEST_F(ParamsTest, ParseFailsOnUnknownKey) {
  const char* argv[] = {"cdc_rsync.exe", "-unknownKey", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("Unknown option: 'u'");
}

TEST_F(ParamsTest, ParseSucceedsWithSupportedKeyValue) {
  const char* argv[] = {"cdc_rsync.exe",
                        "--compress-level",
                        "11",
                        "--contimeout",
                        "99",
                        "--copy-dest=dest",
                        kSrc,
                        kUserHostDst,
                        NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.compress_level, 11);
  EXPECT_EQ(parameters_.options.connection_timeout_sec, 99);
  EXPECT_EQ(parameters_.options.copy_dest, "dest");
  ExpectNoError();
}

TEST_F(ParamsTest, ParseSucceedsWithSupportedKeyValueWithoutEqualityForChars) {
  const char* argv[] = {"cdc_rsync.exe", "--copy-dest", "dest", kSrc,
                        kUserHostDst,    NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.copy_dest, "dest");
  ExpectNoError();
}

TEST_F(ParamsTest, ParseFailsOnDeleteNeedsRecursive) {
  const char* argv[] = {"cdc_rsync.exe", "--delete", kSrc, kUserHostDst, NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("--delete does not work without --recursive (-r)");
}

TEST_F(ParamsTest, ParseChecksCompressLevel) {
  int minLevel = Options::kMinCompressLevel;
  int maxLevel = Options::kMaxCompressLevel;
  int levels[] = {minLevel - 1, minLevel, 0, maxLevel, maxLevel + 1};
  bool valid[] = {false, true, false, true, false};

  for (int n = 0; n < std::size(levels); ++n) {
    std::string level = "--compress-level=" + std::to_string(levels[n]);
    const char* argv[] = {"cdc_rsync.exe", level.c_str(), kSrc, kUserHostDst,
                          NULL};
    EXPECT_EQ(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_),
              valid[n]);
    if (valid[n]) {
      ExpectNoError();
    } else {
      ExpectError("--compress_level must be between");
    }
    ClearErrors();
  }
}

TEST_F(ParamsTest, ParseFailsOnUnknownKeyValue) {
  const char* argv[] = {"cdc_rsync.exe", "--unknownKey=5", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("unknownKey");
}

TEST_F(ParamsTest, ParseFailsWithHelpOption) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  const char* argv2[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "--help", NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv2)) - 1, argv2, &parameters_));
  ExpectNoError();

  const char* argv3[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "-h", NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv3)) - 1, argv3, &parameters_));
  ExpectNoError();
}

TEST_F(ParamsTest, ParseSucceedsWithIncludeExclude) {
  const char* argv[] = {"cdc_rsync.exe",
                        "--include=*.txt",
                        "--exclude",
                        "*.dat",
                        "--include",
                        "*.exe",
                        kSrc,
                        kUserHostDst,
                        NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  const std::vector<PathFilter::Rule>& rules =
      parameters_.options.filter.GetRules();
  ASSERT_EQ(rules.size(), 3);
  ASSERT_EQ(rules[0].type, PathFilter::Rule::Type::kInclude);
  ASSERT_EQ(rules[0].pattern, "*.txt");
  ASSERT_EQ(rules[1].type, PathFilter::Rule::Type::kExclude);
  ASSERT_EQ(rules[1].pattern, "*.dat");
  ASSERT_EQ(rules[2].type, PathFilter::Rule::Type::kInclude);
  ASSERT_EQ(rules[2].pattern, "*.exe");
  ExpectNoError();
}

TEST_F(ParamsTest, FilesFrom_NoFile) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "--files-from",
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("files-from"));
}

TEST_F(ParamsTest, FilesFrom_ImpliesRelative) {
  const char* argv[] = {"cdc_rsync.exe",       "--files-from",
                        sources_file_.c_str(), base_dir_.c_str(),
                        kUserHostDst,          NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_TRUE(parameters_.options.relative);
  ExpectNoError();
}

TEST_F(ParamsTest, FilesFrom_WithoutSourceArg) {
  const char* argv[] = {"cdc_rsync.exe", "--files-from", sources_file_.c_str(),
                        kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_TRUE(parameters_.options.sources_dir.empty());
  EXPECT_EQ(parameters_.user_host, kUserHost);
  EXPECT_EQ(parameters_.destination, kDst);
  ExpectNoError();
}

TEST_F(ParamsTest, FilesFrom_WithSourceArg) {
  const char* argv[] = {"cdc_rsync.exe",       "--files-from",
                        sources_file_.c_str(), base_dir_.c_str(),
                        kUserHostDst,          NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  std::string expected_sources_dir = base_dir_;
  path::EnsureEndsWithPathSeparator(&expected_sources_dir);
  EXPECT_EQ(parameters_.options.sources_dir, expected_sources_dir);
  EXPECT_EQ(parameters_.user_host, kUserHost);
  EXPECT_EQ(parameters_.destination, kDst);
  ExpectNoError();
}

TEST_F(ParamsTest, FilesFrom_ParsesFile) {
  const char* argv[] = {"cdc_rsync.exe", "--files-from", sources_file_.c_str(),
                        kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  std::vector<const char*> expected = {"file1", "file2", "file3"};
  ASSERT_EQ(parameters_.sources.size(), expected.size());
  for (size_t n = 0; n < expected.size(); ++n) {
    EXPECT_EQ(parameters_.sources[n], expected[n]);
  }
  ExpectNoError();
}

TEST_F(ParamsTest, FilesFrom_EmptyFile_WithoutSourceArg) {
  const char* argv[] = {"cdc_rsync.exe", "--files-from",
                        empty_sources_file_.c_str(), kUserHostDst, NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(empty_sources_file_);
  ExpectError("--files-from option is empty");
}

TEST_F(ParamsTest, FilesFrom_EmptyFile_WithSourceArg) {
  const char* argv[] = {
      "cdc_rsync.exe",   "--files-from", empty_sources_file_.c_str(),
      base_dir_.c_str(), kUserHostDst,   NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(empty_sources_file_);
  ExpectError("--files-from option is empty");
}

TEST_F(ParamsTest, FilesFrom_NoDestination) {
  const char* argv[] = {"cdc_rsync.exe", "--files-from", sources_file_.c_str(),
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("Missing destination");
}

TEST_F(ParamsTest, IncludeFrom_NoFile) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "--include-from",
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("include-from"));
}

TEST_F(ParamsTest, IncludeFrom_ParsesFile) {
  std::string file = path::Join(base_dir_, "include_files.txt");
  const char* argv[] = {"cdc_rsync.exe", "--include-from",
                        file.c_str(),    kSrc,
                        kUserHostDst,    NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  const std::vector<PathFilter::Rule>& rules =
      parameters_.options.filter.GetRules();
  ASSERT_EQ(rules.size(), 1);
  ASSERT_EQ(rules[0].type, PathFilter::Rule::Type::kInclude);
  ASSERT_EQ(rules[0].pattern, "file3");
  ExpectNoError();
}

TEST_F(ParamsTest, ExcludeFrom_NoFile) {
  const char* argv[] = {"cdc_rsync.exe", kSrc, kUserHostDst, "--exclude-from",
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("exclude-from"));
}

TEST_F(ParamsTest, ExcludeFrom_ParsesFile) {
  std::string file = path::Join(base_dir_, "exclude_files.txt");
  const char* argv[] = {"cdc_rsync.exe", "--exclude-from",
                        file.c_str(),    kSrc,
                        kUserHostDst,    NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  const std::vector<PathFilter::Rule>& rules =
      parameters_.options.filter.GetRules();
  ASSERT_EQ(rules.size(), 2);
  EXPECT_EQ(rules[0].type, PathFilter::Rule::Type::kExclude);
  EXPECT_EQ(rules[0].pattern, "file1");
  EXPECT_EQ(rules[1].type, PathFilter::Rule::Type::kExclude);
  EXPECT_EQ(rules[1].pattern, "file2");
  ExpectNoError();
}

TEST_F(ParamsTest, IncludeExcludeMixed_ProperOrder) {
  std::string exclude_file = path::Join(base_dir_, "exclude_files.txt");
  std::string include_file = path::Join(base_dir_, "include_files.txt");
  const char* argv[] = {"cdc_rsync.exe",
                        "--include-from",
                        include_file.c_str(),
                        "--exclude=excl1",
                        kSrc,
                        "--exclude-from",
                        exclude_file.c_str(),
                        kUserHostDst,
                        "--include",
                        "incl1",
                        NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));

  const std::vector<PathFilter::Rule>& rules =
      parameters_.options.filter.GetRules();
  ASSERT_EQ(rules.size(), 5);
  EXPECT_EQ(rules[0].type, PathFilter::Rule::Type::kInclude);
  EXPECT_EQ(rules[0].pattern, "file3");
  EXPECT_EQ(rules[1].type, PathFilter::Rule::Type::kExclude);
  EXPECT_EQ(rules[1].pattern, "excl1");
  EXPECT_EQ(rules[2].type, PathFilter::Rule::Type::kExclude);
  EXPECT_EQ(rules[2].pattern, "file1");
  EXPECT_EQ(rules[3].type, PathFilter::Rule::Type::kExclude);
  EXPECT_EQ(rules[3].pattern, "file2");
  EXPECT_EQ(rules[4].type, PathFilter::Rule::Type::kInclude);
  EXPECT_EQ(rules[4].pattern, "incl1");
  ExpectNoError();
}

TEST_F(ParamsTest, ForwardPort_Single) {
  const char* argv[] = {"cdc_rsync.exe", "--forward-port=65535", kSrc,
                        kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.forward_port_first, 65535);
  EXPECT_EQ(parameters_.options.forward_port_last, 65535);
  ExpectNoError();
}

TEST_F(ParamsTest, ForwardPort_Range) {
  const char* argv[] = {
      "cdc_rsync.exe", "--forward-port", "1-2", kSrc, kUserHostDst, NULL};
  EXPECT_TRUE(Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  EXPECT_EQ(parameters_.options.forward_port_first, 1);
  EXPECT_EQ(parameters_.options.forward_port_last, 2);
  ExpectNoError();
}

TEST_F(ParamsTest, ForwardPort_NoValue) {
  const char* argv[] = {"cdc_rsync.exe", "--forward-port=", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError(NeedsValueError("forward-port"));
}

TEST_F(ParamsTest, ForwardPort_BadValueTooSmall) {
  const char* argv[] = {"cdc_rsync.exe", "--forward-port=0", kSrc, kUserHostDst,
                        NULL};
  EXPECT_FALSE(
      Parse(static_cast<int>(std::size(argv)) - 1, argv, &parameters_));
  ExpectError("Failed to parse");
}

}  // namespace
}  // namespace params
}  // namespace cdc_ft
