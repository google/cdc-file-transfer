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

#include "cdc_rsync/progress_tracker.h"

#include <ostream>

#include "common/testing_clock.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Create custom sizes, so that progress can be easily split up into steps, e.g.
// kFileSize / 3 + kFileSize / 3,  + kFileSize / 3, and the end result is still
// kFileSize.
constexpr uint64_t kFileSize = 1 * 2 * 3 * 4 * 5 * 6;

// Verbosity.
const int kV0 = 0;
const int kV1 = 1;

const bool kJson = true;
const bool kNoJson = false;

const bool kQuiet = true;
const bool kNoQuiet = false;

const bool kTTY = true;
const bool kNoTTY = false;

// Class that just creates a list of things it was supposed to print.
class FakeProgressPrinter : public ProgressPrinter {
 public:
  FakeProgressPrinter(bool quiet, bool is_tty)
      : ProgressPrinter(quiet, is_tty) {}

  void Print(std::string text, bool newline, int /*output_width*/) override {
    lines_.push_back(text + (newline || !is_tty() ? "\n" : "\r"));
  }

  void ExpectLinesMatch(std::vector<std::string> expected_lines) {
    EXPECT_EQ(lines_, expected_lines);
  }

 private:
  std::vector<std::string> lines_;
};

class ProgressTrackerTest : public ::testing::Test {
 protected:
  // Returns the time (in ms) that needs to pass to trigger an output update.
  double GetTriggerPrintTimeDeltaMs(const ProgressTracker& progress) {
    return progress.GetDisplayDelaySecForTesting() * 1000 * 1.5;
  }

  TestingSteadyClock clock_;

  uint32_t two_seconds_time_delta_ms_ = 2 * 1000;
  uint32_t two_minutes_time_delta_ms_ = 2 * 60 * 1000;
  uint32_t four_hours_time_delta_ms_ = 4 * 60 * 60 * 1000;
  uint32_t eight_days_time_delta_ms_ = 8 * 24 * 60 * 60 * 1000;

  const int output_width_ = 66;
};

TEST_F(ProgressTrackerTest, FindFiles) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  progress.StartFindFiles();
  progress.ReportFileFound();
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportFileFound();
  progress.ReportFileFound();
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportFileFound();
  progress.ReportFileFound();
  progress.Finish();

  printer.ExpectLinesMatch({"2 file(s) and 0 folder(s) found\r",
                            "4 file(s) and 0 folder(s) found\r",
                            "5 file(s) and 0 folder(s) found\n"});
}

TEST_F(ProgressTrackerTest, FindFilesVerbose) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  progress.StartFindFiles();
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportFileFound();
  progress.Finish();

  // Find files should be the same as non-verbose.
  printer.ExpectLinesMatch({"1 file(s) and 0 folder(s) found\r",
                            "1 file(s) and 0 folder(s) found\n"});
}

TEST_F(ProgressTrackerTest, CopyFiles) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  progress.StartCopy("file.txt", kFileSize);
  progress.ReportCopyProgress(kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  progress.ReportCopyProgress(kFileSize / 3);
  progress.Finish();

  printer.ExpectLinesMatch({"100% TOT 00:00    \r", "100% TOT 00:00    \n"});
}

TEST_F(ProgressTrackerTest, CopyFilesVerbose) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  progress.StartCopy("file.txt", kFileSize);
  progress.ReportCopyProgress(kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  progress.ReportCopyProgress(kFileSize / 3);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"file.txt      C 66%   720B    3.1KB/s 00:00 ETA 100% TOT 00:00    \r",
       "file.txt      C100%   720B    4.7KB/s 00:00     100% TOT 00:00    \n"});
}

TEST_F(ProgressTrackerTest, SyncFiles) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 1 changed file.
  progress.ReportFileStats(0, 0, 0, 1, 0, kFileSize, kFileSize, 0, 0, 0);

  progress.StartSync("file.txt", kFileSize, kFileSize);
  progress.ReportSyncProgress(0, kFileSize / 2);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(0, kFileSize / 2);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(kFileSize / 3, 0);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(kFileSize / 3, 0);
  progress.ReportSyncProgress(kFileSize / 3, 0);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     1 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "  2% TOT 00:05 ETA\r", " 34% TOT 00:00 ETA\r", " 67% TOT 00:00 ETA\r",
       "100% TOT 00:00    \n"});
}

TEST_F(ProgressTrackerTest, SyncFilesVerbose) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  // 1 changed file.
  progress.ReportFileStats(0, 0, 0, 1, 0, kFileSize, kFileSize, 0, 0, 0);

  progress.StartSync("file.txt", kFileSize, kFileSize);
  clock_.Advance(two_seconds_time_delta_ms_);
  progress.ReportSyncProgress(0, kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(kFileSize / 3, 0);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(0, kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(0, kFileSize / 3);
  progress.ReportSyncProgress(kFileSize / 3, 0);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportSyncProgress(kFileSize / 3, 0);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     1 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "file.txt      S  0%   720B  ---.-B /s 04:03 ETA   0% TOT 04:03 ETA\r",
       "file.txt      D 33%   720B  117.1B /s 00:04 ETA  33% TOT 00:04 ETA\r",
       "file.txt      S 34%   720B  ---.-B /s 00:04 ETA  34% TOT 00:04 ETA\r",
       "file.txt      S 34%   720B  ---.-B /s 00:04 ETA  34% TOT 00:04 ETA\r",
       "file.txt      D100%   720B  276.9B /s 00:02     100% TOT 00:02    \r",
       "file.txt      D100%   720B  276.9B /s 00:02     100% TOT 00:02    \n"});
}

TEST_F(ProgressTrackerTest, DeleteFiles) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 2 extraneous files.
  progress.ReportFileStats(0, 4, 0, 0, 0, 0, 0, 0, 0, 0);
  progress.StartDeleteFiles();
  progress.ReportFileDeleted("file1.txt");
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportFileDeleted("file2.txt");
  progress.ReportFileDeleted("file3.txt");
  progress.ReportFileDeleted("file4.txt");
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     4 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "2/4 file(s) and 0/0 folder(s) deleted.\r",
       "4/4 file(s) and 0/0 folder(s) deleted.\n"});
}

TEST_F(ProgressTrackerTest, DeleteFilesVerbose) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  // 2 extraneous files.
  progress.ReportFileStats(0, 2, 0, 0, 0, 0, 0, 0, 0, 0);
  progress.StartDeleteFiles();
  progress.ReportFileDeleted("file1.txt");
  progress.ReportFileDeleted("file2.txt");
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     2 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "file1.txt                                            deleted 1 / 2\n",
       "file2.txt                                            deleted 2 / 2\n"});
}

TEST_F(ProgressTrackerTest, DeleteFilesNoFiles) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  progress.ReportFileStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  progress.StartDeleteFiles();
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n"});
}

TEST_F(ProgressTrackerTest, SetFileStatsAndUnits) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  // Have a very large file and trigger different ETAs and transfer speeds.
  constexpr uint64_t large_file_size = 2ull * 1024 * 1024 * 1024 * 1024;
  progress.ReportFileStats(1, 0, 0, 0, large_file_size, 0, 0, 0, 0, 0);

  progress.StartCopy("file.txt", large_file_size);
  progress.ReportCopyProgress(large_file_size / 4);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(large_file_size / 4);
  clock_.Advance(two_seconds_time_delta_ms_);
  progress.ReportCopyProgress(large_file_size / 4);
  clock_.Advance(two_minutes_time_delta_ms_);
  progress.ReportCopyProgress(large_file_size / 8);
  clock_.Advance(four_hours_time_delta_ms_);
  progress.ReportCopyProgress(large_file_size / 8);
  clock_.Advance(eight_days_time_delta_ms_);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     1 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "file.txt      C 50%  2048GB   6.7TB/s 00:00 ETA  50% TOT 00:00 ETA\r",
       "file.txt      C 75%  2048GB 714.4GB/s 00:00 ETA  75% TOT 00:00 ETA\r",
       "file.txt      C 87%  2048GB  14.7GB/s 00:17 ETA  87% TOT 00:17 ETA\r",
       "file.txt     C100%  2048GB 144.4MB/s 04:02:02     100% TOT 04:02:02    "
       "\r",
       "file.txt     C100%  2048GB   3.0MB/s 196:02:02     100% TOT 196:02:02  "
       "  \n"});
}

TEST_F(ProgressTrackerTest, QuietMode) {
  FakeProgressPrinter printer(kQuiet, kTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);

  progress.StartFindFiles();
  progress.ReportFileFound();
  progress.Finish();

  // 1 missing, 1 extraneous, and 1 changed files
  // 1 extraneous folder
  progress.ReportFileStats(1, 1, 1, 0, kFileSize, kFileSize, kFileSize, 0, 0,
                           1);

  progress.StartCopy("file.txt", kFileSize);
  progress.ReportCopyProgress(kFileSize);
  progress.Finish();

  progress.StartSync("file.txt", kFileSize, kFileSize);
  progress.ReportSyncProgress(kFileSize, kFileSize);
  progress.Finish();

  progress.StartDeleteFiles();
  progress.ReportFileDeleted("file.txt");
  progress.ReportDirDeleted("folder");
  progress.Finish();

  printer.ExpectLinesMatch({});
}

TEST_F(ProgressTrackerTest, NoTTY) {
  FakeProgressPrinter tty_printer(kNoQuiet, kTTY);
  ProgressTracker tty_progress(&tty_printer, kV1, kNoJson, output_width_,
                               &clock_);
  double tty_delta_ms = GetTriggerPrintTimeDeltaMs(tty_progress);

  // In no-TTY-mode (e.g. cdc_rsync .. > out.txt), the display rate should be
  // lower (currently every 1 second instead of 0.1 seconds).
  FakeProgressPrinter printer(kNoQuiet, kNoTTY);
  ProgressTracker progress(&printer, kV1, kNoJson, output_width_, &clock_);
  EXPECT_GT(GetTriggerPrintTimeDeltaMs(progress), tty_delta_ms);

  progress.StartCopy("file.txt", kFileSize);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  progress.ReportCopyProgress(kFileSize / 3);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"file.txt      C 33%   720B  160.0B /s 00:03 ETA 100% TOT 00:01    \n",
       "file.txt      C 66%   720B  160.0B /s 00:01 ETA 100% TOT 00:03    \n",
       "file.txt      C100%   720B  240.0B /s 00:03     100% TOT 00:03    \n"});
}

TEST_F(ProgressTrackerTest, JsonPerFile) {
  FakeProgressPrinter printer(kNoQuiet, kNoTTY);
  ProgressTracker progress(&printer, kV1, kJson, output_width_, &clock_);

  progress.StartCopy("file.txt", kFileSize);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  progress.ReportCopyProgress(kFileSize / 3);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"{\"bytes_per_second\":160.0,\"duration\":1.5,\"eta\":3.0,\"file\":"
       "\"file.txt\",\"operation\":\"Copy\",\"size\":720,\"total_duration\":1."
       "5,\"total_eta\":0.0,\"total_progress\":1.0}\n",
       "{\"bytes_per_second\":160.0,\"duration\":3.0,\"eta\":1.5,\"file\":"
       "\"file.txt\",\"operation\":\"Copy\",\"size\":720,\"total_duration\":3."
       "0,\"total_eta\":0.0,\"total_progress\":1.0}\n",
       "{\"bytes_per_second\":240.0,\"duration\":3.0,\"eta\":0.0,\"file\":"
       "\"file.txt\",\"operation\":\"Copy\",\"size\":720,\"total_duration\":3."
       "0,\"total_eta\":0.0,\"total_progress\":1.0}\n"});
}

TEST_F(ProgressTrackerTest, JsonTotal) {
  FakeProgressPrinter printer(kNoQuiet, kNoTTY);
  ProgressTracker progress(&printer, kV0, kJson, output_width_, &clock_);

  progress.ReportFileStats(0, 0, 0, 1, 0, kFileSize, kFileSize, 0, 0, 0);

  progress.StartCopy("file.txt", kFileSize);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  clock_.Advance(GetTriggerPrintTimeDeltaMs(progress));
  progress.ReportCopyProgress(kFileSize / 3);
  progress.ReportCopyProgress(kFileSize / 3);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     1 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n",
       "{\"total_duration\":1.5,\"total_eta\":3.1124999999999998,\"total_"
       "progress\":0.32520325203252032}\n",
       "{\"total_duration\":3.0,\"total_eta\":1.6124999999999998,\"total_"
       "progress\":0.65040650406504064}\n"});
}

TEST_F(ProgressTrackerTest, SyncFilesWithWholeFile) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 1 changed file with -W arg.
  progress.ReportFileStats(0, 0, 0, 1, 0, kFileSize, kFileSize, 0, 0, 0, true);
  progress.StartCopy("file.txt", kFileSize);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     1 file(s) changed and will be copied due to -W/--whole-file.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n"});
}

TEST_F(ProgressTrackerTest, SyncFilesWithChecksum) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 1 matching file with -c arg.
  progress.ReportFileStats(0, 0, 1, 0, 0, kFileSize, kFileSize, 0, 0, 0, false,
                           true);
  progress.StartCopy("file.txt", kFileSize);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     1 file(s) and 0 folder(s) have matching modified time and size, "
       "but will be synced due to -c/--checksum.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n"});
}

TEST_F(ProgressTrackerTest, SyncFilesWithChecksumAndWholeFile) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 1 changed file, 1 matching file. with -c and -W args.
  progress.ReportFileStats(0, 0, 1, 1, 0, kFileSize, kFileSize, 0, 0, 0, true,
                           true);
  progress.StartCopy("file.txt", kFileSize);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     1 file(s) changed and will be copied due to -W/--whole-file.\n",
       "     1 file(s) and 0 folder(s) have matching modified time and size, "
       "but will be copied due to -c/--checksum and -W/--whole-file.\n",
       "     0 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine.\n"});
}

TEST_F(ProgressTrackerTest, SyncFilesWithDelete) {
  FakeProgressPrinter printer(kNoQuiet, kTTY);
  ProgressTracker progress(&printer, kV0, kNoJson, output_width_, &clock_);

  // 1 extraneous file with --delete arg.
  progress.ReportFileStats(0, 1, 0, 0, 0, kFileSize, kFileSize, 0, 0, 0, false,
                           false, true);
  progress.StartCopy("file.txt", kFileSize);
  progress.Finish();

  printer.ExpectLinesMatch(
      {"     0 file(s) and 0 folder(s) are not present on the instance and "
       "will be copied.\n",
       "     0 file(s) changed and will be updated.\n",
       "     0 file(s) and 0 folder(s) match and do not have to be updated.\n",
       "     1 file(s) and 0 folder(s) on the instance do not exist on this "
       "machine and will be deleted due to --delete.\n"});
}

}  // namespace
}  // namespace cdc_ft
