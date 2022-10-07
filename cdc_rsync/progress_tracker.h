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

#ifndef CDC_RSYNC_PROGRESS_TRACKER_H_
#define CDC_RSYNC_PROGRESS_TRACKER_H_

#include <string>

#include "cdc_rsync/base/cdc_interface.h"
#include "cdc_rsync/file_finder_and_sender.h"
#include "common/stopwatch.h"

namespace Json {
class Value;
}

namespace cdc_ft {

class ProgressPrinter {
 public:
  ProgressPrinter(bool quiet, bool is_tty) : quiet_(quiet), is_tty_(is_tty) {}
  virtual ~ProgressPrinter() = default;

  virtual void Print(std::string text, bool newline, int output_width) = 0;

  bool quiet() const { return quiet_; }
  bool is_tty() const { return is_tty_; }

 private:
  const bool quiet_;
  const bool is_tty_;
};

class ConsoleProgressPrinter : public ProgressPrinter {
 public:
  ConsoleProgressPrinter(bool quiet, bool is_tty);

  // Prints |text| to stdout. Adds a line feed (\n) if |newline| is true or
  // is_tty() is false (e.g. logging to a file). Otherwise, just adds a carriage
  // return (\r), so that the next call to Output overwrites the current line.
  // Fills the rest of the line up to |output_width| characters with spaces to
  // properly overwrite the last line.
  // No-op if quiet().
  void Print(std::string text, bool newline, int output_width) override;
};

// Tracks progress of the various stages of rsync and displays them in a human-
// readable manner.
class ProgressTracker : public ReportCdcProgress,
                        public ReportFindFilesProgress {
 public:
  // |verbosity| (number of -v arguments) impacts the display verbosity.
  // 0 only prints total progress and ETA/time.
  // 1 prints per-file process and ETA/time.
  // |json| prints JSON progress.
  // If |fixed_output_width| > 0, formats output to that width, otherwise, uses
  // the console width.
  ProgressTracker(ProgressPrinter* printer, int verbosity, bool json,
                  int fixed_output_width = 0,
                  SteadyClock* clock = DefaultSteadyClock::GetInstance());
  ~ProgressTracker();

  // Starts reporting finding all source files to copy.
  // Must be in idle state. Must be called before ReportFileFound().
  void StartFindFiles();

  // Reports that a file has been found.
  void ReportFileFound();

  // Reports that a directory has been found.
  void ReportDirFound();

  // Prints out the 4 files numbers and stores the total bytes for progress
  // calculations. See SendFileStatsResponse in messages.proto for more info.
  void ReportFileStats(uint32_t num_missing_files,
                       uint32_t num_extraneous_files,
                       uint32_t num_matching_files, uint32_t num_changed_files,
                       uint64_t total_missing_bytes,
                       uint64_t total_changed_client_bytes,
                       uint64_t total_changed_server_bytes,
                       uint32_t num_missing_dirs, uint32_t num_extraneous_dirs,
                       uint32_t num_matching_dirs, bool whole_file_arg = false,
                       bool checksum_arg = false, bool delete_arg = false);

  // Starts reporting the copy of the file at |filepath| of size |filesize|.
  // Must be in idle state. Must be called before ReportCopyProgress().
  void StartCopy(const std::string& filepath, uint64_t filesize);

  // Reports that |num_bytes_copied| have been copied for the current file.
  void ReportCopyProgress(uint64_t num_bytes_copied);

  // Starts reporting the delta sync of the file at |filepath| of size
  // |client_size|. |server_size| is the size of the corresponding file on the
  // server.
  // Must be in idle state. Must be called before ReportSyncProgress().
  void StartSync(const std::string& filepath, uint64_t client_size,
                 uint64_t server_size);

  // ReportCdcProgress:

  // Reports that |num_client_bytes_processed| of the current client-side file
  // have been read and processed by the delta-transfer algorithm, and that
  // |num_server_bytes_processed| of the current server-side file have been
  // read and processed.
  void ReportSyncProgress(uint64_t num_client_bytes_processed,
                          uint64_t num_server_bytes_processed) override;

  // Starts reporting deletion of extraneous files.
  // Must be in idle state. Must be called before ReportFileDeleted().
  void StartDeleteFiles();

  // Reports that a file has been deleted.
  void ReportFileDeleted(const std::string& filepath);

  // Reports that a directory has been deleted.
  void ReportDirDeleted(const std::string& filepath);

  // Prints final stats (e.g. 100% progress for copy/diff), feeds line and
  // resets state to idle. Must be called
  // - when all files have been found,
  // - after each file copy and
  // - after each diff.
  void Finish();

  // Gets the time delay (in seconds) between two display updates. Returns a
  // lower number (more updates) in TTY mode, e.g. when running from a terminal,
  // and a higher number otherwise, e.g. when piping stdout to a file.
  double GetDisplayDelaySecForTesting() const { return display_delay_sec_; }

 private:
  // Prints progress to the console. Rate-limited, so that it can be called
  // often without performance overhead. |finished| should be set if the current
  // item is finished. It adds a line feed.
  // No-op if printer_->quiet().
  void UpdateOutput(bool finished);

  enum class OutputType { kCopy, kSig, kDiff };

  // Prints out the progress of the current file (for copy/diff state).
  // Used if |verbosity_| > 0 and not printer_->quiet().
  void OutputFileProgress(OutputType type, double progress,
                          bool finished) const;

  // Wrapper for printer_->Print(), but checks printer_->quiet().
  void Print(std::string text, bool finished) const;

  // Prints the JSON value |val|.
  void PrintJson(const Json::Value& val, bool finished) const;

  // Returns the total progress (between 0 and 1).
  double GetTotalProgress() const;

  // Gets the |total_progress| (between 0 and 1), the total duration so far in
  // |total_sec| and the estimated ETA (time left) in |total_eta_sec|.
  void GetTotalProgressStats(double* total_progress, double* total_sec,
                             double* total_eta_sec) const;

  // Returns a string with the total progress, e.g. " 19% TOT 03:59 ETA".
  std::string GetTotalProgressText() const;

  // Returns |fixed_output_width_| if > 0 or Util::GetConsoleWidth().
  int GetOutputWidth() const;

  ProgressPrinter* const printer_;
  const bool only_total_progress_ = false;
  const bool json_ = false;
  const int fixed_output_width_;
  const double display_delay_sec_;

  // Timer for total copy and diff time (not file search, that's not timed).
  Stopwatch total_timer_;
  // Timer for single file copy and diff time.
  Stopwatch file_timer_;
  // Timer for processing signatures of server files.
  Stopwatch sig_timer_;
  // Timer to limit the rate of console outputs.
  Stopwatch print_timer_;

  enum class State { kIdle, kSearch, kCopy, kSyncSig, kSyncDiff, kDelete };

  State state_ = State::kIdle;

  // Number of files found so far.
  uint32_t files_found_ = 0;

  // Number of directories found so far.
  uint32_t dirs_found_ = 0;

  // Path of the file currently copied or synced.
  std::string curr_filepath_;
  // Size of the file at |current_filepath_|.
  uint64_t curr_filesize_ = 0;

  // Number of bytes of the file at |current_filepath_| already copied.
  uint64_t curr_bytes_copied_ = 0;
  // Total number of bytes already copied.
  uint64_t total_bytes_copied_ = 0;
  // Total number of bytes to copy.
  uint64_t total_bytes_to_copy_ = 0;

  // Number of signature bytes read so far on the server.
  uint64_t curr_sig_bytes_read_ = 0;
  // Total number of signature bytes already processed on the server.
  uint64_t total_sig_bytes_read_ = 0;
  // Total number of signature bytes.
  uint64_t total_sig_bytes_ = 0;
  // Total size of the server-side file.
  uint64_t server_filesize_ = 0;
  // Duration for signature computation.
  double sig_time_sec_ = 0.0;

  // Number of bytes of the file at |current_filepath_| already diffed.
  uint64_t curr_bytes_diffed_ = 0;
  // Total number of bytes already diffed.
  uint64_t total_bytes_diffed_ = 0;
  // Total number of bytes to diff.
  uint64_t total_bytes_to_diff_ = 0;

  // Number of files deleted so far.
  uint32_t files_deleted_ = 0;
  // Total number of files to be deleted.
  uint32_t total_files_to_delete_ = 0;
  // Number of directories deleted so far.
  uint32_t dirs_deleted_ = 0;
  // Total number of directories to be deleted.
  uint32_t total_dirs_to_delete_ = 0;
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_PROGRESS_TRACKER_H_
