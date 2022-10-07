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

#include <algorithm>
#include <cassert>

#include "absl/strings/str_format.h"
#include "common/util.h"
#include "json/json.h"

namespace cdc_ft {

namespace {

// Count signature progress 1/40 because it's probably faster than the rest.
// This assumes a sig speed of 800 MB/sec and a diffing speed of 20 MB/sec,
// but since we don't know the exact numbers, we're estimating them.
constexpr int kSigFactor = 40;

// Fills up |str| with spaces up to a string length of |size|.
void PaddRight(std::string* str, size_t size) {
  if (str->size() < size) {
    str->insert(str->size(), size - str->size(), ' ');
  }
}

// Shortens |filepath| if it is longer than |max_len|.
// TODO: Improve this, e.g. by replacing directories in the middle by "..".
// TODO: Also make sure it plays nicely with UTF-8 code points.
std::string ShortenFilePath(const std::string filepath, size_t max_len) {
  if (filepath.size() > max_len && max_len >= 3) {
    return "..." + filepath.substr(filepath.size() - max_len + 3);
  }
  return filepath;
}

// Formatting might be messed up for >9999 ZB. Please don't sync large files!
const char* kSizeUnits[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"};

// Divides |size| by 1024 as long as it has more than 4 digits and returns the
// corresponding unit, e.g. 10240 -> 10KB.
const char* FormatIntBytes(uint64_t* size) {
  int unitIdx = 0;

  while (*size > 9999 && unitIdx < std::size(kSizeUnits) - 1) {
    ++unitIdx;
    *size /= 1024;
  }

  return kSizeUnits[unitIdx];
}

// Divides |size| by 1024 as long as it has more than 3 digits and returns the
// corresponding unit, e.g. 1500 -> 1.5KB.
const char* FormatDoubleBytes(double* size) {
  int unitIdx = 0;

  while (*size > 999.9 && unitIdx < std::size(kSizeUnits) - 1) {
    ++unitIdx;
    *size /= 1024.0;
  }

  return kSizeUnits[unitIdx];
}

// Formats |sec| seconds into hh::mm:ss if more than one hour or else mm:ss.
std::string FormatTime(double sec) {
  int isec = static_cast<int>(sec);
  int ihour = isec / 3600;
  isec -= ihour * 3600;
  int imin = isec / 60;
  isec -= imin * 60;

  if (ihour > 0) {
    return absl::StrFormat("%02i:%02i:%02i", ihour, imin, isec);
  }

  return absl::StrFormat("%02i:%02i", imin, isec);
}

// Returns curr/total as double number, clamped to [0,1].
double GetProgress(uint64_t curr, uint64_t total) {
  double progress = static_cast<double>(curr) / std::max<uint64_t>(1, total);
  return std::min(std::max(progress, 0.0), 1.0);
}

}  // namespace

ConsoleProgressPrinter::ConsoleProgressPrinter(bool quiet, bool is_tty)
    : ProgressPrinter(quiet, is_tty) {}

void ConsoleProgressPrinter::Print(std::string text, bool newline,
                                   int output_width) {
  if (quiet()) {
    return;
  }

  char linechar = newline || !is_tty() ? '\n' : '\r';
  if (is_tty()) PaddRight(&text, output_width);
  printf("%s%c", text.c_str(), linechar);
  if (!is_tty()) fflush(stdout);
}

ProgressTracker::ProgressTracker(ProgressPrinter* printer, int verbosity,
                                 bool json, int fixed_output_width,
                                 SteadyClock* clock)
    : printer_(printer),
      only_total_progress_(verbosity == 0),
      json_(json),
      fixed_output_width_(fixed_output_width),
      display_delay_sec_(printer_->is_tty() ? 0.1 : 1.0),
      total_timer_(clock),
      file_timer_(clock),
      sig_timer_(clock),
      print_timer_(clock) {}

ProgressTracker::~ProgressTracker() = default;

void ProgressTracker::StartFindFiles() {
  assert(state_ == State::kIdle);
  state_ = State::kSearch;

  files_found_ = 0;
}

void ProgressTracker::ReportFileFound() {
  assert(state_ == State::kSearch);
  ++files_found_;

  UpdateOutput(false);
}

void ProgressTracker::ReportDirFound() {
  assert(state_ == State::kSearch);
  ++dirs_found_;

  UpdateOutput(false);
}

void ProgressTracker::ReportFileStats(
    uint32_t num_missing_files, uint32_t num_extraneous_files,
    uint32_t num_matching_files, uint32_t num_changed_files,
    uint64_t total_missing_bytes, uint64_t total_changed_client_bytes,
    uint64_t total_changed_server_bytes, uint32_t num_missing_dirs,
    uint32_t num_extraneous_dirs, uint32_t num_matching_dirs,
    bool whole_file_arg, bool checksum_arg, bool delete_arg) {
  const char* fmt[] = {
      "%6u file(s) and %u folder(s) are not present on the instance and will "
      "be copied.",
      "%6u file(s) changed and will be updated.",
      "%6u file(s) and %u folder(s) match and do not have to be updated.",
      "%6u file(s) and %u folder(s) on the instance do not exist on this "
      "machine."};

  if (whole_file_arg) {
    fmt[1] = "%6u file(s) changed and will be copied due to -W/--whole-file.";
  }

  if (checksum_arg) {
    fmt[2] =
        "%6u file(s) and %u folder(s) have matching modified time and size, "
        "but will be synced due to -c/--checksum.";
  }

  if (checksum_arg & whole_file_arg) {
    fmt[2] =
        "%6u file(s) and %u folder(s) have matching modified time and size, "
        "but will be copied due to -c/--checksum and -W/--whole-file.";
  }

  if (delete_arg) {
    fmt[3] =
        "%6u file(s) and %u folder(s) on the instance do not exist on this "
        "machine and will be deleted due to --delete.";
  }

  Print(absl::StrFormat(fmt[0], num_missing_files, num_missing_dirs), true);
  Print(absl::StrFormat(fmt[1], num_changed_files), true);
  Print(absl::StrFormat(fmt[2], num_matching_files, num_matching_dirs), true);
  Print(absl::StrFormat(fmt[3], num_extraneous_files, num_extraneous_dirs),
        true);

  total_bytes_to_copy_ = total_missing_bytes;
  total_bytes_to_diff_ = total_changed_client_bytes;
  total_sig_bytes_ = total_changed_server_bytes;
  total_files_to_delete_ = num_extraneous_files;
  total_dirs_to_delete_ = num_extraneous_dirs;
}

void ProgressTracker::StartCopy(const std::string& filepath,
                                uint64_t filesize) {
  assert(state_ == State::kIdle);
  state_ = State::kCopy;
  file_timer_.Reset();
  sig_time_sec_ = 0;

  curr_filepath_ = filepath;
  curr_filesize_ = filesize;
  curr_bytes_copied_ = 0;
}

void ProgressTracker::ReportCopyProgress(uint64_t num_bytes_copied) {
  assert(state_ == State::kCopy);
  curr_bytes_copied_ += num_bytes_copied;
  total_bytes_copied_ += num_bytes_copied;

  UpdateOutput(false);
}

void ProgressTracker::StartSync(const std::string& filepath,
                                uint64_t client_size, uint64_t server_size) {
  assert(state_ == State::kIdle);
  state_ = State::kSyncDiff;
  file_timer_.Reset();
  sig_timer_.Reset();
  sig_time_sec_ = 0;

  curr_filepath_ = filepath;
  curr_filesize_ = client_size;
  server_filesize_ = server_size;
  curr_sig_bytes_read_ = 0;
  curr_bytes_diffed_ = 0;
}

void ProgressTracker::ReportSyncProgress(size_t num_client_bytes_processed,
                                         size_t num_server_bytes_processed) {
  assert(state_ == State::kSyncSig || state_ == State::kSyncDiff);

  // If diffing is blocked on getting more server chunks, switch to kSyncSig,
  // which effectively changes the output from "Dxxx%" to "Sxxx%" and removes
  // the ETA.
  State new_state = state_;
  if (num_client_bytes_processed > 0) {
    new_state = State::kSyncDiff;
  } else if (num_server_bytes_processed > 0) {
    new_state = State::kSyncSig;
  }

  // Measure time exclusively spent in server-side signature computation. This
  // is later taken into account to get a better speed estimation for diffing.
  if (state_ == State::kSyncDiff && new_state == State::kSyncSig) {
    sig_timer_.Reset();
  } else if (state_ == State::kSyncSig && new_state == State::kSyncDiff) {
    sig_time_sec_ += sig_timer_.ElapsedSeconds();
  }
  state_ = new_state;

  curr_bytes_diffed_ += num_client_bytes_processed;
  total_bytes_diffed_ += num_client_bytes_processed;
  curr_sig_bytes_read_ += num_server_bytes_processed;
  total_sig_bytes_read_ += num_server_bytes_processed;

  UpdateOutput(false);
}

void ProgressTracker::StartDeleteFiles() {
  assert(state_ == State::kIdle);
  state_ = State::kDelete;

  files_deleted_ = 0;
}

void ProgressTracker::ReportFileDeleted(const std::string& filepath) {
  curr_filepath_ = filepath;
  ++files_deleted_;

  UpdateOutput(false);
}

void ProgressTracker::ReportDirDeleted(const std::string& filepath) {
  curr_filepath_ = filepath;
  ++dirs_deleted_;

  UpdateOutput(false);
}

void ProgressTracker::Finish() {
  assert(state_ != State::kIdle);

  UpdateOutput(true);
  if (state_ == State::kSearch) {
    // Total time does not count file search time.
    total_timer_.Reset();
  }

  state_ = State::kIdle;
}

void ProgressTracker::UpdateOutput(bool finished) {
  if (printer_->quiet()) {
    return;
  }

  if (state_ == State::kDelete) {
    if (total_files_to_delete_ + total_dirs_to_delete_ == 0 ||
        (!only_total_progress_ && finished)) {
      // No need to print here, it would result in "0/0" or duplicate lines.
      return;
    }

    if (only_total_progress_) {
      if (!finished && print_timer_.ElapsedSeconds() < display_delay_sec_) {
        return;
      }
      print_timer_.Reset();

      Print(absl::StrFormat("%u/%u file(s) and %u/%u folder(s) deleted.",
                            files_deleted_, total_files_to_delete_,
                            dirs_deleted_, total_dirs_to_delete_),
            finished);
      return;
    }

    std::string txt =
        absl::StrFormat("deleted %u / %u", files_deleted_ + dirs_deleted_,
                        total_files_to_delete_ + total_dirs_to_delete_);

    int width = GetOutputWidth();
    int file_width = std::max(12, width - static_cast<int>(txt.size()));
    std::string short_path = ShortenFilePath(curr_filepath_, file_width);
    PaddRight(&short_path, file_width);
    printer_->Print(short_path + txt, true, width);
    return;
  }

  if (only_total_progress_ && state_ != State::kSearch) {
    finished &= GetTotalProgress() == 1;
    if (!finished && print_timer_.ElapsedSeconds() < display_delay_sec_) {
      return;
    }
    print_timer_.Reset();

    if (json_) {
      double total_progress, total_sec, total_eta_sec;
      GetTotalProgressStats(&total_progress, &total_sec, &total_eta_sec);

      Json::Value val;
      val["total_progress"] = total_progress;
      val["total_duration"] = total_sec;
      val["total_eta"] = total_eta_sec;
      PrintJson(val, finished);
    } else {
      Print(GetTotalProgressText(), finished);
    }
    return;
  }

  // Always print if finished (to make sure to get the line feed).
  if (!finished && print_timer_.ElapsedSeconds() < display_delay_sec_) {
    return;
  }
  print_timer_.Reset();

  switch (state_) {
    case State::kSearch: {
      Print(absl::StrFormat("%u file(s) and %u folder(s) found", files_found_,
                            dirs_found_),
            finished);
      break;
    }

    case State::kCopy: {
      // file               C 50% 12345MB 123.4MB/s 00:10 ETA  50% TOT 05:00 ETA
      double progress = GetProgress(curr_bytes_copied_, curr_filesize_);
      OutputFileProgress(OutputType::kCopy, progress, finished);
      break;
    }

    case State::kSyncSig: {
      // file               S 50% 12345MB ---.-MB/s 00:10 ETA  50% TOT 05:00 ETA
      double progress =
          GetProgress(curr_bytes_diffed_ + curr_sig_bytes_read_ / kSigFactor,
                      curr_filesize_ + server_filesize_ / kSigFactor);
      OutputFileProgress(OutputType::kSig, progress, finished);
      break;
    }

    case State::kSyncDiff: {
      // file               D 50% 12345MB 123.4MB/s 00:10 ETA  50% TOT 05:00 ETA
      double progress =
          GetProgress(curr_bytes_diffed_ + curr_sig_bytes_read_ / kSigFactor,
                      curr_filesize_ + server_filesize_ / kSigFactor);
      OutputFileProgress(OutputType::kDiff, progress, finished);
      break;
    }

    case State::kDelete:
      // Should have been handled above.
      assert(false);
    case State::kIdle:
      break;
  }
}

void ProgressTracker::OutputFileProgress(OutputType type, double progress,
                                         bool finished) const {
  // Just in case some calculation wasn't 100% right.
  if (finished) {
    progress = 1.0;
  }

  double file_sec = file_timer_.ElapsedSeconds();
  double file_eta_sec = file_sec / std::max(1e-3, progress) - file_sec;
  double filespeed = 0;

  // Don't bother trying to estimate transfer speed for signature.
  if (type == OutputType::kCopy) {
    filespeed = curr_filesize_ * progress / std::max(1e-3, file_sec);
  } else if (type == OutputType::kDiff) {
    // Take the sig time into account to estimate the effective transfer speed,
    // using the actual diffing progress, not the combined diffing + signing
    // progress.
    double diff_progress = GetProgress(curr_bytes_diffed_, curr_filesize_);
    double diff_sec = file_sec - sig_time_sec_;
    double final_file_sec =
        diff_sec / std::max(1e-3, diff_progress) + sig_time_sec_;
    filespeed = curr_filesize_ / std::max(1e-3, final_file_sec);
  }

  if (json_) {
    const char* op = type == OutputType::kCopy  ? "Copy"
                     : type == OutputType::kSig ? "Sign"
                                                : "Diff";

    double total_progress, total_sec, total_eta_sec;
    GetTotalProgressStats(&total_progress, &total_sec, &total_eta_sec);

    Json::Value val;
    val["file"] = curr_filepath_;
    val["operation"] = op;
    val["size"] = curr_filesize_;
    val["bytes_per_second"] = filespeed;
    val["duration"] = file_sec;
    val["eta"] = file_eta_sec;
    val["total_progress"] = total_progress;
    val["total_duration"] = total_sec;
    val["total_eta"] = total_eta_sec;
    PrintJson(val, finished);
  } else {
    char ch = type == OutputType::kCopy  ? 'C'
              : type == OutputType::kSig ? 'S'
                                         : 'D';

    int progress_percent = static_cast<int>(progress * 100);

    uint64_t filesize = curr_filesize_;
    const char* filesize_unit = FormatIntBytes(&filesize);
    std::string filetime_str = FormatTime(file_sec);

    const char* filespeed_unit = "B";
    std::string filespeed_str = "---.-";

    // Don't bother trying to estimate transfer speed for signature.
    if (type != OutputType::kSig) {
      filespeed_unit = FormatDoubleBytes(&filespeed);
      filespeed_str = absl::StrFormat("%5.1f", filespeed);
    }

    const char* fileeta_str = "   ";
    if (progress < 1.0) {
      // While in progress, time is an ETA.
      filetime_str = FormatTime(file_eta_sec);
      fileeta_str = "ETA";
    }

    // file                S 50% 12345MB ---.-MB/s --:-- ETA  50% TOT 005:00 ETA
    std::string txt = absl::StrFormat(
        " %c%3i%% %5i%-2s "
        "%s%-2s/s %s %3s "
        "%s",
        ch, progress_percent, filesize, filesize_unit, filespeed_str.c_str(),
        filespeed_unit, filetime_str.c_str(), fileeta_str,
        GetTotalProgressText().c_str());

    int width = GetOutputWidth();
    int file_width = std::max(12, width - static_cast<int>(txt.size()));
    std::string short_path = ShortenFilePath(curr_filepath_, file_width);
    PaddRight(&short_path, file_width);

    printer_->Print(short_path + txt, finished, width);
  }
}

void ProgressTracker::Print(std::string text, bool finished) const {
  if (printer_->quiet()) return;

  printer_->Print(std::move(text), finished, GetOutputWidth());
}

void ProgressTracker::PrintJson(const Json::Value& val, bool finished) const {
  if (printer_->quiet()) return;

  Json::FastWriter writer;
  std::string json = writer.write(val);
  if (!json.empty() && json.back() == '\n') json.pop_back();
  printer_->Print(json, finished, 0);
}

double ProgressTracker::GetTotalProgress() const {
  return GetProgress(total_bytes_copied_ + total_bytes_diffed_ +
                         total_sig_bytes_read_ / kSigFactor,
                     total_bytes_to_copy_ + total_bytes_to_diff_ +
                         total_sig_bytes_ / kSigFactor);
}

void ProgressTracker::GetTotalProgressStats(double* total_progress,
                                            double* total_sec,
                                            double* total_eta_sec) const {
  *total_progress = GetTotalProgress();
  *total_sec = total_timer_.ElapsedSeconds();
  *total_eta_sec = *total_sec / std::max(1e-3, *total_progress) - *total_sec;
}

std::string ProgressTracker::GetTotalProgressText() const {
  double total_progress, total_sec, total_eta_sec;
  GetTotalProgressStats(&total_progress, &total_sec, &total_eta_sec);
  int total_progress_percent = static_cast<int>(total_progress * 100);

  const char* total_eta_str = "   ";
  if (total_progress < 1.0) {
    // total_sec is an ETA.
    total_sec = total_eta_sec;
    total_eta_str = "ETA";
  }
  std::string total_sec_str = FormatTime(total_sec);

  return absl::StrFormat("%3i%% TOT %s %s", total_progress_percent,
                         total_sec_str.c_str(), total_eta_str);
}

int ProgressTracker::GetOutputWidth() const {
  return fixed_output_width_ > 0 ? fixed_output_width_
                                 : Util::GetConsoleWidth();
}

}  // namespace cdc_ft
