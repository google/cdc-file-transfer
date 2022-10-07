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

#include "manifest/stats_printer.h"

#include "absl/strings/str_format.h"
#include "common/path.h"
#include "common/util.h"

namespace cdc_ft {
namespace {

// See https://ss64.com/nt/syntax-ansi.html.
enum class AnsiCode {
  // Foreground colors
  kBlackFg = 0,
  kDarkRedFg = 1,
  kDarkGreenFg = 2,
  kDarkYellowFg = 3,
  kDarkBlueFg = 4,
  kDarkMagentaFg = 5,
  kDarkCyanFg = 6,
  kLightGrayFg = 7,
  kDarkGrayFg = 8,
  kLightRedFg = 9,
  kLightGreenFg = 10,
  kLightYellowFg = 11,
  kLightBlueFg = 12,
  kLightMagentaFg = 13,
  kLightCyanFg = 14,
  kWhiteFg = 15,

  // Background colors
  kBlackBg = 16,
  kDarkRedBg = 17,
  kDarkGreenBg = 18,
  kDarkYellowBg = 19,
  kDarkBlueBg = 20,
  kDarkMagentaBg = 21,
  kDarkCyanBg = 22,
  kLightGrayBg = 23,
  kDarkGrayBg = 24,
  kLightRedBg = 25,
  kLightGreenBg = 26,
  kLightYellowBg = 27,
  kLightBlueBg = 28,
  kLightMagentaBg = 29,
  kLightCyanBg = 30,
  kWhiteBg = 31,

  // Misc
  kBold = 32,
  kUnderline = 33,
  kNoUnderline = 34,
  kReverseText = 35,
  kNoReverseText = 36,
  kDefault = 37
};

constexpr char kAnsiCodeStr[][7]{
    "\033[30m",  "\033[31m",  "\033[32m",  "\033[33m",  "\033[34m",
    "\033[35m",  "\033[36m",  "\033[37m",  "\033[90m",  "\033[91m",
    "\033[92m",  "\033[93m",  "\033[94m",  "\033[95m",  "\033[96m",
    "\033[97m",  "\033[40m",  "\033[41m",  "\033[42m",  "\033[43m",
    "\033[44m",  "\033[45m",  "\033[46m",  "\033[47m",  "\033[100m",
    "\033[101m", "\033[102m", "\033[103m", "\033[104m", "\033[105m",
    "\033[106m", "\033[107m", "\033[1m",   "\033[4m",   "\033[24m",
    "\033[7m",   "\033[27m",  "\033[0m"};

constexpr int kBgColors[] = {
    static_cast<int>(AnsiCode::kLightRedBg),
    static_cast<int>(AnsiCode::kLightGreenBg),
    static_cast<int>(AnsiCode::kLightBlueBg),
    static_cast<int>(AnsiCode::kLightYellowBg),
    static_cast<int>(AnsiCode::kLightMagentaBg),
    static_cast<int>(AnsiCode::kLightCyanBg),
    static_cast<int>(AnsiCode::kDarkRedBg),
    static_cast<int>(AnsiCode::kDarkGreenBg),
    static_cast<int>(AnsiCode::kDarkBlueBg),
    static_cast<int>(AnsiCode::kDarkYellowBg),
    static_cast<int>(AnsiCode::kDarkMagentaBg),
    static_cast<int>(AnsiCode::kDarkCyanBg),
};
constexpr int kNumBgColors = static_cast<int>(std::size(kBgColors));

constexpr int kFgColors[] = {
    static_cast<int>(AnsiCode::kBlackFg),
    static_cast<int>(AnsiCode::kDarkGrayFg),
    static_cast<int>(AnsiCode::kLightGrayFg),
};
constexpr int kNumFgColors = static_cast<int>(std::size(kFgColors));

// Max length of filenames to print.
constexpr size_t kMaxFilenameSize = 32;

// Number of most recent files to print.
constexpr size_t kMaxNumRecentFiles = 32;

void PrintPadded(std::string line, size_t padded_size) {
  line.resize(padded_size, ' ');
  printf("%s\n", line.c_str());
}

// Returns teh base name of |path|, shortened to |kMaxFilenameSize| characters.
std::string GetShortFilename(const std::string path) {
  std::string filename = path::BaseName(path);
  if (filename.size() > kMaxFilenameSize)
    filename = filename.substr(0, kMaxFilenameSize - 2) + "..";
  return filename;
}

}  // namespace

StatsPrinter::StatsPrinter() = default;

StatsPrinter::~StatsPrinter() = default;

void StatsPrinter::InitFile(const std::string& path, size_t num_chunks) {
  path_to_file_[path].chunks.resize(num_chunks);
}

void StatsPrinter::Clear() {
  recent_files_.clear();
  path_to_file_.clear();
  thread_id_to_color_.clear();
  num_threads_ = 0;

  // Don't clear max_bandwidth_, it can't be recalculated, the others can.
  total_streamed_bytes_ = 0;
  total_cached_bytes_ = 0;
}

void StatsPrinter::ResetBandwidthStats() {
  bandwidth_timer_.Reset();
  curr_bandwidth_ = 0;
  curr_streamed_bytes_ = 0;
}

void StatsPrinter::RecordStreamedChunk(const std::string& path, size_t index,
                                       uint32_t size, size_t thread_id) {
  AddToRecentFiles(path);
  assert(path_to_file_.find(path) != path_to_file_.end());
  assert(index < path_to_file_[path].chunks.size());
  path_to_file_[path].chunks[index] =
      FileChunk(ChunkState::kStreamed, thread_id);
  curr_streamed_bytes_ += size;
  total_streamed_bytes_ += size;

  // Update thread-to-color map.
  if (thread_id_to_color_.find(thread_id) == thread_id_to_color_.end())
    thread_id_to_color_[thread_id] = num_threads_++;
}

void StatsPrinter::RecordCachedChunk(const std::string& path, size_t index,
                                     uint32_t size) {
  AddToRecentFiles(path);
  path_to_file_[path].chunks[index] = FileChunk(ChunkState::kCached, 0);
  total_cached_bytes_ += size;
}

void StatsPrinter::Print() {
  int console_width = Util::GetConsoleWidth();
  if (console_width < static_cast<int>(kMaxFilenameSize) + 4) return;
  printf("\r");

  size_t max_filename_size = 0;
  for (const std::string& path : recent_files_) {
    max_filename_size =
        std::max(max_filename_size, GetShortFilename(path).size());
  }

  std::string line;
  for (const std::string& path : recent_files_) {
    const File& file = path_to_file_[path];
    line = GetShortFilename(path);
    line.resize(max_filename_size + 1, ' ');

    // Fill the rest of the line with a visualization of the chunk states.
    size_t num_chunks = file.chunks.size();
    size_t print_width =
        std::min(num_chunks, static_cast<size_t>(console_width) - line.size());
    size_t num_chars = line.size() + print_width;

    for (int n = 0; n < print_width; ++n) {
      // There can be multiple chunks per output char. Pick the most recent one.
      size_t begin_idx = n * num_chunks / print_width;
      size_t end_idx = (n + 1) * num_chunks / print_width;

      absl::Time last_modified_time = file.chunks[begin_idx].modified_time;
      size_t last_modified_idx = begin_idx;
      for (size_t k = begin_idx + 1; k < end_idx; ++k) {
        if (last_modified_time < file.chunks[k].modified_time) {
          last_modified_time = file.chunks[k].modified_time;
          last_modified_idx = k;
        }
      }

      // Print character depending on the chunk type:
      //   - for chunks that have not been loaded.
      //   X for chunks that have been streamed.
      //   C for chunks that were cached.
      const FileChunk& chunk = file.chunks[last_modified_idx];
      if (chunk.state == ChunkState::kNotLoaded) {
        line += kAnsiCodeStr[static_cast<int>(AnsiCode::kDefault)];
        line.push_back('-');
      } else if (chunk.state == ChunkState::kCached) {
        line += kAnsiCodeStr[static_cast<int>(AnsiCode::kBlackFg)];
        line += kAnsiCodeStr[static_cast<int>(AnsiCode::kLightGrayBg)];
        line.push_back('C');
      } else {
        int col = thread_id_to_color_[chunk.thread_id];
        line += kAnsiCodeStr[kBgColors[col % kNumBgColors]];
        line += kAnsiCodeStr[kFgColors[(col / kNumBgColors) % kNumFgColors]];
        line.push_back('X');
      }

      // Return to default coloring.
      line += kAnsiCodeStr[static_cast<int>(AnsiCode::kDefault)];
    }

    // Fill with spaces and print.
    PrintPadded(std::move(line), line.size() + console_width - num_chars + 1);
  }

  // Print bandwidth and other stats.
  UpdateBandwidthStats();

  line = "Legend: (-) not loaded, (C) cached, (X) streamed (color=FUSE thread)";
  PrintPadded(std::move(line), console_width);

  constexpr double MBd = 1024.0 * 1024.0;
  line = absl::StrFormat("Bandwidth %7.2f MB/sec (curr) %7.2f MB/sec (max)",
                         curr_bandwidth_ / MBd, max_bandwidth_ / MBd);
  PrintPadded(std::move(line), console_width);

  constexpr int MBi = 1024 * 1024;
  line =
      absl::StrFormat("Total data %6i MB (streamed) %7i MB (cached)",
                      total_streamed_bytes_ / MBi, total_cached_bytes_ / MBi);
  PrintPadded(std::move(line), console_width);

  // Move cursor up, so that printing again overwrites the old content.
  for (size_t n = 0; n < recent_files_.size() + 3; ++n) printf("\033[F");
}

void StatsPrinter::AddToRecentFiles(const std::string& path) {
  if (std::find(recent_files_.begin(), recent_files_.end(), path) !=
      recent_files_.end()) {
    return;
  }

  recent_files_.push_back(path);
  if (recent_files_.size() > kMaxNumRecentFiles) recent_files_.pop_front();
}

void StatsPrinter::UpdateBandwidthStats() {
  double deltaSec = bandwidth_timer_.ElapsedSeconds();
  if (deltaSec < 1.0f) return;

  curr_bandwidth_ = curr_streamed_bytes_ / deltaSec;
  if (max_bandwidth_ < curr_bandwidth_) max_bandwidth_ = curr_bandwidth_;

  curr_streamed_bytes_ = 0;
  bandwidth_timer_.Reset();
}

}  // namespace cdc_ft
