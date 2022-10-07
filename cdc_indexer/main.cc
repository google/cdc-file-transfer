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

#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdio>
#include <iostream>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/flags/usage_config.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl_helper/jedec_size_flag.h"
#include "cdc_indexer/indexer.h"
#include "common/errno_mapping.h"
#include "common/path.h"

ABSL_FLAG(std::vector<std::string>, inputs, std::vector<std::string>(),
          "List of input files or directory to read from.");
ABSL_FLAG(uint32_t, num_threads, 0,
          "How many threads should read files in parallel, use 0 to "
          "auto-dertermine the best concurrency for this machine.");
ABSL_FLAG(cdc_ft::JedecSize, min_chunk_size, cdc_ft::JedecSize(0),
          "The minimum chunk size to size the files into. Defaults to half of "
          "the average chunk size. Supports common unit suffixes K, M, G.");
ABSL_FLAG(cdc_ft::JedecSize, avg_chunk_size, cdc_ft::JedecSize(256 << 10),
          "The average chunk size to size the files into. Supports common "
          "unit suffixes K, M, G.");
ABSL_FLAG(cdc_ft::JedecSize, max_chunk_size, cdc_ft::JedecSize(0),
          "The maximum chunk size to size the files into. Defaults to twice "
          "the average chunk size. Supports common unit suffixes K, M, G.");
ABSL_FLAG(cdc_ft::JedecSize, read_block_size, cdc_ft::JedecSize(0),
          "The block size to read the input file(s) from disk. Defaults to the "
          "value of --max_chunk_size. Supports common unit suffixes K, M, G.");
ABSL_FLAG(std::string, hash, "blake3",
          "Which hash function to use. Supported values are \"blake3\" and "
          "\"null\".");
ABSL_FLAG(std::string, results_file, "",
          "File name to append results to in CVS format.");
ABSL_FLAG(std::string, description, "",
          "A descriptive string of the experiment that was run. If given, this "
          "will be prepended literally to the results_file. Multiple columns "
          "can be separated with commas.");

namespace cdc_ft {
namespace {

const char* GearTable() {
  // The following macros are defined in indexer.h.
#if CDC_GEAR_TABLE == CDC_GEAR_32BIT
  return "32 bit";
#elif CDC_GEAR_TABLE == CDC_GEAR_64BIT
  return "64 bit";
#else
#error "Unknown gear table"
  return "unknown";
#endif
}

void SetupFlagsHelp() {
  absl::SetProgramUsageMessage(
      "CDC indexer to measure and report data redundancy.");
  absl::FlagsUsageConfig fuc;
  // Filter flags to show when the --help flag is set.
  fuc.contains_help_flags = [](absl::string_view f) {
    return absl::EndsWith(f, "main.cc");
  };
  absl::SetFlagsUsageConfig(fuc);
}

// Prints a human-readable representation of the given size, such as "4 KB".
template <typename T>
std::string HumanBytes(T size, int precision = 0) {
  const size_t threshold = 2048;
  if (size < 1024)
    return absl::StrFormat("%d bytes", static_cast<size_t>(size));
  double s = static_cast<double>(size) / 1024;
  std::string units = "KB";
  if (s > threshold) {
    s /= 1024;
    units = "MB";
  }
  if (s > threshold) {
    s /= 1024;
    units = "GB";
  }
  if (s > threshold) {
    s /= 1024;
    units = "TB";
  }
  if (s > threshold) {
    s /= 1024;
    units = "PB";
  }
  return absl::StrFormat("%.*f %s", precision, s, units);
}

// Prints a human-readable representation of a duration as minutes and seconds
// in the format "m:ss".
std::string HumanDuration(const absl::Duration& d) {
  auto sec = absl::ToInt64Seconds(d);
  return absl::StrFormat("%02d:%02d", sec / 60, std::abs(sec) % 60);
}

std::string HashTypeToString(IndexerConfig::HashType type) {
  switch (type) {
    case IndexerConfig::HashType::kNull:
      return "(no hashing)";
    case IndexerConfig::HashType::kBlake3:
      return "BLAKE3";
    default:
      return "unknown";
  }
}

// Prints progress information on stdout.
void ShowProgress(const Indexer::OpStats& stats) {
  static absl::Time op_start = absl::Now();
  static absl::Time last_progress = op_start;
  static size_t last_total_bytes = 0;

  auto now = absl::Now();
  auto elapsed = now - last_progress;
  if (elapsed < absl::Milliseconds(500)) return;

  double bps =
      (stats.total_bytes - last_total_bytes) / absl::ToDoubleSeconds(elapsed);
  double dedup_pct = (stats.total_bytes - stats.unique_bytes) /
                     static_cast<double>(stats.total_bytes) * 100.0;
  std::cout << '\r' << HumanDuration(now - op_start) << "   " << std::setw(2)
            << HumanBytes(stats.total_bytes, 2) << " in " << stats.total_files
            << " files processed at " << HumanBytes(bps, 1) << "/s"
            << ", " << static_cast<int>(dedup_pct) << "% deduplication"
            << std::flush;
  last_progress = now;
  last_total_bytes = stats.total_bytes;
}

void ShowSummary(const IndexerConfig& cfg, const Indexer::OpStats& stats,
                 absl::Duration elapsed) {
  const int title_w = 20;
  const int num_w = 16;
  double dedup_pct = (stats.total_bytes - stats.unique_bytes) /
                     static_cast<double>(stats.total_bytes) * 100.0;
  double bps = stats.total_bytes / absl::ToDoubleSeconds(elapsed);
  std::cout << "Chunk size (min/avg/max): " << HumanBytes(cfg.min_chunk_size)
            << " / " << HumanBytes(cfg.avg_chunk_size) << " / "
            << HumanBytes(cfg.max_chunk_size)
            << "  |  Hash: " << HashTypeToString(cfg.hash_type)
            << "  |  Threads: " << cfg.num_threads << std::endl;
  std::cout << "gear_table: " << GearTable() << "  |  mask_s: 0x" << std::hex
            << cfg.mask_s << "  |  mask_l: 0x" << cfg.mask_l << std::dec
            << std::endl;
  std::cout << std::setw(title_w) << "Duration:" << std::setw(num_w)
            << HumanDuration(elapsed) << std::endl;
  std::cout << std::setw(title_w) << "Total files:" << std::setw(num_w)
            << stats.total_files << std::endl;
  std::cout << std::setw(title_w) << "Total chunks:" << std::setw(num_w)
            << stats.total_chunks << std::endl;
  std::cout << std::setw(title_w) << "Unique chunks:" << std::setw(num_w)
            << stats.unique_chunks << std::endl;
  std::cout << std::setw(title_w) << "Total data:" << std::setw(num_w)
            << HumanBytes(stats.total_bytes, 2) << std::endl;
  std::cout << std::setw(title_w) << "Unique data:" << std::setw(num_w)
            << HumanBytes(stats.unique_bytes, 2) << std::endl;
  std::cout << std::setw(title_w) << "Throughput:" << std::setw(num_w - 2)
            << HumanBytes(bps, 2) << "/s" << std::endl;
  std::cout << std::setw(title_w) << "Avg. chunk size:" << std::setw(num_w)
            << HumanBytes(static_cast<double>(stats.unique_bytes) /
                          stats.unique_chunks)
            << std::endl;
  std::cout << std::setw(title_w) << "Deduplication:" << std::setw(num_w - 1)
            << std::setprecision(4) << dedup_pct << "%" << std::endl;
}

void ShowChunkSize(size_t size, uint64_t cnt, uint64_t max_count,
                   uint64_t total_count) {
  const int key_w = 7;
  const int hbar_w = 40;
  const int num_w = 10;
  const int pct_w = 2;

  double pct = 100.0 * static_cast<double>(cnt) / total_count;
  double hscale = static_cast<double>(cnt) / max_count;
  int blocks = round(hscale * hbar_w);

  std::cout << std::setw(key_w) << HumanBytes(size) << " ";
  for (int i = 0; i < blocks; i++) std::cout << "#";
  for (int i = hbar_w - blocks; i > 0; i--) std::cout << " ";
  std::cout << " " << std::setw(num_w) << cnt << " (" << std::setw(pct_w)
            << round(pct) << "%)" << std::endl;
}

std::vector<size_t> ChunkSizeBuckets(const IndexerConfig& cfg,
                                     const Indexer::ChunkSizeMap& sizes,
                                     size_t fixed_min_size,
                                     size_t fixed_max_size,
                                     uint64_t* max_count_out,
                                     uint64_t* total_count_out) {
  size_t min_size = 1u << 31;
  size_t max_size = 0;
  uint64_t max_count = 0;
  uint64_t total_count = 0, found_count = 0;
  uint64_t outside_min_max_count = 0;
  std::vector<size_t> buckets;
  // Find out min/max chunk sizes
  for (auto [chunk_size, count] : sizes) {
    if (chunk_size < min_size) min_size = chunk_size;
    if (chunk_size > max_size) max_size = chunk_size;
    if (count > max_count) max_count = count;
    if (chunk_size < fixed_min_size) outside_min_max_count += count;
    if (fixed_max_size > 0 && chunk_size > fixed_max_size)
      outside_min_max_count += count;
    total_count += count;
  }
  if (fixed_min_size > 0) min_size = fixed_min_size;
  // Use steps of powers of two until min. chunk size is reached.
  uint64_t size;
  uint64_t pow_end_size = std::min(cfg.min_chunk_size, max_size);
  for (size = min_size; size < pow_end_size; size <<= 1) {
    buckets.push_back(size);
    auto it = sizes.find(size);
    if (it != sizes.end()) found_count += it->second;
  }
  if (fixed_max_size > max_size) max_size = fixed_max_size;
  // Use step increments of max_chunk_size_step afterwards.
  for (; size <= max_size; size += cfg.max_chunk_size_step) {
    buckets.push_back(size);
    auto it = sizes.find(size);
    if (it != sizes.end()) found_count += it->second;
  }
  // Make sure we found every bucket.
  assert(total_count == found_count + outside_min_max_count);
  if (max_count_out) *max_count_out = max_count;
  if (total_count_out) *total_count_out = total_count;
  return buckets;
}

void ShowChunkSizes(const IndexerConfig& cfg,
                    const Indexer::ChunkSizeMap& sizes) {
  uint64_t max_count = 0;
  uint64_t total_count = 0;
  auto buckets = ChunkSizeBuckets(cfg, sizes, 0, 0, &max_count, &total_count);
  for (auto size : buckets) {
    auto it = sizes.find(size);
    uint64_t cnt = it != sizes.end() ? it->second : 0;
    ShowChunkSize(size, cnt, max_count, total_count);
  }
}

absl::Status WriteResultsFile(const std::string& filepath,
                              const std::string& description,
                              const IndexerConfig& cfg,
                              const Indexer::OpStats& stats,
                              const Indexer::ChunkSizeMap& sizes) {
  bool exists = path::FileExists(filepath);
  std::FILE* fout = std::fopen(filepath.c_str(), "a");
  if (!fout) {
    return ErrnoToCanonicalStatus(
        errno, absl::StrFormat("Couldn't write to file '%s'", filepath));
  }

  path::FileCloser closer(fout);

  static constexpr int num_columns = 15;
  static const char* columns[num_columns] = {
      "gear_table",
      "mask_s",
      "mask_l",
      "Min chunk size [KiB]",
      "Avg chunk size [KiB]",
      "Max chunk size [KiB]",
      "Read speed [MiB/s]",
      "Files",
      "Total chunks",
      "Unique chunks",
      "Total size [MiB]",
      "Unique size [MiB]",
      "Dedup size [MiB]",
      "Dedup ratio",
      "Res avg chunk size [KiB]",
  };

  auto buckets = ChunkSizeBuckets(cfg, sizes, cfg.min_chunk_size,
                                  cfg.max_chunk_size, nullptr, nullptr);
  // Write column headers this is a new file.
  if (!exists) {
    // Write empty columns corresponding to the no. of given columns.
    int desc_cols = description.empty() ? 0 : 1;
    desc_cols += std::count(description.begin(), description.end(), ',');
    for (int i = 0; i < desc_cols; i++) {
      std::fprintf(fout, i == 0 ? "Description," : ",");
    }
    // Write fixed column headers.
    for (int i = 0; i < num_columns; i++) {
      std::fprintf(fout, "%s,", columns[i]);
    }
    // Write chunk distribution column headers
    for (auto size : buckets) {
      std::fprintf(fout, "%s,", HumanBytes(size).c_str());
    }
    std::fprintf(fout, "\n");
  }

  // Count allow chunks below min_chunk_size and above max_chunk_size as they
  // won't be included in the buckets list automatically.
  uint64_t below_min_cnt = 0, above_max_cnt = 0;
  for (auto [chunk_size, count] : sizes) {
    if (chunk_size < cfg.min_chunk_size) below_min_cnt += count;
    if (chunk_size > cfg.max_chunk_size) above_max_cnt += count;
  }

  static constexpr double mib = static_cast<double>(1 << 20);

  // Write user-supplied description
  if (!description.empty()) std::fprintf(fout, "%s,", description.c_str());
  // Write chunking params.
  std::fprintf(fout, "%s,0x%zx,0x%zx,", GearTable(), cfg.mask_s, cfg.mask_l);
  std::fprintf(fout, "%zu,%zu,%zu,", cfg.min_chunk_size >> 10,
               cfg.avg_chunk_size >> 10, cfg.max_chunk_size >> 10);
  // Write speed, files, chunks.
  double mibps =
      (stats.total_bytes / mib) / absl::ToDoubleSeconds(stats.elapsed);
  std::fprintf(fout, "%f,%zu,%zu,%zu,", mibps, stats.total_files,
               stats.total_chunks, stats.unique_chunks);
  // Write total and unique sizes.
  std::fprintf(fout, "%f,%f,%f,", stats.total_bytes / mib,
               stats.unique_bytes / mib,
               (stats.total_bytes - stats.unique_bytes) / mib);
  // Write dedup ratio and avg. chunk size.
  double dedup_ratio = (stats.total_bytes - stats.unique_bytes) /
                       static_cast<double>(stats.total_bytes);
  size_t avg_size = stats.unique_bytes / stats.unique_chunks;
  std::fprintf(fout, "%f,%zu,", dedup_ratio, avg_size >> 10);
  // Write chunk distribution
  size_t index = 0;
  for (auto size : buckets) {
    auto it = sizes.find(size);
    uint64_t cnt = it != sizes.end() ? it->second : 0;
    if (index == 0) {
      cnt += below_min_cnt;
    } else if (index + 1 == buckets.size()) {
      cnt += above_max_cnt;
    }
    ++index;
    std::fprintf(fout, "%f,", static_cast<double>(cnt) / stats.unique_chunks);
  }
  std::fprintf(fout, "\n");
  return absl::OkStatus();
}

IndexerConfig::HashType GetHashType(const std::string name) {
  if (name == "null") return IndexerConfig::HashType::kNull;
  if (name == "blake3") return IndexerConfig::HashType::kBlake3;
  std::cerr << "Unknown hash type: \"" << name << "\"" << std::endl;
  return IndexerConfig::HashType::kUndefined;
}

}  // namespace
}  // namespace cdc_ft

int main(int argc, char* argv[]) {
  cdc_ft::SetupFlagsHelp();
  absl::ParseCommandLine(argc, argv);

  std::vector<std::string> inputs = absl::GetFlag(FLAGS_inputs);

  if (inputs.empty()) {
    std::cout << "Execute the following command to get help on the usage:"
              << std::endl
              << argv[0] << " --help" << std::endl;
    return 0;
  }

  cdc_ft::IndexerConfig cfg;
  cfg.num_threads = absl::GetFlag(FLAGS_num_threads);
  cfg.min_chunk_size = absl::GetFlag(FLAGS_min_chunk_size).Size();
  cfg.avg_chunk_size = absl::GetFlag(FLAGS_avg_chunk_size).Size();
  cfg.max_chunk_size = absl::GetFlag(FLAGS_max_chunk_size).Size();
  cfg.read_block_size = absl::GetFlag(FLAGS_read_block_size).Size();
  cfg.hash_type = cdc_ft::GetHashType(absl::GetFlag(FLAGS_hash));

  if (!cfg.min_chunk_size) cfg.min_chunk_size = cfg.avg_chunk_size >> 1;
  if (!cfg.max_chunk_size) cfg.max_chunk_size = cfg.avg_chunk_size << 1;
  if (!cfg.read_block_size) cfg.read_block_size = cfg.max_chunk_size;
  cfg.max_chunk_size_step = std::max<size_t>(cfg.min_chunk_size >> 2, 1024u);
  assert(cfg.avg_chunk_size > 0);
  assert(cfg.avg_chunk_size > cfg.min_chunk_size);
  assert(cfg.avg_chunk_size < cfg.max_chunk_size);
  assert(cfg.hash_type != cdc_ft::IndexerConfig::HashType::kUndefined);

  cdc_ft::Indexer idx;
  std::cout << "Starting indexer on " << inputs.size() << " inputs."
            << std::endl;
  static absl::Time start = absl::Now();
  absl::Status res = idx.Run(cfg, inputs, cdc_ft::ShowProgress);
  auto elapsed = absl::Now() - start;
  std::cout << std::endl;
  if (res.ok()) {
    std::cout << "Operation succeeded." << std::endl << std::endl;
    cdc_ft::ShowSummary(idx.Config(), idx.Stats(), elapsed);
    std::cout << std::endl;
    cdc_ft::ShowChunkSizes(idx.Config(), idx.ChunkSizes());
    std::string results_file = absl::GetFlag(FLAGS_results_file);
    if (!results_file.empty()) {
      res = cdc_ft::WriteResultsFile(
          results_file, absl::GetFlag(FLAGS_description), idx.Config(),
          idx.Stats(), idx.ChunkSizes());
      if (!res.ok())
        std::cerr << "Failed to write results to '" << results_file
                  << "': " << res.message() << std::endl;
    }
  } else {
    std::cerr << "Error: (" << res.code() << ") " << res.message() << std::endl;
  }

  return static_cast<int>(res.code());
}
