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

#include "cdc_rsync_server/file_diff_generator.h"

#include "cdc_rsync_server/file_info.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "common/test_main.h"
#include "gtest/gtest.h"

namespace cdc_ft {

bool operator==(const FileInfo& a, const FileInfo& b) {
  return a.filepath == b.filepath && a.modified_time == b.modified_time &&
         a.size == b.size && a.client_index == b.client_index &&
         a.base_dir == b.base_dir;
}

bool operator==(const ChangedFileInfo& a, const ChangedFileInfo& b) {
  return a.filepath == b.filepath &&
         a.client_modified_time == b.client_modified_time &&
         a.client_size == b.client_size && a.server_size == b.server_size &&
         a.client_index == b.client_index && a.base_dir == b.base_dir;
}

bool operator==(const DirInfo& a, const DirInfo& b) {
  return a.filepath == b.filepath && a.client_index == b.client_index &&
         a.base_dir == b.base_dir;
}

namespace {

constexpr int64_t kModTime1 = 123;
constexpr int64_t kModTime2 = 234;
constexpr int64_t kModTime3 = 345;
constexpr int64_t kModTime4 = 456;

constexpr uint64_t kFileSize1 = 1000;
constexpr uint64_t kFileSize2 = 2000;
constexpr uint64_t kFileSize3 = 3000;
constexpr uint64_t kFileSize4 = 4000;

constexpr uint32_t kClientIndex1 = 1;
constexpr uint32_t kClientIndex2 = 2;
constexpr uint32_t kClientIndex3 = 3;
constexpr uint32_t kClientIndex4 = 4;
constexpr uint32_t kClientIndex5 = 5;

constexpr bool kDoubleCheckMissing = true;
constexpr bool kNoDoubleCheckMissing = false;

constexpr bool kExisting = true;
constexpr bool kNoExisting = false;

constexpr bool kChecksum = true;
constexpr bool kNoChecksum = false;

constexpr bool kWholeFile = true;
constexpr bool kNoWholeFile = false;

constexpr char kNoCopyDest[] = "";

// Note: FileDiffGenerator is a server-only class and only runs on GGP, but the
// code is independent of the platform, so we can test it from Windows.
class FileDiffGeneratorTest : public ::testing::Test {
 protected:
  std::string base_dir_ =
      path::Join(GetTestDataDir("file_diff_generator"), "base_dir");

  std::string copy_dest_ =
      path::Join(GetTestDataDir("file_diff_generator"), "copy_dest");

  const FileInfo client_file_ =
      FileInfo("file/path1", kModTime1, kFileSize1, kClientIndex1, nullptr);
  const FileInfo server_file_ =
      FileInfo("file/path2", kModTime2, kFileSize2, FileInfo::kInvalidIndex,
               base_dir_.c_str());

  FileInfo matching_client_file_ =
      FileInfo("file/path3", kModTime3, kFileSize3, kClientIndex2, nullptr);
  const FileInfo matching_server_file_ =
      FileInfo("file/path3", kModTime3, kFileSize3, FileInfo::kInvalidIndex,
               base_dir_.c_str());

  FileInfo changed_size_client_file_ =
      FileInfo("file/path4", kModTime3, kFileSize3, kClientIndex3, nullptr);
  const FileInfo changed_size_server_file_ =
      FileInfo("file/path4", kModTime3, kFileSize4, FileInfo::kInvalidIndex,
               base_dir_.c_str());

  FileInfo changed_time_client_file_ =
      FileInfo("file/path5", kModTime3, kFileSize3, kClientIndex3, nullptr);
  const FileInfo changed_time_server_file_ =
      FileInfo("file/path5", kModTime4, kFileSize3, FileInfo::kInvalidIndex,
               base_dir_.c_str());

  const DirInfo client_dir_ = DirInfo("dir/dir1", kClientIndex4, nullptr);
  const DirInfo server_dir_ =
      DirInfo("dir/dir2", FileInfo::kInvalidIndex, base_dir_.c_str());

  const DirInfo matching_client_dir_ =
      DirInfo("dir/dir3", kClientIndex5, nullptr);
  const DirInfo matching_server_dir_ =
      DirInfo("dir/dir3", FileInfo::kInvalidIndex, base_dir_.c_str());

  // Creates a FileInfo struct by filling data from the file at
  // |fi_base_dir|\|filename|. If |fi_base_dir| is nullptr (for client files)
  // reads from |base_dir_| instead.
  FileInfo CreateFileInfo(const char* filename, const char* fi_base_dir) {
    std::string path =
        path::Join(fi_base_dir ? fi_base_dir : base_dir_, filename);
    path::Stats stats;
    EXPECT_OK(path::GetStats(path, &stats));
    return FileInfo(filename, stats.modified_time, stats.size, 0, fi_base_dir);
  }

  // Creates a default file_diff::Result with one file/dir in each bucket.
  file_diff::Result MakeResultForAdjustTests() {
    file_diff::Result diff;

    diff.matching_files.push_back(matching_client_file_);
    diff.changed_files.push_back(ChangedFileInfo(
        changed_size_server_file_, FileInfo(changed_size_client_file_)));
    diff.missing_files.push_back(client_file_);
    diff.extraneous_files.push_back(server_file_);

    diff.matching_dirs.push_back(matching_client_dir_);
    diff.missing_dirs.push_back(client_dir_);
    diff.extraneous_dirs.push_back(server_dir_);

    return diff;
  }
};

TEST_F(FileDiffGeneratorTest, MissingFile) {
  file_diff::Result diff =
      file_diff::Generate({client_file_}, {}, {}, {}, base_dir_, kNoCopyDest,
                          kNoDoubleCheckMissing);

  EXPECT_EQ(diff.missing_files, std::vector<FileInfo>({client_file_}));
  EXPECT_TRUE(diff.extraneous_files.empty());
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_TRUE(diff.extraneous_dirs.empty());
}

TEST_F(FileDiffGeneratorTest, ExtraneousFile) {
  file_diff::Result diff =
      file_diff::Generate({}, {server_file_}, {}, {}, base_dir_, kNoCopyDest,
                          kNoDoubleCheckMissing);

  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_TRUE(diff.extraneous_dirs.empty());
}

TEST_F(FileDiffGeneratorTest, MatchingFiles) {
  file_diff::Result diff =
      file_diff::Generate({matching_client_file_}, {matching_server_file_}, {},
                          {}, base_dir_, kNoCopyDest, kNoDoubleCheckMissing);

  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_TRUE(diff.extraneous_files.empty());
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_EQ(diff.matching_files,
            std::vector<FileInfo>({matching_client_file_}));
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_TRUE(diff.extraneous_dirs.empty());
}

TEST_F(FileDiffGeneratorTest, ChangedFiles) {
  // Purposely swap the order for the server files to test sorting.
  file_diff::Result diff = file_diff::Generate(
      {changed_time_client_file_, changed_size_client_file_},
      {changed_size_server_file_, changed_time_server_file_}, {}, {}, base_dir_,
      kNoCopyDest, kNoDoubleCheckMissing);

  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_TRUE(diff.extraneous_files.empty());
  EXPECT_EQ(diff.changed_files,
            std::vector<ChangedFileInfo>(
                {ChangedFileInfo(changed_size_server_file_,
                                 std::move(changed_size_client_file_)),
                 ChangedFileInfo(changed_time_server_file_,
                                 std::move(changed_time_client_file_))}));
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_TRUE(diff.extraneous_dirs.empty());
}

TEST_F(FileDiffGeneratorTest, OrderIndependence) {
  std::vector<FileInfo> client_files = {client_file_, matching_client_file_,
                                        changed_size_client_file_,
                                        changed_time_client_file_};
  std::vector<FileInfo> server_files = {server_file_, matching_server_file_,
                                        changed_size_server_file_,
                                        changed_time_server_file_};

  std::vector<FileInfo> expected_missing_files = {client_file_};
  std::vector<FileInfo> expected_extraneous_files = {server_file_};
  std::vector<ChangedFileInfo> expected_changed_files = {
      ChangedFileInfo(changed_size_server_file_,
                      std::move(changed_size_client_file_)),
      ChangedFileInfo(changed_time_server_file_,
                      std::move(changed_time_client_file_))};
  std::vector<FileInfo> expected_matching_files = {matching_client_file_};

  // Make several tests, each time with |server_files| permuted a bit..
  for (size_t backwards = 0; backwards < 2; ++backwards) {
    for (size_t circular = 0; circular < server_files.size(); ++circular) {
      file_diff::Result diff =
          file_diff::Generate(std::vector<FileInfo>(client_files),
                              std::vector<FileInfo>(server_files), {}, {},
                              base_dir_, kNoCopyDest, kNoDoubleCheckMissing);

      EXPECT_EQ(diff.missing_files, expected_missing_files);
      EXPECT_EQ(diff.extraneous_files, expected_extraneous_files);
      EXPECT_EQ(diff.changed_files, expected_changed_files);
      EXPECT_EQ(diff.matching_files, expected_matching_files);

      // Circular permutation.
      server_files.insert(server_files.begin(), server_files.back());
      server_files.pop_back();
    }

    // Reverse order.
    std::reverse(server_files.begin(), server_files.end());
  }
}

TEST_F(FileDiffGeneratorTest, DoubleCheckMissing_NoCopyDest) {
  // file_a is matching the real file, file_b is changed, file_h is missing.
  FileInfo file_a = CreateFileInfo("a.txt", nullptr);
  FileInfo file_b = CreateFileInfo("b.txt", nullptr);
  file_b.modified_time = 0;
  FileInfo file_h("h.txt", 0, 0, 0, nullptr);

  file_diff::Result diff =
      file_diff::Generate({file_a, file_b, file_h}, {}, {}, {}, base_dir_,
                          kNoCopyDest, kDoubleCheckMissing);

  FileInfo server_file_a = CreateFileInfo("a.txt", base_dir_.c_str());
  FileInfo server_file_b = CreateFileInfo("b.txt", base_dir_.c_str());

  ChangedFileInfo changed_file_b(server_file_b, std::move(file_b));

  EXPECT_EQ(diff.matching_files, std::vector<FileInfo>({file_a}));
  EXPECT_EQ(diff.changed_files, std::vector<ChangedFileInfo>({changed_file_b}));
  EXPECT_EQ(diff.missing_files, std::vector<FileInfo>({file_h}));
  EXPECT_TRUE(diff.extraneous_files.empty());
}

TEST_F(FileDiffGeneratorTest, DoubleCheckMissing_CopyDest) {
  // Tests all permutations of client files and server files in base_dir as
  // well as copy_dest. Special treatment is marked as !!!.
  //    client files        server files                 resulting diff list
  //                 base_dir          copy_dest
  // a  exists       exists, matching  missing           matching/base_dir
  // b  exists       exists, changed   missing           changed/base_dir
  // c  missing      exists            missing           extraneous/base_dir
  // d  exists       missing           missing           missing
  // e  exists       exists, matching  exists, ignored   matching/base_dir
  // f  exists       exists, changed   exists, ignored   changed/base_dir
  // g  missing      exists            exists, ignored   extraneous/base_dir
  // h  exists       missing           exists, matching  changed/copy_dest (!!!)
  // i  exists       missing           exists, changed   changed/copy_dest
  // j  missing      missing           exists            ignored (!!!)

  // Client files.
  FileInfo file_a = CreateFileInfo("a.txt", nullptr);
  FileInfo file_b = CreateFileInfo("b.txt", nullptr);
  // c missing
  FileInfo file_d = FileInfo("d.txt", 0, 0, 0, nullptr);
  FileInfo file_e = CreateFileInfo("e.txt", nullptr);
  FileInfo file_f = CreateFileInfo("f.txt", nullptr);
  // g missing
  FileInfo file_h = CreateFileInfo("h.txt", copy_dest_.c_str());
  file_h.base_dir = nullptr;
  FileInfo file_i = FileInfo("i.txt", 0, 0, 0, nullptr);
  // j missing

  // Mark files b and f as changed.
  file_b.modified_time = 0;
  file_f.modified_time = 0;

  std::vector<FileInfo> client_files = {file_a, file_b, file_d, file_e,
                                        file_f, file_h, file_i};

  // Server files in base_dir. d, h, i and j are missing.
  FileInfo server_file_a = CreateFileInfo("a.txt", base_dir_.c_str());
  FileInfo server_file_b = CreateFileInfo("b.txt", base_dir_.c_str());
  FileInfo server_file_c = CreateFileInfo("c.txt", base_dir_.c_str());
  FileInfo server_file_e = CreateFileInfo("e.txt", base_dir_.c_str());
  FileInfo server_file_f = CreateFileInfo("f.txt", base_dir_.c_str());
  FileInfo server_file_g = CreateFileInfo("g.txt", base_dir_.c_str());

  std::vector<FileInfo> server_files = {server_file_a, server_file_b,
                                        server_file_c, server_file_e,
                                        server_file_f, server_file_g};

  std::vector<FileInfo> expected_matching_files = {file_a, file_e};

  ChangedFileInfo changed_file_b(server_file_b, std::move(file_b));
  ChangedFileInfo changed_file_f(server_file_f, std::move(file_f));
  ChangedFileInfo changed_file_h(CreateFileInfo("h.txt", copy_dest_.c_str()),
                                 std::move(file_h));
  ChangedFileInfo changed_file_i(CreateFileInfo("i.txt", copy_dest_.c_str()),
                                 std::move(file_i));
  std::vector<ChangedFileInfo> expected_changed_files = {
      changed_file_b, changed_file_f, changed_file_h, changed_file_i};

  std::vector<FileInfo> expected_missing_files = {file_d};
  std::vector<FileInfo> expected_extraneous_files = {server_file_c,
                                                     server_file_g};

  // The server files in copy_dest are stat'ed by file_diff::Generate().

  file_diff::Result diff =
      file_diff::Generate(std::move(client_files), std::move(server_files), {},
                          {}, base_dir_, copy_dest_, kDoubleCheckMissing);

  EXPECT_EQ(diff.matching_files, expected_matching_files);
  EXPECT_EQ(diff.changed_files, expected_changed_files);
  EXPECT_EQ(diff.missing_files, expected_missing_files);
  EXPECT_EQ(diff.extraneous_files, expected_extraneous_files);
}

TEST_F(FileDiffGeneratorTest, MissingDir) {
  file_diff::Result diff = file_diff::Generate(
      {}, {}, {client_dir_}, {}, base_dir_, kNoCopyDest, kNoDoubleCheckMissing);

  EXPECT_EQ(diff.missing_dirs, std::vector<DirInfo>({client_dir_}));
  EXPECT_TRUE(diff.extraneous_dirs.empty());
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_TRUE(diff.extraneous_files.empty());
}

TEST_F(FileDiffGeneratorTest, ExtraneousDir) {
  file_diff::Result diff = file_diff::Generate(
      {}, {}, {}, {server_dir_}, base_dir_, kNoCopyDest, kNoDoubleCheckMissing);

  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));
  EXPECT_TRUE(diff.matching_dirs.empty());
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_TRUE(diff.extraneous_files.empty());
}

TEST_F(FileDiffGeneratorTest, MatchingDirs) {
  file_diff::Result diff = file_diff::Generate(
      {}, {}, {matching_client_dir_}, {matching_server_dir_}, base_dir_,
      kNoCopyDest, kNoDoubleCheckMissing);

  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_TRUE(diff.extraneous_dirs.empty());
  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_TRUE(diff.extraneous_files.empty());
}

TEST_F(FileDiffGeneratorTest, DirOrderIndependence) {
  std::vector<DirInfo> client_dirs = {client_dir_, matching_client_dir_};
  std::vector<DirInfo> server_dirs = {server_dir_, matching_server_dir_};

  std::vector<DirInfo> expected_missing_dirs = {client_dir_};
  std::vector<DirInfo> expected_extraneous_dirs = {server_dir_};
  std::vector<DirInfo> expected_matching_dirs = {matching_client_dir_};

  // Make several tests, each time with |server_dirs| permuted a bit.
  for (size_t backwards = 0; backwards < 2; ++backwards) {
    for (size_t circular = 0; circular < server_dirs.size(); ++circular) {
      file_diff::Result diff =
          file_diff::Generate({}, {}, std::vector<DirInfo>(client_dirs),
                              std::vector<DirInfo>(server_dirs), base_dir_,
                              kNoCopyDest, kNoDoubleCheckMissing);

      EXPECT_EQ(diff.missing_dirs, expected_missing_dirs);
      EXPECT_EQ(diff.extraneous_dirs, expected_extraneous_dirs);
      EXPECT_EQ(diff.matching_dirs, expected_matching_dirs);

      // Circular permutation.
      server_dirs.insert(server_dirs.begin(), server_dirs.back());
      server_dirs.pop_back();
    }

    // Reverse order.
    std::reverse(server_dirs.begin(), server_dirs.end());
  }
}

TEST_F(FileDiffGeneratorTest, CopyDest_Dirs) {
  DirInfo client_dir1("dir/dir1", kClientIndex1, nullptr);
  DirInfo client_dir2("dir/dir2", kClientIndex2, nullptr);

  // Matching in |copy_dest_|
  // -> counted as missing (so it gets created in destination)
  DirInfo server_dir1("dir/dir1", FileInfo::kInvalidIndex, copy_dest_.c_str());
  // Extraneous in |copy_dest_|
  // -> ignored (shouldn't delete dirs in package dir!)
  DirInfo server_dir2("dir/dir3", FileInfo::kInvalidIndex, copy_dest_.c_str());

  file_diff::Result diff = file_diff::Generate(
      {}, {}, std::vector<DirInfo>{client_dir1, client_dir2},
      std::vector<DirInfo>{server_dir1, server_dir2}, base_dir_, copy_dest_,
      kNoDoubleCheckMissing);

  EXPECT_EQ(diff.missing_dirs,
            std::vector<DirInfo>({client_dir1, client_dir2}));
  EXPECT_TRUE(diff.extraneous_dirs.empty());
  EXPECT_TRUE(diff.matching_dirs.empty());
}

TEST_F(FileDiffGeneratorTest, Adjust_DefaultParams) {
  file_diff::Result diff = MakeResultForAdjustTests();
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      kNoExisting, kNoChecksum, kNoWholeFile, &diff);

  EXPECT_EQ(diff.matching_files,
            std::vector<FileInfo>({matching_client_file_}));
  EXPECT_EQ(diff.missing_files, std::vector<FileInfo>({client_file_}));
  EXPECT_EQ(
      diff.changed_files,
      std::vector<ChangedFileInfo>({ChangedFileInfo(
          changed_size_server_file_, std::move(changed_size_client_file_))}));
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));

  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_EQ(diff.missing_dirs, std::vector<DirInfo>({client_dir_}));
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));

  EXPECT_EQ(response.num_matching_files(), 1);
  EXPECT_EQ(response.num_missing_files(), 1);
  EXPECT_EQ(response.num_changed_files(), 1);
  EXPECT_EQ(response.num_extraneous_files(), 1);

  EXPECT_EQ(response.num_matching_dirs(), 1);
  EXPECT_EQ(response.num_missing_dirs(), 1);
  EXPECT_EQ(response.num_extraneous_dirs(), 1);

  EXPECT_EQ(response.total_changed_client_bytes(),
            changed_size_client_file_.size);
  EXPECT_EQ(response.total_changed_server_bytes(),
            changed_size_server_file_.size);
  EXPECT_EQ(response.total_missing_bytes(), client_file_.size);
}

TEST_F(FileDiffGeneratorTest, Adjust_Existing) {
  file_diff::Result diff = MakeResultForAdjustTests();
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      kExisting, kNoChecksum, kNoWholeFile, &diff);

  // Existing removes missing files.
  EXPECT_EQ(diff.matching_files,
            std::vector<FileInfo>({matching_client_file_}));
  EXPECT_TRUE(diff.missing_files.empty());
  EXPECT_EQ(
      diff.changed_files,
      std::vector<ChangedFileInfo>({ChangedFileInfo(
          changed_size_server_file_, std::move(changed_size_client_file_))}));
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));

  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_TRUE(diff.missing_dirs.empty());
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));

  // These stats should be unchanged.
  EXPECT_EQ(response.num_matching_files(), 1);
  EXPECT_EQ(response.num_missing_files(), 1);
  EXPECT_EQ(response.num_changed_files(), 1);
  EXPECT_EQ(response.num_extraneous_files(), 1);

  EXPECT_EQ(response.num_matching_dirs(), 1);
  EXPECT_EQ(response.num_missing_dirs(), 1);
  EXPECT_EQ(response.num_extraneous_dirs(), 1);

  // These stats should be computed from the actual containers.
  EXPECT_EQ(response.total_changed_client_bytes(),
            changed_size_client_file_.size);
  EXPECT_EQ(response.total_changed_server_bytes(),
            changed_size_server_file_.size);
  EXPECT_EQ(response.total_missing_bytes(), 0);
}

TEST_F(FileDiffGeneratorTest, Adjust_Checksum) {
  file_diff::Result diff = MakeResultForAdjustTests();
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      kNoExisting, kChecksum, kNoWholeFile, &diff);

  // Checksum moves matching files to changed files.
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_EQ(diff.missing_files, std::vector<FileInfo>({client_file_}));
  EXPECT_EQ(diff.changed_files,
            std::vector<ChangedFileInfo>(
                {ChangedFileInfo(changed_size_server_file_,
                                 std::move(changed_size_client_file_)),
                 ChangedFileInfo(matching_client_file_,
                                 std::move(matching_client_file_))}));
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));

  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_EQ(diff.missing_dirs, std::vector<DirInfo>({client_dir_}));
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));

  // These stats should be unchanged.
  EXPECT_EQ(response.num_matching_files(), 1);
  EXPECT_EQ(response.num_missing_files(), 1);
  EXPECT_EQ(response.num_changed_files(), 1);
  EXPECT_EQ(response.num_extraneous_files(), 1);

  EXPECT_EQ(response.num_matching_dirs(), 1);
  EXPECT_EQ(response.num_missing_dirs(), 1);
  EXPECT_EQ(response.num_extraneous_dirs(), 1);

  // These stats should be computed from the actual containers.
  EXPECT_EQ(response.total_changed_client_bytes(),
            changed_size_client_file_.size + matching_client_file_.size);
  EXPECT_EQ(response.total_changed_server_bytes(),
            changed_size_server_file_.size + matching_client_file_.size);
  EXPECT_EQ(response.total_missing_bytes(), client_file_.size);
}

TEST_F(FileDiffGeneratorTest, Adjust_WholeFile) {
  file_diff::Result diff = MakeResultForAdjustTests();
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      kNoExisting, kNoChecksum, kWholeFile, &diff);

  // WholeFile moves changed files to missing files.
  EXPECT_EQ(diff.matching_files,
            std::vector<FileInfo>({matching_client_file_}));
  EXPECT_EQ(diff.missing_files,
            std::vector<FileInfo>({client_file_, changed_size_client_file_}));
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));

  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_EQ(diff.missing_dirs, std::vector<DirInfo>({client_dir_}));
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));

  // These stats should be unchanged.
  EXPECT_EQ(response.num_matching_files(), 1);
  EXPECT_EQ(response.num_missing_files(), 1);
  EXPECT_EQ(response.num_changed_files(), 1);
  EXPECT_EQ(response.num_extraneous_files(), 1);

  EXPECT_EQ(response.num_matching_dirs(), 1);
  EXPECT_EQ(response.num_missing_dirs(), 1);
  EXPECT_EQ(response.num_extraneous_dirs(), 1);

  // These stats should be computed from the actual containers.
  EXPECT_EQ(response.total_changed_client_bytes(), 0);
  EXPECT_EQ(response.total_changed_server_bytes(), 0);
  EXPECT_EQ(response.total_missing_bytes(),
            client_file_.size + changed_size_client_file_.size);
}

TEST_F(FileDiffGeneratorTest, Adjust_ChecksumAndWholeFile) {
  file_diff::Result diff = MakeResultForAdjustTests();
  SendFileStatsResponse response = file_diff::AdjustToFlagsAndGetStats(
      kNoExisting, kChecksum, kWholeFile, &diff);

  // Checksum+WholeFile moves both matching and changed files to missing files.
  EXPECT_TRUE(diff.matching_files.empty());
  EXPECT_EQ(diff.missing_files,
            std::vector<FileInfo>({client_file_, changed_size_client_file_,
                                   matching_client_file_}));
  EXPECT_TRUE(diff.changed_files.empty());
  EXPECT_EQ(diff.extraneous_files, std::vector<FileInfo>({server_file_}));

  EXPECT_EQ(diff.matching_dirs, std::vector<DirInfo>({matching_client_dir_}));
  EXPECT_EQ(diff.missing_dirs, std::vector<DirInfo>({client_dir_}));
  EXPECT_EQ(diff.extraneous_dirs, std::vector<DirInfo>({server_dir_}));

  // These stats should be unchanged.
  EXPECT_EQ(response.num_matching_files(), 1);
  EXPECT_EQ(response.num_missing_files(), 1);
  EXPECT_EQ(response.num_changed_files(), 1);
  EXPECT_EQ(response.num_extraneous_files(), 1);

  EXPECT_EQ(response.num_matching_dirs(), 1);
  EXPECT_EQ(response.num_missing_dirs(), 1);
  EXPECT_EQ(response.num_extraneous_dirs(), 1);

  // These stats should be computed from the actual containers.
  EXPECT_EQ(response.total_changed_client_bytes(), 0);
  EXPECT_EQ(response.total_changed_server_bytes(), 0);
  EXPECT_EQ(response.total_missing_bytes(), client_file_.size +
                                                changed_size_client_file_.size +
                                                matching_client_file_.size);
}

}  // namespace
}  // namespace cdc_ft
