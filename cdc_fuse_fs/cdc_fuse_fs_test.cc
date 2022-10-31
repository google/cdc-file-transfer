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

// Bazel does not honor copts for test targets, so we need to define
// USE_MOCK_LIBFUSE in the test code itself *before* including cdc_fuse_fs.h.
#ifndef USE_MOCK_LIBFUSE
#define USE_MOCK_LIBFUSE = 1
#endif

#include "cdc_fuse_fs/cdc_fuse_fs.h"

#include <memory>
#include <vector>

#include "cdc_fuse_fs/mock_config_stream_client.h"
#include "cdc_fuse_fs/mock_libfuse.h"
#include "common/log.h"
#include "common/path.h"
#include "common/status_test_macros.h"
#include "data_store/mem_data_store.h"
#include "gtest/gtest.h"
#include "manifest/fake_manifest_builder.h"

namespace cdc_ft {

// FUSE callback methods. Declared here since they depend on Fuse types that
// should not be exposed in cdc_fuse_fs.h.
void CdcFuseForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);
void CdcFuseForgetMulti(fuse_req_t req, size_t count,
                        struct fuse_forget_data* forgets);
void CdcFuseGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
void CdcFuseLookup(fuse_req_t req, fuse_ino_t parent_ino, const char* name);
void CdcFuseOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
void CdcFuseOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
void CdcFuseRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info* fi);
void CdcFuseReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                    fuse_file_info* fi);
void CdcFuseRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
void CdcFuseReleaseDir(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info* fi);
size_t CdcFuseGetInodeCountForTesting();
size_t CdcFuseGetInvalidInodeCountForTesting();
void CdcFuseAccess(fuse_req_t req, fuse_ino_t ino, int mask);

namespace {

class FuseLog : public ConsoleLog {
 public:
  explicit FuseLog(LogLevel log_level) : ConsoleLog(log_level) {}

  const std::string& LastMessage() const { return last_message_; }

 protected:
  void WriteLogMessage(LogLevel level, const char* file, int line,
                       const char* func, const char* message) override {
    ConsoleLog::WriteLogMessage(level, file, line, func, message);
    last_message_ = message;
  }

 private:
  std::string last_message_;
};

class CdcFuseFsTest : public ::testing::Test {
 protected:
  static constexpr char kFile1Name[] = "file1.txt";
  static constexpr uint32_t kFile1Perm = path::MODE_IRUSR;
  static constexpr int64_t kFile1Mtime = 1ull << 40;
  const std::vector<char> kFile1Data = {'f', 'i', 'l', 'e', '1'};

  static constexpr char kFile2Name[] = "file2.txt";
  static constexpr uint32_t kFile2Perm = path::MODE_IRWXU;
  static constexpr int64_t kFile2Mtime = -kFile1Mtime;
  const std::vector<char> kFile2Data = {'H', 'e', 'l', 'l', 'o', ' ',
                                        'W', 'o', 'r', 'l', 'd', '!'};

  static constexpr char kSubdirName[] = "subdir";
  static constexpr uint32_t kSubdirPerm = path::MODE_IRUSR | path::MODE_IXUSR;
  static constexpr int64_t kSubdirMtime = 0;

  static constexpr char kWorldFile[] = "world_file.txt";
  static constexpr char kGroupFile[] = "group_file.txt";
  static constexpr char kUserFile[] = "user_file.txt";

 public:
  CdcFuseFsTest() : builder_(&cache_) {
    cdc_fuse_fs::Initialize(0, nullptr).IgnoreError();
    Log::Initialize(std::make_unique<FuseLog>(LogLevel::kInfo));
    cdc_fuse_fs::SetConfigClient(std::make_unique<MockConfigStreamClient>());
  }
  ~CdcFuseFsTest() {
    Log::Shutdown();
    cdc_fuse_fs::Shutdown();
  }

  void SetUp() override {
    // Set up an in-memory directory structure for testing.
    // - file1.txt
    // - subdir
    //    |
    //     - file2.txt
    builder_.AddFile(builder_.Root(), kFile1Name, kFile1Mtime, kFile1Perm,
                     kFile1Data);
    AssetProto* subdir = builder_.AddDirectory(builder_.Root(), kSubdirName,
                                               kSubdirMtime, kSubdirPerm);
    builder_.AddFile(subdir, kFile2Name, kFile2Mtime, kFile2Perm, kFile2Data);

    manifest_id_ = cache_.AddProto(*builder_.Manifest());
    fuse_.SetUid(internal::kCdcFuseCloudcastUid);
    fuse_.SetGid(internal::kCdcFuseCloudcastGid);

    // Note: Run(&cache_) immediately exits after setting the provider/id on
    // Windows.
    EXPECT_OK(cdc_fuse_fs::Run(&cache_, true));
    EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
    EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  }

  void TearDown() override {}

 protected:
  void ExpectAttr(const struct stat& attr, uint32_t mode, uint64_t size,
                  int64_t mtime) {
    EXPECT_NE(attr.st_ino, 0);
    EXPECT_EQ(attr.st_mode, mode);
    EXPECT_EQ(attr.st_size, size);
    EXPECT_EQ(attr.st_nlink, internal::kCdcFuseDefaultNLink);
    EXPECT_EQ(attr.st_mtime, mtime);
    EXPECT_EQ(attr.st_uid, internal::kCdcFuseCloudcastUid);
    EXPECT_EQ(attr.st_gid, internal::kCdcFuseCloudcastGid);
  }

  void ExpectAccessError(int mask, int exp_error) {
    size_t num_errors = fuse_.errors.size();
    for (size_t it = 0; it < fuse_.entries.size(); ++it, ++num_errors) {
      CdcFuseAccess(req_, fuse_.entries[it].ino, mask);
      ASSERT_EQ(fuse_.errors.size(), num_errors + 1);
      EXPECT_EQ(fuse_.errors[num_errors], exp_error);
    }
  }

  void ExpectAccessSucceeds() {
    // Each file should allow read access.
    ExpectAccessError(R_OK, 0 /*error*/);

    // Each file should allow write access.
    ExpectAccessError(W_OK, 0 /*error*/);

    // Each file should allow exec access.
    ExpectAccessError(X_OK, 0 /*error*/);

    // All files should provide all types of access.
    ExpectAccessError(R_OK | W_OK | X_OK, 0 /*error*/);
  }

  // Wipes chunks for |kFile1Name| and assets for |kSubDirName| to simulate an
  // intermediate manifest, i.e. the manifest for which some files and
  // directories are not processed yet.
  // Returns the intermediate manifest id.
  ContentIdProto CreateIntermediateManifestId() {
    ManifestProto manifest;
    EXPECT_OK(cache_.GetProto(manifest_id_, &manifest));
    EXPECT_GT(manifest.root_dir().dir_assets_size(), 0);
    if (manifest.root_dir().dir_assets_size() == 0) return ContentIdProto();

    AssetProto* file1 = manifest.mutable_root_dir()->mutable_dir_assets(0);
    EXPECT_EQ(file1->name(), kFile1Name);
    file1->clear_file_chunks();
    file1->set_in_progress(true);

    AssetProto* subdir = manifest.mutable_root_dir()->mutable_dir_assets(1);
    EXPECT_EQ(subdir->name(), kSubdirName);
    subdir->clear_dir_assets();
    subdir->set_in_progress(true);

    return cache_.AddProto(manifest);
  }

  MemDataStore cache_;
  MockLibFuse fuse_;
  fuse_req_t req_ = nullptr;
  ContentIdProto manifest_id_;
  FakeManifestBuilder builder_;
  FuseLog* Log() const { return static_cast<FuseLog*>(Log::Instance()); }
};

TEST_F(CdcFuseFsTest, LookupSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);
  EXPECT_NE(fuse_.entries[0].ino, 0);
  ExpectAttr(fuse_.entries[0].attr, kFile1Perm | path::MODE_IFREG,
             kFile1Data.size(), kFile1Mtime);
}

TEST_F(CdcFuseFsTest, LookupFailsNotADirectory) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);
  EXPECT_EQ(fuse_.errors.size(), 0);

  // The ino refers to a file, but Lookup() wants a directory.
  CdcFuseLookup(req_, fuse_.entries[0].ino, kFile1Name);
  EXPECT_EQ(fuse_.entries.size(), 1);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOENT);
}

TEST_F(CdcFuseFsTest, LookupFailsDoesNotExist) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, "does_not_exist");
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOENT);
}

TEST_F(CdcFuseFsTest, GetAttrSucceedsRootDir) {
  fuse_file_info fi;
  CdcFuseGetAttr(req_, FUSE_ROOT_ID, &fi);
  ASSERT_EQ(fuse_.attrs.size(), 1);
  EXPECT_EQ(fuse_.attrs[0].timeout, internal::kCdcFuseInodeTimeoutSec);
  ExpectAttr(fuse_.attrs[0].value,
             FakeManifestBuilder::kRootDirPerms | path::MODE_IFDIR, 0, 0);
}

TEST_F(CdcFuseFsTest, GetAttrSucceedsFile) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  fuse_file_info fi;
  CdcFuseGetAttr(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.attrs.size(), 1);
  EXPECT_EQ(fuse_.attrs[0].timeout, internal::kCdcFuseInodeTimeoutSec);
  ExpectAttr(fuse_.attrs[0].value, kFile1Perm | path::MODE_IFREG,
             kFile1Data.size(), kFile1Mtime);
}

TEST_F(CdcFuseFsTest, OpenSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  fuse_file_info fi;
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.open_files.size(), 1);
}

TEST_F(CdcFuseFsTest, OpenRespectsODirect) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  fuse_file_info fi;
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);
  fi.flags = O_DIRECT;
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);

  ASSERT_EQ(fuse_.open_files.size(), 2);

  ASSERT_FALSE(fuse_.open_files[0].direct_io);
  ASSERT_TRUE(fuse_.open_files[1].direct_io);

  ASSERT_TRUE(fuse_.open_files[0].keep_cache);
  ASSERT_FALSE(fuse_.open_files[1].keep_cache);
}

TEST_F(CdcFuseFsTest, OpenFailsDirectory) {
  fuse_file_info fi;
  CdcFuseOpen(req_, FUSE_ROOT_ID, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EISDIR);
}

TEST_F(CdcFuseFsTest, OpenFailsWriteAccess) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  fuse_file_info fi(O_RDWR);
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EACCES);
}

TEST_F(CdcFuseFsTest, RequestsQueuedForIntermediateManifest) {
  ContentIdProto intermediate_manifest_id = CreateIntermediateManifestId();
  EXPECT_OK(cdc_fuse_fs::SetManifest(intermediate_manifest_id));
  fuse_file_info fi;
  auto cfg_client_ptr = std::make_unique<MockConfigStreamClient>();
  MockConfigStreamClient* cfg_client = cfg_client_ptr.get();
  cdc_fuse_fs::SetConfigClient(std::move(cfg_client_ptr));

  // Opening file1 should be queued as it is marked as in-progress.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);
  EXPECT_EQ(fuse_.open_files.size(), 0);
  EXPECT_EQ(cfg_client->ReleasePrioritizedAssets(),
            std::vector<std::string>({kFile1Name}));

  // Opening subdir should be queued as it is marked as in-progress.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 2);
  CdcFuseOpenDir(req_, fuse_.entries[1].ino, &fi);
  EXPECT_EQ(fuse_.open_files.size(), 0);
  EXPECT_EQ(cfg_client->ReleasePrioritizedAssets(),
            std::vector<std::string>({kSubdirName}));

  // Setting the final manifest should fulfill queued open requests.
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ(fuse_.open_files.size(), 2);
}

TEST_F(CdcFuseFsTest, QueuedRequestsRequeue) {
  ContentIdProto intermediate_manifest_id = CreateIntermediateManifestId();
  EXPECT_OK(cdc_fuse_fs::SetManifest(intermediate_manifest_id));
  fuse_file_info fi;

  // Opening file1 should be queued as it is marked as in-progress
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);
  CdcFuseOpen(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.open_files.size(), 0);

  // Opening subdir should be queued as it is marked as in-progress.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 2);
  CdcFuseOpenDir(req_, fuse_.entries[1].ino, &fi);
  EXPECT_EQ(fuse_.open_files.size(), 0);

  // Setting the same incomplete manifest again should requeue the request.
  EXPECT_OK(cdc_fuse_fs::SetManifest(intermediate_manifest_id));
  ASSERT_EQ(fuse_.open_files.size(), 0);

  // Setting the final manifest should fulfill queued open requests.
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  ASSERT_EQ(fuse_.open_files.size(), 2);
}

TEST_F(CdcFuseFsTest, ReadSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  // Read everything from file1 except the first and the last byte.
  fuse_file_info fi;
  CdcFuseRead(req_, fuse_.entries[0].ino, kFile1Data.size() - 2, 1, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  std::vector<char> data(kFile1Data.begin() + 1, kFile1Data.end() - 1);
  EXPECT_EQ(fuse_.buffers[0], data);
}

TEST_F(CdcFuseFsTest, ReadFailsNotAFile) {
  fuse_file_info fi;
  CdcFuseRead(req_, FUSE_ROOT_ID, kFile1Data.size(), 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EIO);
}

TEST_F(CdcFuseFsTest, ReadDirSucceeds) {
  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  fuse_file_info fi;
  CdcFuseReadDir(req_, FUSE_ROOT_ID, kEntrySize * 10, 0, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  ASSERT_EQ(fuse_.buffers[0].size(), kEntrySize * 4);  // ., .., file1, subdir
  MockLibFuse::DirEntry* entries =
      reinterpret_cast<MockLibFuse::DirEntry*>(fuse_.buffers[0].data());

  // Get inos for "file1.txt" and "subdir".
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 2);

  std::unordered_map<std::string, MockLibFuse::DirEntry> expected;
  expected["."] = {FUSE_ROOT_ID,
                   FakeManifestBuilder::kRootDirPerms | path::MODE_IFDIR,
                   {0},
                   0};
  expected[".."] = {FUSE_ROOT_ID,
                    FakeManifestBuilder::kRootDirPerms | path::MODE_IFDIR,
                    {0},
                    0};
  expected[kFile1Name] = {
      fuse_.entries[0].ino & 0xFFFF, kFile1Perm | path::MODE_IFREG, {0}, 0};
  expected[kSubdirName] = {
      fuse_.entries[1].ino & 0xFFFF, kSubdirPerm | path::MODE_IFDIR, {0}, 0};

  // A couple of things to note:
  // - ReadDir() only fills the ino and mode attr entries. This is expected
  // and matches what libfuse uses. The filesystem actually GetAttr() on every
  // entry to get the full attributes. This has been fixed by ReadDirPlus()
  // in LibFuse 3.
  // - ".." is assigned the |FUSE_ROOT_ID| (this works fine, trust me!).
  // - Unfortunately, on Windows stat::st_ino is 16 bit and there seems to be
  // no good way to make it 64 bit. On Linux, we assert 64 bits, though.

  std::unordered_set<std::string> unique_assets;
  for (size_t i = 0; i < 4; ++i) {
    auto it = expected.find(entries[i].name);
    ASSERT_NE(it, expected.end());
    EXPECT_EQ(entries[i].ino & 0xFFFF, it->second.ino);
    EXPECT_EQ(entries[i].mode, it->second.mode);
    unique_assets.insert(entries[i].name);
  }
  EXPECT_EQ(unique_assets.size(), expected.size());
}

TEST_F(CdcFuseFsTest, ReadDirWithOffsetSizeSucceeds) {
  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  fuse_file_info fi;
  CdcFuseReadDir(req_, FUSE_ROOT_ID, kEntrySize * 2, kEntrySize, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  ASSERT_EQ(fuse_.buffers[0].size(), kEntrySize * 2);
  MockLibFuse::DirEntry* entries =
      reinterpret_cast<MockLibFuse::DirEntry*>(fuse_.buffers[0].data());

  std::unordered_set<std::string> known_assets;
  known_assets.insert(".");
  known_assets.insert("..");
  known_assets.insert(kFile1Name);
  known_assets.insert(kSubdirName);
  for (size_t i = 0; i < 2; ++i)
    EXPECT_NE(known_assets.find(entries[i].name), known_assets.end())
        << "Could not find " << entries[i].name;
}

TEST_F(CdcFuseFsTest, ReadDirBeyondEofSucceeds) {
  // Start reading at entry 7, but there are only 6 entries.
  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  fuse_file_info fi;
  CdcFuseReadDir(req_, FUSE_ROOT_ID, kEntrySize * 14, kEntrySize * 7, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  EXPECT_TRUE(fuse_.buffers[0].empty());
}

TEST_F(CdcFuseFsTest, ReadDirFailsNotADirectory) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  fuse_file_info fi;
  CdcFuseReadDir(req_, fuse_.entries[0].ino, 1, 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOTDIR);
}

TEST_F(CdcFuseFsTest, ReadDirFailsInvalidIndirectAssetList) {
  FakeManifestBuilder builder(&cache_);
  ContentIdProto invalid_id;
  *builder.Root()->add_dir_indirect_assets() = invalid_id;
  ContentIdProto root_id = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(root_id));

  fuse_file_info fi;
  CdcFuseReadDir(req_, FUSE_ROOT_ID, 1, 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EBADF);
}

TEST_F(CdcFuseFsTest, Forget) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  EXPECT_EQ(fuse_.entries.size(), 1u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);

  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  // No new entry should be created as forget() finishes with
  // fuse_reply_none().
  EXPECT_EQ(fuse_.entries.size(), 1u);
  EXPECT_TRUE(fuse_.errors.empty());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(fuse_.none_counter, 1u);
}

TEST_F(CdcFuseFsTest, ForgetMulti) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 2);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  fuse_forget_data nodes_to_forget[2];
  nodes_to_forget[0].ino = fuse_.entries[0].ino;
  nodes_to_forget[0].nlookup = 1;
  nodes_to_forget[1].ino = fuse_.entries[1].ino;
  nodes_to_forget[1].nlookup = 1;
  CdcFuseForgetMulti(req_, 2u, &nodes_to_forget[0]);

  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(fuse_.none_counter, 1u);
}

TEST_F(CdcFuseFsTest, DoNotForgetRoot) {
  CdcFuseForget(req_, FUSE_ROOT_ID, 1u);
  EXPECT_EQ(fuse_.none_counter, 1u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
}

TEST_F(CdcFuseFsTest, DoNotForgetParent) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);

  CdcFuseLookup(req_, fuse_.entries[0].ino, kFile2Name);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  ASSERT_EQ(fuse_.entries.size(), 2);

  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 1u);

  // The inode for kFile2Name still holds a reference to its parent.
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  // Unreal scenario: second forget for parent.
  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 2u);

  // The inode for kFile2Name still holds a reference to its parent.
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);
}

TEST_F(CdcFuseFsTest, ForgetAllParentsExceptRoot) {
  FakeManifestBuilder builder(&cache_);
  builder.AddDirectory(builder.Root(), kFile1Name, kFile1Mtime, kFile1Perm);
  AssetProto* subdir1 = builder.AddDirectory(builder.Root(), kSubdirName,
                                             kSubdirMtime, kSubdirPerm);
  AssetProto* subdir2 =
      builder.AddDirectory(subdir1, kSubdirName, kSubdirMtime, kSubdirPerm);
  AssetProto* subdir3 =
      builder.AddDirectory(subdir2, kSubdirName, kSubdirMtime, kSubdirPerm);
  builder.AddFile(subdir3, kFile2Name, kFile2Mtime, kFile2Perm, kFile2Data);
  EXPECT_OK(cdc_fuse_fs::SetManifest(cache_.AddProto(*builder.Manifest())));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 1);
  CdcFuseLookup(req_, fuse_.entries[0].ino, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 2);
  CdcFuseLookup(req_, fuse_.entries[1].ino, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 3);
  CdcFuseLookup(req_, fuse_.entries[2].ino, kFile2Name);
  ASSERT_EQ(fuse_.entries.size(), 4);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 4u);

  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 1u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 4u);

  CdcFuseForget(req_, fuse_.entries[1].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 2u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 4u);

  CdcFuseForget(req_, fuse_.entries[2].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 3u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 4u);

  CdcFuseForget(req_, fuse_.entries[3].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 4u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
}

TEST_F(CdcFuseFsTest, AccessToReadSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // Only read access for a specific rights collection.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRUSR,
                  kFile1Data);
  builder.AddFile(builder.Root(), kGroupFile, kFile1Mtime, path::MODE_IRGRP,
                  kFile1Data);
  builder.AddFile(builder.Root(), kWorldFile, kFile1Mtime, path::MODE_IROTH,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kWorldFile);
  EXPECT_EQ(fuse_.entries.size(), 3u);

  // Each file should allow read access.
  ExpectAccessError(R_OK, 0 /*error*/);

  // No file should provide write access.
  ExpectAccessError(W_OK, EACCES /*error*/);

  // No file should provide exec access.
  ExpectAccessError(X_OK, EACCES /*error*/);

  // No file should provide all types of access.
  ExpectAccessError(R_OK | W_OK | X_OK, EACCES /*error*/);
}

TEST_F(CdcFuseFsTest, AccessToAllRightsSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // All access rights for a specific rights collection.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                  kFile1Data);
  builder.AddFile(builder.Root(), kGroupFile, kFile1Mtime, path::MODE_IRWXG,
                  kFile1Data);
  builder.AddFile(builder.Root(), kWorldFile, kFile1Mtime, path::MODE_IRWXO,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kWorldFile);
  EXPECT_EQ(fuse_.entries.size(), 3u);

  ExpectAccessSucceeds();
}

TEST_F(CdcFuseFsTest, AccessFailsWrongUser) {
  FakeManifestBuilder builder(&cache_);

  // Only the default user has all rights.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  // The user does not have rights to access the file.
  // Only root and default users have access.
  fuse_.SetUid(100);

  ExpectAccessError(R_OK, EACCES /*error*/);
}

TEST_F(CdcFuseFsTest, AccessFailsWrongGroup) {
  FakeManifestBuilder builder(&cache_);

  // Only the users of the default group have all rights.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXG,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  fuse_.SetGid(100);

  // The user does not have rights to access the file.
  ExpectAccessError(R_OK, EACCES /*error*/);
}

TEST_F(CdcFuseFsTest, AccessAsRootUserSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // No rights are set.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, 0, kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  fuse_.SetUid(internal::kCdcFuseRootUid);

  ExpectAccessSucceeds();
}

TEST_F(CdcFuseFsTest, AccessAsRootGroupSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // No rights are set.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, 0, kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());

  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  fuse_.SetGid(internal::kCdcFuseRootGid);

  ExpectAccessSucceeds();
}

TEST_F(CdcFuseFsTest, SetInvalidManifestFails) {
  ContentIdProto invalid_manifest_id;
  absl::Status status = cdc_fuse_fs::SetManifest(invalid_manifest_id);
  EXPECT_NOT_OK(status);

  // The old manifest still should be valid: its files should be requestable.
  ExpectAccessSucceeds();
}

TEST_F(CdcFuseFsTest, AddFileUpdateManifestSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // All access rights for a specific rights collection.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  // The file does not exists -> error.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOENT);

  // Add the missing file and update the manifest id.
  builder.AddFile(builder.Root(), kGroupFile, kFile1Mtime, path::MODE_IRWXG,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  // Can get access to the first file.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 2u);

  // Can get access to the new file.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  EXPECT_EQ(fuse_.entries.size(), 3u);
}

TEST_F(CdcFuseFsTest, CompletelyUpdatedManifestSucceeds) {
  FakeManifestBuilder builder(&cache_);

  // All access rights for a specific rights collection.
  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  FakeManifestBuilder builder2(&cache_);
  // Add the missing file and update the manifest id.
  builder2.AddFile(builder2.Root(), kGroupFile, kFile1Mtime, path::MODE_IRWXG,
                   kFile1Data);
  manifest_id_ = cache_.AddProto(*builder2.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  // Cannot get access to the old file.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOENT);

  // Can get access to the new file.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  EXPECT_EQ(fuse_.entries.size(), 2u);
}

TEST_F(CdcFuseFsTest, CompletelyUpdatedManifestForgetOldFileSucceeds) {
  FakeManifestBuilder builder(&cache_);

  builder.AddFile(builder.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 1u);

  FakeManifestBuilder builder2(&cache_);
  // Add file and update the manifest id.
  builder2.AddFile(builder2.Root(), kGroupFile, kFile1Mtime, path::MODE_IRWXG,
                   kFile1Data);
  manifest_id_ = cache_.AddProto(*builder2.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 1u);

  // Cannot get access to the old file.
  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  EXPECT_EQ(fuse_.none_counter, 1u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 0u);

  // Can get access to the new file.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kGroupFile);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);
  EXPECT_EQ(fuse_.entries.size(), 2u);
}

TEST_F(CdcFuseFsTest, AddFileUpdateManifestOldInodesValid) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  // Update manifest while adding a new file.
  builder_.AddFile(builder_.Root(), kUserFile, kFile1Mtime, path::MODE_IRWXU,
                   kFile1Data);
  manifest_id_ = cache_.AddProto(*builder_.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // New file should be accessible.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kUserFile);
  EXPECT_EQ(fuse_.entries.size(), 2u);

  // inode for kFile1Name should be valid.
  fuse_file_info fi;
  CdcFuseRead(req_, fuse_.entries[0].ino, kFile1Data.size(), 0, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  EXPECT_EQ(fuse_.buffers[0], kFile1Data);
}

TEST_F(CdcFuseFsTest, ModifyFileUpdateManifestOldInodesValid) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  // Update manifest by modifying a file.
  builder_.ModifyFile(builder_.Root(), kFile1Name, kFile2Mtime, kFile2Perm,
                      kFile2Data);
  manifest_id_ = cache_.AddProto(*builder_.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 0u);

  // inode for kFile1Name should be valid, but the content should be new ->
  // reload is required.
  fuse_file_info fi;
  CdcFuseRead(req_, fuse_.entries[0].ino, kFile2Data.size(), 0, &fi);
  EXPECT_TRUE(fuse_.buffers.empty());
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EIO);

  // Forget the inode, lookup + read it again -> should succeed.
  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(fuse_.none_counter, 1u);

  // Read it again.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ExpectAttr(fuse_.entries[1].attr, kFile2Perm | path::MODE_IFREG,
             kFile2Data.size(), kFile2Mtime);
  CdcFuseRead(req_, fuse_.entries[1].ino, kFile2Data.size(), 0, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  EXPECT_EQ(fuse_.buffers[0], kFile2Data);
}

TEST_F(CdcFuseFsTest,
       LookupFileInSubfolderRemoveSubfolderUpdateManifestReadFile) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  CdcFuseLookup(req_, fuse_.entries[1].ino, kFile2Name);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 3u);
  ASSERT_EQ(fuse_.entries.size(), 3);

  // Update manifest: it has only 1 file.
  FakeManifestBuilder builder(&cache_);
  builder.AddFile(builder.Root(), kFile1Name, kFile1Mtime, kFile1Perm,
                  kFile1Data);

  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // The total amount of valid and invalid inodes should stay the same.
  // As the old inodes could be potentially accessed by the system and should be
  // forgotten with forget() or forget_multi().
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 2u);

  fuse_file_info fi;
  CdcFuseRead(req_, fuse_.entries[2].ino, kFile2Data.size(), 0, &fi);
  EXPECT_TRUE(fuse_.buffers.empty());
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOENT);

  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  CdcFuseReadDir(req_, fuse_.entries[1].ino, kEntrySize * 10, 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 2u);
  EXPECT_EQ(fuse_.errors[1], ENOENT);
}

TEST_F(CdcFuseFsTest, FileToFolderUpdateManifest) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1);

  // Read everything from file1 -> fill internal asset's structures.
  fuse_file_info file_info;
  CdcFuseRead(req_, fuse_.entries[0].ino, kFile1Data.size(), 0, &file_info);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  EXPECT_EQ(fuse_.buffers[0], kFile1Data);

  // Change file1.txt to folder.
  FakeManifestBuilder builder(&cache_);
  builder.AddDirectory(builder.Root(), kFile1Name, kFile1Mtime, kFile1Perm);
  AssetProto* subdir = builder.AddDirectory(builder.Root(), kSubdirName,
                                            kSubdirMtime, kSubdirPerm);
  builder.AddFile(subdir, kFile2Name, kFile2Mtime, kFile2Perm, kFile2Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // Read directory should succeed.
  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  fuse_file_info dir_info;
  CdcFuseReadDir(req_, fuse_.entries[0].ino, kEntrySize * 10, 0, &dir_info);
  ASSERT_EQ(fuse_.buffers.size(), 2);
  ASSERT_EQ(fuse_.buffers[1].size(), kEntrySize * 2);  // ., ..
  MockLibFuse::DirEntry* entries =
      reinterpret_cast<MockLibFuse::DirEntry*>(fuse_.buffers[1].data());
  EXPECT_STREQ(entries[0].name, ".");
  EXPECT_STREQ(entries[1].name, "..");
}

TEST_F(CdcFuseFsTest, FolderToFileUpdateManifest) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);

  // Read directory to fill internal asset's structures.
  const size_t kEntrySize = sizeof(MockLibFuse::DirEntry);
  fuse_file_info dir_info;
  CdcFuseReadDir(req_, fuse_.entries[0].ino, kEntrySize * 10, 0, &dir_info);
  ASSERT_EQ(fuse_.buffers.size(), 1);
  ASSERT_EQ(fuse_.buffers[0].size(), kEntrySize * 3);  // ., .., file2.txt
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  // Get inos for "file2.txt".
  CdcFuseLookup(req_, fuse_.entries[0].ino, kFile2Name);
  ASSERT_EQ(fuse_.entries.size(), 2);

  // Change subfolder to file.
  FakeManifestBuilder builder(&cache_);
  builder.AddFile(builder.Root(), kSubdirName, kSubdirMtime, kSubdirPerm,
                  kFile1Data);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(),
            1u);  // the number of inodes should not change.

  // Reading file should fail as the proto has been changed.
  fuse_file_info fi;
  CdcFuseRead(req_, fuse_.entries[0].ino, kFile1Data.size(), 0, &fi);
  EXPECT_EQ(fuse_.buffers.size(), 1);  // should not change.
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], EIO);

  // Forget the inode, lookup + read it again -> should succeed.
  CdcFuseForget(req_, fuse_.entries[0].ino, 1u);
  CdcFuseForget(req_, fuse_.entries[1].ino, 2u);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(fuse_.none_counter, 2u);

  // Read it again.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  CdcFuseRead(req_, fuse_.entries[2].ino, kFile1Data.size(), 0, &fi);
  ASSERT_EQ(fuse_.buffers.size(), 2);
  EXPECT_EQ(fuse_.buffers[1], kFile1Data);
}

TEST_F(CdcFuseFsTest, ModifyFileUpdateManifestTwiceOldInodesValid) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  // Update manifest by modifying a file.
  builder_.ModifyFile(builder_.Root(), kFile1Name, kFile2Mtime, kFile2Perm,
                      kFile2Data);
  manifest_id_ = cache_.AddProto(*builder_.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // Lookup should return the same ino, but different attributes.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 2u);
  EXPECT_EQ(fuse_.entries[1].ino, fuse_.entries[0].ino);
  ExpectAttr(fuse_.entries[1].attr, kFile2Perm | path::MODE_IFREG,
             kFile2Data.size(), kFile2Mtime);

  // Update the file and manifest second time.
  builder_.ModifyFile(builder_.Root(), kFile1Name, kFile1Mtime, kFile1Perm,
                      kFile1Data);
  manifest_id_ = cache_.AddProto(*builder_.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // Lookup should return the same ino, but different attributes.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 3u);
  EXPECT_EQ(fuse_.entries[2].ino, fuse_.entries[1].ino);
  ExpectAttr(fuse_.entries[2].attr, kFile1Perm | path::MODE_IFREG,
             kFile1Data.size(), kFile1Mtime);
}

TEST_F(CdcFuseFsTest, RemoveFolderUpdateManifest) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  CdcFuseLookup(req_, fuse_.entries[0].ino, kFile2Name);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  // Remove subfolder.
  FakeManifestBuilder builder(&cache_);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 2u);
}

TEST_F(CdcFuseFsTest, InvalidateSubSubDirInvalidateSubDirSucceeds) {
  // Create a subdir in a subdir of the root.
  FakeManifestBuilder builder(&cache_);
  AssetProto* subdir = builder.AddDirectory(builder.Root(), kSubdirName,
                                            kSubdirMtime, kSubdirPerm);
  builder.AddDirectory(subdir, kSubdirName, kSubdirMtime, kSubdirPerm);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  // Both subdirs are in inodes.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  CdcFuseLookup(req_, fuse_.entries[0].ino, kSubdirName);
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 2u);

  // Update manifest while removing subsubdir.
  FakeManifestBuilder builder1(&cache_);
  builder1.AddDirectory(builder1.Root(), kSubdirName, kSubdirMtime,
                        kSubdirPerm);
  manifest_id_ = cache_.AddProto(*builder1.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 1u);

  // Update manifest while removing subdir.
  FakeManifestBuilder builder2(&cache_);
  manifest_id_ = cache_.AddProto(*builder2.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 0u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 2u);

  fuse_file_info fi;
  CdcFuseReadDir(req_, fuse_.entries[0].ino, 1, 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1);
  EXPECT_EQ(fuse_.errors[0], ENOENT);

  CdcFuseReadDir(req_, fuse_.entries[1].ino, 1, 0, &fi);
  ASSERT_EQ(fuse_.errors.size(), 2);
  EXPECT_EQ(fuse_.errors[1], ENOENT);
}

TEST_F(CdcFuseFsTest, OpenDirSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseOpenDir(req_, fuse_.entries[0].ino, &fi);
  EXPECT_TRUE(fuse_.errors.empty());
  ASSERT_EQ(fuse_.open_files.size(), 1u);
}

TEST_F(CdcFuseFsTest, OpenDirFailsNotADirectory) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseOpenDir(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOTDIR);
}

TEST_F(CdcFuseFsTest, OpenDirFailsInvalidDir) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  // Remove subdir.
  FakeManifestBuilder builder(&cache_);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  fuse_file_info fi;
  CdcFuseOpenDir(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOENT);
}

TEST_F(CdcFuseFsTest, ReleaseSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseRelease(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], 0u);
}

TEST_F(CdcFuseFsTest, ReleaseFailsForDirectory) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseRelease(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], EISDIR);
}

TEST_F(CdcFuseFsTest, ReleaseFailsInvalidFile) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);

  // Remove file.
  FakeManifestBuilder builder(&cache_);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  fuse_file_info fi;
  CdcFuseRelease(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOENT);
}

TEST_F(CdcFuseFsTest, ReleaseDirSucceeds) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseRelease(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], 0u);
}

TEST_F(CdcFuseFsTest, ReleaseDirFailsNotADirectory) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  ASSERT_EQ(fuse_.entries.size(), 1u);

  fuse_file_info fi;
  CdcFuseReleaseDir(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOTDIR);
}

TEST_F(CdcFuseFsTest, ReleaseDirFailsInvalidDirectory) {
  // Get inode.
  CdcFuseLookup(req_, FUSE_ROOT_ID, kSubdirName);

  // Remove subdir.
  FakeManifestBuilder builder(&cache_);
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));
  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());

  fuse_file_info fi;
  CdcFuseReleaseDir(req_, fuse_.entries[0].ino, &fi);
  ASSERT_EQ(fuse_.errors.size(), 1u);
  EXPECT_EQ(fuse_.errors[0], ENOENT);
}

TEST_F(CdcFuseFsTest, UpdateManifestEmptyFile) {
  CdcFuseLookup(req_, FUSE_ROOT_ID, kFile1Name);
  EXPECT_EQ(fuse_.entries.size(), 1u);
  FakeManifestBuilder builder(&cache_);

  builder.AddFile(builder.Root(), kFile1Name, kFile1Mtime, path::MODE_IRWXU,
                  {});
  manifest_id_ = cache_.AddProto(*builder.Manifest());
  EXPECT_OK(cdc_fuse_fs::SetManifest(manifest_id_));

  EXPECT_EQ("FUSE consistency check succeeded", Log()->LastMessage());
  EXPECT_EQ(CdcFuseGetInodeCountForTesting(), 1u);
  EXPECT_EQ(CdcFuseGetInvalidInodeCountForTesting(), 0u);
}

}  // namespace
}  // namespace cdc_ft
