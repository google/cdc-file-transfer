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

#ifndef COMMON_PATH_H_
#define COMMON_PATH_H_

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "common/platform.h"

#if PLATFORM_LINUX
#define feof_nolock feof_unlocked
#define fread_nolock fread_unlocked
#define fseek64 fseeko
#define fseek64_nolock fseeko
#define ftell64 ftello
#define fwrite_nolock fwrite_unlocked
#elif PLATFORM_WINDOWS
#define feof_nolock feof
#define fread_nolock _fread_nolock
#define fseek64 _fseeki64
#define fseek64_nolock _fseeki64_nolock
#define ftell64 _ftelli64
#define fwrite_nolock _fwrite_nolock
#endif

namespace cdc_ft {

class Buffer;

namespace path {

// Returns the native path separator character for this platform.
inline char PathSeparator() {
#ifdef PLATFORM_WINDOWS
  return '\\';
#else
  return '/';
#endif
}

// Returns the non-native path separator character for this platform.
inline char OtherPathSeparator() {
#ifdef PLATFORM_WINDOWS
  return '/';
#else
  return '\\';
#endif
}

// Closes the given FILE pointer once the object is deleted.
class FileCloser {
 public:
  explicit FileCloser(FILE* f) : fp_(f) {}
  ~FileCloser() {
    if (fp_) fclose(fp_);
  }

 private:
  FILE* fp_;
};

// All file paths are assumed to be UTF-8 encoded.

// Returns the |dir| that contains the current executable.
// The returned string does not have a trailing path separator.
absl::Status GetExeDir(std::string* dir);

// Returns a directory suitable for temporary files. The path is guaranteed to
// exist and to be a directory.
std::string GetTempDir();

#if PLATFORM_WINDOWS
enum class FolderId {
  // E.g. C:\Users\jdoe\AppData\Roaming.
  kRoamingAppData,
  // E.g. C:\Program Files.
  kProgramFiles
};

// Returns the Windows known folder path for the given |folder_id|.
absl::Status GetKnownFolderPath(FolderId folder_id, std::string* path);
#endif

// Expands environment path variables like %APPDATA% on Windows or ~ on Linux.
// On Windows, variables are matched case invariantly. Unknown environment
// variables are not changed.
// On Linux, performs a shell-like expansion. Returns an error if multiple
// results would be returned, e.g. from *.txt.
absl::Status ExpandPathVariables(std::string* path);

// Returns the environment variable with given |name| in |value|.
// Returns a NotFound error and sets |value| to an empty string if the variable
// does not exist.
absl::Status GetEnv(const std::string& name, std::string* value);

// Sets the environment variable with given |name| to |value|. Only affects the
// current process, not system environment variables nor environment variables
// of other processes.
absl::Status SetEnv(const std::string& name, const std::string& value);

#if PLATFORM_WINDOWS
// Returns the part before the actual directory path without trailing path
// separator, i.e.
// - the drive letter "C:" for "C:\path\to\file" or
// - the network share "\\computer\share" for "\\computer\share\path\to\file".
// Returns an empty string if there is no such prefix (e.g. relative paths).
std::string GetDrivePrefix(const std::string& path);
#endif

// Gets the current working directory.
std::string GetCwd();

// Sets the current working directory.
void SetCwd(const std::string& path);

// Expands a relative path to an absolute path (relative to the current working
// directory). Also canonicalizes the path, removing any . and .. elements.
// Note that if the path does not exist or contains invalid characters, it may
// only be partially canonicalized, see
// https://en.cppreference.com/w/cpp/filesystem/canonical.
std::string GetFullPath(const std::string& path);

// Returns true if the given |path| is an absolute path.
bool IsAbsolute(const std::string& path);

// Returns the parent directory part of a |path|, without the trailing path
// separator. For instance, for "foo/bar", "foo/bar/", "foo\bar" or "foo\bar\",
// "foo" is returned.
// On Windows, if |path| only consists of a drive prefix, then the drive prefix
// is returned (see GetDrivePrefix()). If the path only has one component "foo",
// an empty string is returned.
std::string DirName(const std::string& path);

// Returns the last component of |path|. For instance, for "foo/bar",
// "foo/bar/", or "foo\bar", only "bar" is returned.
// If the path only has one component "bar", that component is returned.
std::string BaseName(const std::string& path);

// Joins |path| and |to_append|, adding a path separator if necessary.
std::string Join(absl::string_view path, absl::string_view to_append);

// Joins |path| and |to_append*|, adding a path separator if necessary.
std::string Join(absl::string_view path, absl::string_view to_append1,
                 absl::string_view to_append2);

// Joins |path| and |to_append*|, adding a path separator if necessary.
std::string Join(absl::string_view path, absl::string_view to_append1,
                 absl::string_view to_append2, absl::string_view to_append3);

// Joins |path| and |to_append|, adding a path separator if necessary, and
// stores the result in |dest|. |dest| must not overlap with |path| or
// |to_append|.
void Join(std::string* dest, absl::string_view path,
          absl::string_view to_append);

// Joins |path| and |to_append|, adding a Unix path separator if necessary.
std::string JoinUnix(absl::string_view path, absl::string_view to_append);

// Appends |path| to |dest|, adding a path separator if necessary.
void Append(std::string* dest, absl::string_view to_append);

// Appends |path| to |dest|, adding a Unix path separator if necessary.
void AppendUnix(std::string* dest, absl::string_view to_append);

// Returns true if |path| ends with '\' or '//'.
bool EndsWithPathSeparator(const std::string& path);

// Adds a path separator at the end if there is none yet.
void EnsureEndsWithPathSeparator(std::string* path);

// Removes all path separators at the end if there are any.
void EnsureDoesNotEndWithPathSeparator(std::string* path);

// Converts path separators to the current platform's version, e.g. \ to /
// on Linux.
void FixPathSeparators(std::string* path);

// Converts a Windows path having backslashes as directory separators to a Unix
// path with forward slashes. A leading Windows drive letter like "C:" will be
// unchanged. For example: "C:\foo\bar" is converted to "C:/foo/bar".
std::string ToUnix(std::string path);

// Converts a path (eg, Windows or Unix path) to current system path.
// Substitutes forward slashes with backward slashes for Windows,
// substitutes backward slashes with forward slashes for Unix.
// A leading Windows drive letter like "C:" will be unchanged.
std::string ToNative(std::string path);

// Opens a file for the given |mode|.
absl::StatusOr<FILE*> OpenFile(const std::string& path, const char* mode);

using SearchHandler = std::function<absl::Status(
    const std::string& dir, const std::string& filename, int64_t modified_time,
    uint64_t size, bool is_dir)>;

// Searches all files and directories for which the path matches |pattern|.
//
// On Windows, if |pattern| is a directory path, finds that directory,
// traverses it, and finds all files and subdirectories in that directory.
// If |pattern| is a file path, finds that file. If |pattern| is
// a directory plus a file name pattern, e.g. "C:\files\*.t?t", returns all
// files and directories in that directory for which the path names match
// the pattern. Directory wildcards are not supported. A directory ending with
// "\" is treated like "\*", so it will not find the base directory, only
// subdirectories.
//
// On Linux, |pattern| must be a directory name and the function returns all
// files and directories in that directory. Glob-style patterns or file name
// matchings are not supported.
//
// If |recursive| is true, recurses into subdirectories. Does best effort,
// errors accessing directories does not stop the search, but prints out
// warnings (for access denied) or errors.
//
// Calls |handler| for every file and directory found. If the |handler| returns
// an error, the search is interrupted and the method returns that error.
// |modified_time| passed to |handler| is UTC time.
absl::Status SearchFiles(const std::string& pattern, bool recursive,
                         SearchHandler handler);

using StreamReadFileHandler =
    std::function<absl::Status(const void* data, size_t data_size)>;

// Reads |file| in chunks of size |buffer_size| and calls |handler|. On EOF,
// calls handler(nullptr, 0). If |handler| returns an error, reading stops
// and the function returns that error. Also returns an error if a read fails.
absl::Status StreamReadFileContents(FILE* file, size_t buffer_size,
                                    StreamReadFileHandler handler);

// Same as above, but uses the pre-allocated memory in |buffer|.
absl::Status StreamReadFileContents(FILE* file, Buffer* buffer,
                                    StreamReadFileHandler handler);

using StreamWriteFileHandler =
    std::function<absl::Status(const void** data, size_t* data_size)>;

// Calls |handler| and writes the data it returns to |file|. If |handler| sets
// (data, size) to (nullptr, 0) and returns ok, the function returns ok. If
// |handler| returns an error, the function returns that error. Also returns
// an error if a write fails.
absl::Status StreamWriteFileContents(FILE* file,
                                     StreamWriteFileHandler handler);

// Reads the contents of the file at |path| to |data|.
absl::Status ReadFile(const std::string& path, Buffer* data);

// Reads at most |len| bytes starting at |offset| of the file
// at |path| into |data|. Returns the number of read bytes.
absl::StatusOr<size_t> ReadFile(const std::string& path, void* data,
                                size_t offset, size_t len);

// Reads the contents of the file at |path| and returns it as string.
absl::StatusOr<std::string> ReadFile(const std::string& path);

enum class ReadFlags {
  kNone = 0,                 // Read all lines.
  kTrimWhitespace = 1 << 0,  // Trim whitespace from each line.
  kRemoveEmpty = 1 << 1      // Remove empty lines (after optional trim).
};

// Reads the contents of the file at |path| line-by-line to |lines|.
absl::Status ReadAllLines(const std::string& path,
                          std::vector<std::string>* lines, ReadFlags flags);

// Writes |data| of |len| bytes to the file at |path|. If the file does not
// exist, it is created. Otherwise, its content is discarded.
absl::Status WriteFile(const std::string& path, const void* data, size_t len);

// Writes |data| to the file at |path|.
absl::Status WriteFile(const std::string& path, const Buffer& data);

// Writes |data| to the file at |path|.
absl::Status WriteFile(const std::string& path, const std::string& data);

// Creates a symlink at |link_path| pointing to |target|.
// |is_dir| should be true if |target| is a directory, otherwise false.
absl::Status CreateSymlink(const std::string& target,
                           const std::string& link_path, bool is_dir);

// Retrieves a symlink target at |link_path|.
absl::StatusOr<std::string> GetSymlinkTarget(const std::string& link_path);

// Checks the directory at |path| exists.
bool DirExists(const std::string& path);

// Checks the file or directory at |path| exists.
bool FileExists(const std::string& path);

// Checks the file or directory at |path| exists.
bool Exists(const std::string& path);

// Creates a directory for |path|.
// If the path already exists, the call is a no-op and returns success.
// Only creates the top-level directory. The call fails if intermediate
// directories are missing.
absl::Status CreateDir(const std::string& path);

// Creates a directory for |path|.
// If the path already exists, the call is a no-op and returns success.
// Also creates missing intermediate directories.
absl::Status CreateDirRec(const std::string& path);

// Renames/moves from |from_path| to |to_path|. Note that Windows fails when
// |to_path| exists, while Linux atomically replaces the |to_path| with
// |from_path|.
absl::Status RenameFile(const std::string& from_path,
                        const std::string& to_path);

// Copies from |from_path| to |to_path|.
// Also creates missing intermediate directories.
// Note that on Windows it's impossible to move or replace items between drives.
// In that case this method should be used.
absl::Status CopyFileRec(const std::string& from_path,
                         const std::string& to_path);

// Deletes the file at |path|. Returns OK if the file was deleted, or if
// |path| does not exist. On Windows, this requires write permissions to the
// file, while on Linux, it requires write permissions to the directory that
// contains the file.
absl::Status RemoveFile(const std::string& path);

// Deletes the file at |path| and moves the file at |replacement_path| to
// |old_path|.
absl::Status ReplaceFile(const std::string& path,
                         const std::string& replacement_path);

// Mode bits for GetStats. It's a bit unclear how these work on Windows.
// Apparently, the group/other flags are mirrored from the user flags.
enum Mode {
  MODE_IFMT = 0170000,   // File type bits
  MODE_IFREG = 0100000,  // Regular file
  MODE_IFDIR = 0040000,  // Directory
  MODE_IFCHR = 0020000,  // Character-oriented device file
  MODE_IFIFO = 0010000,  // FIFO or Pipe

  MODE_ISUID = 0004000,  // Set-user-ID on execute bit
  MODE_ISGID = 0002000,  // Set-group-ID on execute bit
  MODE_ISVTX = 0001000,  // Sticky bit

  MODE_IRWXU = 0000700,  // User rwx
  MODE_IRUSR = 0000400,  // User r
  MODE_IWUSR = 0000200,  // User w
  MODE_IXUSR = 0000100,  // User x

  MODE_IRWXG = 0000070,  // Group rwx
  MODE_IRGRP = 0000040,  // Group r
  MODE_IWGRP = 0000020,  // Group w
  MODE_IXGRP = 0000010,  // Group x

  MODE_IRWXO = 0000007,  // Other rwx
  MODE_IROTH = 0000004,  // Other r
  MODE_IWOTH = 0000002,  // Other w
  MODE_IXOTH = 0000001,  // Other x
};

struct Stats {
  uint16_t mode = 0;          // Mode enum
  int64_t modified_time = 0;  // Unix epoch (local time)
  uint64_t size = 0;
};

// Returns stats for |path|. |modified_time| in
// the returned struct is local time. In other words
// it is the number of ticks from start of epoch by local time.
// TODO: Use consistent timestamps.
absl::Status GetStats(const std::string& path, Stats* stats);

// Returns the file size of the given file.
absl::Status FileSize(const std::string& path, uint64_t* size);

// Changes the mode bits of the file at |path| to |mode|.
absl::Status ChangeMode(const std::string& path, uint16_t mode);

// Removes |path| recursively.
absl::Status RemoveDirRec(const std::string& path);

// Reads the modification time of |path| and writes it to the given Unix
// epoch |timestamp| in UTC. |path| can point to a file or a directory.
absl::Status GetFileTime(const std::string& path, time_t* timestamp);

// Updates both the access and modification time of |path| to the given Unix
// epoch |timestamp| in UTC.
absl::Status SetFileTime(const std::string& path, time_t timestamp);

// Compares |path1| and |path2| and returns true if they are equal.
// Converts both paths to canonical form before coparison.
bool AreEqual(std::string path1, std::string path2);

}  // namespace path

inline path::ReadFlags operator|(path::ReadFlags a, path::ReadFlags b) {
  using T = std::underlying_type_t<path::ReadFlags>;
  return static_cast<path::ReadFlags>(static_cast<T>(a) | static_cast<T>(b));
}

inline path::ReadFlags operator&(path::ReadFlags a, path::ReadFlags b) {
  using T = std::underlying_type_t<path::ReadFlags>;
  return static_cast<path::ReadFlags>(static_cast<T>(a) & static_cast<T>(b));
}

}  // namespace cdc_ft

#endif  // COMMON_PATH_H_
