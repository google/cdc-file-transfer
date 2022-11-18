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

#include "common/path.h"

#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cassert>
#include <filesystem>  // NOLINT [We are not in google3]

#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "common/buffer.h"
#include "common/errno_mapping.h"
#include "common/log.h"
#include "common/status.h"
#include "common/status_macros.h"
#include "common/util.h"

#if PLATFORM_LINUX
#include <ftw.h>     // nftw
#include <stdlib.h>  // putenv
#include <unistd.h>  // readlink
#include <utime.h>   // struct utimbuf
#include <wordexp.h>
#define __stat64 stat64
#define _chmod chmod
#endif

#if PLATFORM_WINDOWS
#include <corecrt_io.h>  // chmod
#include <direct.h>      // _rmdir

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#undef ReplaceFile

#include <shlobj_core.h>  // SHGetKnownFolderPath
#endif

namespace cdc_ft {
namespace {

constexpr char kPathSeparators[] = "\\/";

#if PLATFORM_WINDOWS
// Jan 1, 1970 (begin of Unix epoch) in 100 ns ticks since Jan 1, 1601 (begin
// of Windows epoch).
constexpr int64_t kUnixToWindowsEpochOffset = 0x019DB1DED53E8000;

// A Windows tick is 100 ns.
constexpr int64_t kWinTicksPerSecond = 10000000;
#endif

// Checks if the given path needs a path separator appended before the next
// component can be added.
bool NeedsPathSep(const std::string* path) {
  if (path->empty()) return false;
  return path->back() != path::PathSeparator() &&
         path->back() != path::OtherPathSeparator();
}

void JoinImpl(std::string* dest, absl::string_view path,
              absl::string_view to_append, char separator) {
  assert(dest != nullptr);
  dest->clear();
  dest->reserve(to_append.length() + path.length() + 1);
  if (!path.empty()) absl::StrAppend(dest, path);
  if (!to_append.empty()) {
    if (NeedsPathSep(dest)) dest->push_back(separator);
    absl::StrAppend(dest, to_append);
  }
}

void AppendImpl(std::string* dest, absl::string_view to_append,
                char separator) {
  if (to_append.empty()) return;
  assert(dest != nullptr);
  dest->reserve(dest->length() + to_append.length() + 1);
  if (NeedsPathSep(dest)) dest->push_back(separator);
  absl::StrAppend(dest, to_append);
}

#if PLATFORM_WINDOWS
FILETIME UnixTimeToWindowsFileTime(time_t unix_timestamp) {
  int64_t winTicks = static_cast<int64_t>(unix_timestamp) * kWinTicksPerSecond +
                     kUnixToWindowsEpochOffset;
  if (winTicks < 0) {
    // FILETIME does not support dates before 1601. Ah well, we'll get over it.
    winTicks = 0;
  }

  FILETIME ft;
  ft.dwLowDateTime = winTicks & 0xffffffff;
  ft.dwHighDateTime = winTicks >> 32;
  return ft;
}
#endif

}  // namespace

namespace path {
namespace {

// absl::StrSplit delimiter that handles both "\r\n" and "\n" newlines.
class ByNewline {
 public:
  absl::string_view Find(absl::string_view text, size_t pos) const {
    size_t found_pos = text.find("\n", pos);
    if (found_pos == absl::string_view::npos) {
      return absl::string_view(text.data() + text.size(), 0);
    }
    if (found_pos > 0 && text[found_pos - 1] == '\r') {
      return text.substr(found_pos - 1, 2);
    }
    return text.substr(found_pos, 1);
  }
};

#if PLATFORM_WINDOWS
uint64_t ToUint64(uint32_t high, uint32_t low) {
  return (((uint64_t)high) << 32) | low;
}

// Converts Windows ticks to Unix epoch. Adapted from
// https://developers.google.com/protocol-buffers/docs/reference/csharp/class/google/protobuf/well-known-types/timestamp
int64_t FileTimeToTimeT(FILETIME ft) {
  uint64_t ticks = ToUint64(ft.dwHighDateTime, ft.dwLowDateTime);
  // Divide by kWinTicksPerSecond first to avoid overlfow when
  // ticks < kUnixToWindowsEpochOffset.
  return static_cast<int64_t>(ticks / kWinTicksPerSecond) -
         static_cast<int64_t>(kUnixToWindowsEpochOffset / kWinTicksPerSecond);
}

KNOWNFOLDERID ToWinFolderId(FolderId folder_id) {
  switch (folder_id) {
    case FolderId::kRoamingAppData:
      return FOLDERID_RoamingAppData;
    case FolderId::kProgramFiles:
      return FOLDERID_ProgramFiles;
  }
  return {0};
}

#endif

}  // namespace

absl::Status GetExeDir(std::string* dir) {
  dir->clear();
#if PLATFORM_WINDOWS
  wchar_t wide_exe_path[MAX_PATH];
  if (FAILED(GetModuleFileName(nullptr, wide_exe_path, MAX_PATH))) {
    return MakeStatus("Failed to get module file name");
  }
  std::string exe_path = Util::WideToUtf8Str(wide_exe_path);
#elif PLATFORM_LINUX
  char exe_path[PATH_MAX];
  ssize_t count = readlink("/proc/self/exe", exe_path, PATH_MAX);
  if (count == -1) {
    return MakeStatus("readlink() failed");
  }
#endif

  *dir = DirName(exe_path);
  return absl::OkStatus();
}

std::string GetTempDir() {
  return std::filesystem::temp_directory_path().u8string();
}

#if PLATFORM_WINDOWS
absl::Status GetKnownFolderPath(FolderId folder_id, std::string* path) {
  path->clear();
  wchar_t* wchar_path = nullptr;
  KNOWNFOLDERID win_id = ToWinFolderId(folder_id);
  if (!SUCCEEDED(SHGetKnownFolderPath(win_id, 0, NULL, &wchar_path))) {
    return MakeStatus("Failed to get known folder path");
  }
  *path = Util::WideToUtf8Str(wchar_path);
  CoTaskMemFree(wchar_path);
  return absl::OkStatus();
}
#endif

absl::Status ExpandPathVariables(std::string* path) {
#if PLATFORM_WINDOWS
  std::wstring wchar_path = Util::Utf8ToWideStr(*path);

  DWORD size = ::ExpandEnvironmentStrings(wchar_path.c_str(), nullptr, 0);
  if (size == 0) {
    return MakeStatus("Failed to get length of expanded path");
  }

  std::wstring wchar_expanded(size, 0);
  wchar_t* out_ptr = const_cast<wchar_t*>(wchar_expanded.c_str());
  if (ExpandEnvironmentStrings(wchar_path.c_str(), out_ptr, size) != size) {
    return MakeStatus("Failed to expand path");
  }

  // wchar_expanded has two null terminators at this point. Pop one.
  wchar_expanded.pop_back();
  *path = Util::WideToUtf8Str(wchar_expanded);
  return absl::OkStatus();
#else
  wordexp_t res;
  wordexp(path->c_str(), &res, 0);
  if (res.we_wordc > 1) {
    return absl::InvalidArgumentError(
        "Path expands to multiple results (did you use * etc. ?");
  }
  *path = res.we_wordv[0];
  wordfree(&res);
  return absl::OkStatus();
#endif
}

absl::Status GetEnv(const std::string& name, std::string* value) {
  value->clear();
#if PLATFORM_WINDOWS
  // The first call returns the size INCLUDING the null terminator.
  std::wstring wchar_name = Util::Utf8ToWideStr(name);
  DWORD size = ::GetEnvironmentVariable(wchar_name.c_str(), NULL, 0);
  if (size == 0) {
    return absl::NotFoundError(
        absl::StrFormat("Environment variable '%s' not found", name.c_str()));
  }

  // The first call returns the bytes written EXCLUDING the null terminator,
  // hence the '+ 1'.
  std::wstring wstr_value(size, 0);
  wchar_t* val_ptr = const_cast<wchar_t*>(wstr_value.c_str());
  if (::GetEnvironmentVariable(wchar_name.c_str(), val_ptr, size) + 1 != size) {
    return MakeStatus("Failed to get environment variable '%s'", name);
  }

  // Pop null terminator.
  wstr_value.pop_back();

  *value = Util::WideToUtf8Str(wstr_value);
  return absl::OkStatus();
#elif PLATFORM_LINUX
  char* value_cstr = std::getenv(name.c_str());
  if (!value_cstr) {
    return absl::NotFoundError(
        absl::StrFormat("Environment variable '%s' not found", name.c_str()));
  }

  *value = value_cstr;
  return absl::OkStatus();
#endif
}

absl::Status SetEnv(const std::string& name, const std::string& value) {
#if PLATFORM_WINDOWS
  std::wstring wchar_name = Util::Utf8ToWideStr(name);
  std::wstring wchar_value = Util::Utf8ToWideStr(value);
  if (!SetEnvironmentVariable(wchar_name.c_str(), wchar_value.c_str())) {
    return MakeStatus("Failed to set environment variable '%s'", name);
  }
  return absl::OkStatus();
#elif PLATFORM_LINUX
  if (setenv(name.c_str(), value.c_str(), /*overwrite=*/1) != 0) {
    return MakeStatus("Failed to set environment variable '%s'", name);
  }
  return absl::OkStatus();
#endif
}

#if PLATFORM_WINDOWS
std::string GetDrivePrefix(const std::string& path) {
  if (path.empty()) {
    return std::string();
  }

  if (path[0] != '\\') {
    size_t pos = path.find(":");
    if (pos == std::string::npos) {
      // E.g. "\path\to\file" or "path\to\file".
      return std::string();
    }

    // E.g. "C:" or "C:\path\to\file".
    return path.substr(0, pos + 1);
  }

  // Check if it is a network share like \\computername\sharename\path\to\file.
  if (path.size() == 1 || path[1] != '\\') {
    return std::string();
  }

  // 3 since the computername should be non-empty.
  size_t third_backslash_pos = path.find('\\', 2);
  if (third_backslash_pos == std::string::npos || third_backslash_pos < 3 ||
      path.size() == third_backslash_pos + 1) {
    // E.g. "\\c" or "\\\" or "\\c\".
    return std::string();
  }

  size_t fourth_backslash_pos = path.find('\\', third_backslash_pos + 1);
  if (fourth_backslash_pos < third_backslash_pos + 2) {
    // E.g. "\\c\".
    return std::string();
  }

  // E.g. "\\c\s" or "\\c\s\path\to\file"
  return path.substr(0, fourth_backslash_pos);
}
#endif  // if PLATFORM_WINDOWS

std::string GetCwd() { return std::filesystem::current_path().u8string(); }

void SetCwd(const std::string& path) {
  std::filesystem::current_path(std::filesystem::u8path(path));
}

std::string GetFullPath(const std::string& path) {
  if (path.empty()) {
    return std::string();
  }
  std::filesystem::path fspath = std::filesystem::u8path(path);
  // Expand relative paths with the current working directory to match the
  // behavior of the Windows GetFullPathName*() API.
  if (fspath.is_relative()) {
    auto cwd = std::filesystem::current_path();
    // Special case on Windows: paths like "C:" or "C:foo", which are
    // interpreted relative to the drive's cwd. If the drive letter is different
    // from the current drive, then the directory is treated as begin absolute.
    if (fspath.has_root_name()) {
      if (fspath.root_name() != cwd.root_name()) {
        // This matches the behavior of std::filesystem::absolute() for a root
        // other than the cwd.
        fspath = std::filesystem::absolute(fspath.root_name()) / fspath;
      }
    }
    fspath = cwd / fspath;
  }
  std::error_code ec;
  std::filesystem::path cfspath = std::filesystem::weakly_canonical(fspath, ec);
  if (ec) {
    // Fall back to creating a normalized absolute path.
    cfspath = std::filesystem::absolute(fspath, ec).lexically_normal();
    if (ec) {
      LOG_WARNING("Failed to canonicalize path '%s': %s", path, ec.message());
      return fspath.u8string();
    }
  }
  return cfspath.u8string();
}

bool IsAbsolute(const std::string& path) {
  return std::filesystem::u8path(path).is_absolute();
}

std::string DirName(const std::string& path) {
  if (path.empty()) return std::string();
#if PLATFORM_WINDOWS
  // Keep the drive prefix.
  const std::string drive = GetDrivePrefix(path);
  std::string dirpart = path.substr(drive.size());
#else
  const std::string drive;
  const std::string& dirpart = path;
#endif
  size_t non_sep_pos = dirpart.find_last_not_of(kPathSeparators);
  size_t sep_pos = dirpart.find_last_of(kPathSeparators, non_sep_pos);
  if (sep_pos == std::string::npos) {
    // Handle the cases "foo", "C:", "\\computer\share".
    return drive;
  }
  // Handle the cases "/", "\\\\", "C:\\".
  if (non_sep_pos == std::string::npos) return path;
  // Handle the cases "/foo", "///foo", "C:\\foo", or "\\\\foo".
  size_t dir_end_pos = dirpart.find_last_not_of(kPathSeparators, sep_pos);
  if (dir_end_pos == std::string::npos)
    return path.substr(0, drive.size() + sep_pos + 1);
  // Handle the cases "/foo/bar", "foo/bar", "C:\\foo", "\\\\foo\\bar".
  return path.substr(0, drive.size() + dir_end_pos + 1);
}

std::string BaseName(const std::string& path) {
  if (path.empty()) return std::string();
  size_t non_sep_pos = path.find_last_not_of(kPathSeparators);
  size_t sep_pos = path.find_last_of(kPathSeparators, non_sep_pos);
  if (non_sep_pos == std::string::npos) non_sep_pos = path.size() - 1;
  if (sep_pos != std::string::npos)
    return path.substr(sep_pos + 1, non_sep_pos - sep_pos);
  return path.substr(0, non_sep_pos + 1);
}

std::string Join(absl::string_view path, absl::string_view to_append) {
  std::string dest;
  Join(&dest, path, to_append);
  return dest;
}

std::string Join(absl::string_view path, absl::string_view to_append1,
                 absl::string_view to_append2) {
  return Join(Join(path, to_append1), to_append2);
}

std::string Join(absl::string_view path, absl::string_view to_append1,
                 absl::string_view to_append2, absl::string_view to_append3) {
  return Join(Join(path, to_append1, to_append2), to_append3);
}

void Join(std::string* dest, absl::string_view path,
          absl::string_view to_append) {
  JoinImpl(dest, path, to_append, path::PathSeparator());
}

std::string JoinUnix(absl::string_view path, absl::string_view to_append) {
  std::string dest;
  JoinImpl(&dest, path, to_append, '/');
  return dest;
}

void Append(std::string* dest, absl::string_view to_append) {
  AppendImpl(dest, to_append, path::PathSeparator());
}

void AppendUnix(std::string* dest, absl::string_view to_append) {
  AppendImpl(dest, to_append, '/');
}

bool EndsWithPathSeparator(const std::string& path) {
  return !path.empty() && (path.back() == PathSeparator() ||
                           path.back() == OtherPathSeparator());
}

void EnsureEndsWithPathSeparator(std::string* path) {
  if (!EndsWithPathSeparator(*path)) {
    path->push_back(PathSeparator());
  }
}

void EnsureDoesNotEndWithPathSeparator(std::string* path) {
  while (EndsWithPathSeparator(*path)) {
    path->pop_back();
  }
}

void FixPathSeparators(std::string* path) {
  std::replace(path->begin(), path->end(), OtherPathSeparator(),
               PathSeparator());
}

std::string ToUnix(std::string path) {
  std::replace(path.begin(), path.end(), '\\', '/');
  return path;
}

std::string ToNative(std::string path) {
  std::replace(path.begin(), path.end(), OtherPathSeparator(), PathSeparator());
  return path;
}

absl::StatusOr<FILE*> OpenFile(const std::string& path, const char* mode) {
  FILE* file;
#if PLATFORM_WINDOWS
  std::wstring wchar_path = Util::Utf8ToWideStr(path);
  std::wstring wchar_mode = Util::Utf8ToWideStr(mode);

  file = _wfsopen(wchar_path.c_str(), wchar_mode.c_str(), _SH_DENYNO);
#else
  // In Linux, _SH_DENYNO is set by default.
  // Locking might be realized by lockf or fcntl.
  file = fopen(path.c_str(), mode);
#endif
  if (!file) {
    int err = errno;
    return ErrnoToCanonicalStatus(err, "Failed to open file '%s'", path);
  }
  return file;
}

#if PLATFORM_WINDOWS

void LogSearchError(const std::wstring& path) {
  uint32_t last_error = GetLastError();
  if (last_error == ERROR_FILE_NOT_FOUND || last_error == ERROR_NO_MORE_FILES) {
    // This is fine, could be an empty directory or pattern didn't find any
    // files in this directory.
    return;
  }

  std::string utf8_path = Util::WideToUtf8Str(path);

  if (last_error == ERROR_ACCESS_DENIED) {
    // Access denied is somewhat expected, so just warn and return true.
    LOG_WARNING("Failed to search '%s': Access denied", utf8_path.c_str());
    return;
  }

  LOG_ERROR("Failed to search '%s': %s", utf8_path.c_str(),
            Util::GetLastWin32Error().c_str());
  return;
}

bool IsRegularDir(const WIN32_FIND_DATA& data) {
  // Not a directory? Note that FindExSearchLimitToDirectories is just a hint.
  if (!(data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
    return false;
  }

  // Ignore "." and "..".
  if (wcscmp(data.cFileName, L".") == 0 || wcscmp(data.cFileName, L"..") == 0) {
    return false;
  }

  return true;
}

absl::Status SearchFilesWin(const std::wstring& dir,
                            const std::wstring& pattern, bool recursive,
                            SearchHandler handler, bool reset_pattern) {
  if (!recursive && pattern.empty()) {
    return absl::OkStatus();
  }
  WIN32_FIND_DATA data;
  HANDLE hFind;
  // For non-trivial patterns (!= "*"), we need a second pass to recurse into
  // subdirectories. For instance "*.txt" wouldn't find "subdir\text.txt" since
  // "subdir" doesn't match "*.txt".
  // The condition with pattern.find('*') guarantees that the same directory
  // is traversed only once, which is necessary for example for the first-level
  // directory: for sync \source\dir_to_copy /destination: dir = source, pattern
  // = dir_co_copy.
  bool need_separate_dir_pass =
      pattern != L"*" && pattern.find('*') != std::wstring::npos;
  //
  // Find all files. Also recurse into subdirs if !need_separate_dir_pass.
  //
  std::wstring path = dir + pattern;
  hFind = FindFirstFileEx(path.c_str(), FindExInfoBasic, &data,
                          FindExSearchNameMatch, nullptr,
                          FIND_FIRST_EX_LARGE_FETCH);

  if (hFind == INVALID_HANDLE_VALUE) {
    LogSearchError(path);
  } else {
    do {
      if (data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
        if (IsRegularDir(data)) {
          // Send information about the directory to the server, then
          // continue on processing its files recursively.
          std::string utf8_filename = Util::WideToUtf8Str(data.cFileName);
          std::string utf8_dir = Util::WideToUtf8Str(dir);
          int64_t modified_time = FileTimeToTimeT(data.ftLastWriteTime);
          absl::Status status =
              handler(utf8_dir, utf8_filename, modified_time, 0, true);
          if (!status.ok()) {
            return status;
          }
          if (recursive) {
            // Recurse into subdirectories.
            std::wstring subdir = dir + data.cFileName + L"\\";
            std::wstring new_pattern = pattern;
            if (reset_pattern) {
              new_pattern = L"*";
            }
            status =
                SearchFilesWin(subdir, new_pattern, recursive, handler, false);
            if (!status.ok()) {
              return status;
            }
          }
        }
        continue;
      }
      // Send filename, write time and file size to the handler.
      std::string utf8_dir = Util::WideToUtf8Str(dir);
      std::string utf8_filename = Util::WideToUtf8Str(data.cFileName);
      int64_t modified_time = FileTimeToTimeT(data.ftLastWriteTime);
      uint64_t size = ToUint64(data.nFileSizeHigh, data.nFileSizeLow);
      absl::Status status =
          handler(utf8_dir, utf8_filename, modified_time, size, false);
      if (!status.ok()) {
        FindClose(hFind);
        return WrapStatus(status, "Failed to search '%s': Handler failed",
                          utf8_dir + utf8_filename);
      }
    } while (FindNextFile(hFind, &data) != 0);
    LogSearchError(dir);
    FindClose(hFind);
  }

  //
  // Do separate directory pass to recurse into subdirectories.
  //

  if (recursive && need_separate_dir_pass) {
    hFind = FindFirstFileEx((dir + L"*").c_str(), FindExInfoBasic, &data,
                            FindExSearchLimitToDirectories, nullptr,
                            FIND_FIRST_EX_LARGE_FETCH);
    if (hFind == INVALID_HANDLE_VALUE) {
      LogSearchError(dir);
    } else {
      do {
        if (IsRegularDir(data)) {
          std::wstring subdir = dir + data.cFileName + L"\\";
          std::wstring new_pattern = pattern;
          if (reset_pattern) {
            new_pattern = L"*";
          }
          absl::Status status =
              SearchFilesWin(subdir, new_pattern, recursive, handler, false);
          if (!status.ok()) {
            return status;
          }
        }
      } while (FindNextFile(hFind, &data) != 0);

      LogSearchError(dir);
      FindClose(hFind);
    }
  }
  return absl::OkStatus();
}

#elif PLATFORM_LINUX

namespace {

// nftw() does not allow currying its handler function. Capturing lambdas don't
// cast to a plain function pointer. This is an (ugly) workaround. See e.g.
// https://stackoverflow.com/questions/7852101/c-lambda-with-captures-as-a-function-pointer
thread_local struct GlobalFtwData {
  SearchHandler handler;
  bool recursive = false;
  absl::Status status;
} global_ftw_data;

int ftw_handler(const char* filepath, const struct stat* info, int typeflag,
                struct FTW* pathinfo) {
  // Send files to the handler.
  if (typeflag == FTW_F) {
    std::string dir(filepath, pathinfo->base);
    std::string filename = filepath + pathinfo->base;
    int64_t modified_time = info->st_mtime;
    uint64_t size = info->st_size;

    // Make nftw stop iterating the file tree if the handler fails.
    global_ftw_data.status =
        global_ftw_data.handler(dir, filename, modified_time, size, false);
    if (!global_ftw_data.status.ok()) {
      return FTW_STOP;
    }
  }
  if (typeflag == FTW_D && pathinfo->level > 0) {
    std::string dir(filepath, pathinfo->base);
    std::string dirname = filepath + pathinfo->base;
    if (dirname != "." && dirname != "..") {
      int64_t modified_time = info->st_mtime;

      // Make nftw stop iterating the tree if the handler fails.
      global_ftw_data.status =
          global_ftw_data.handler(dir, dirname, modified_time, 0, true);
      if (!global_ftw_data.status.ok()) {
        return FTW_STOP;
      }
      if (!global_ftw_data.recursive) {
        return FTW_SKIP_SUBTREE;
      }
    }
  }
  return FTW_CONTINUE;
}

}  // namespace

absl::Status SearchFilesLinux(const std::string& dir, bool recursive,
                              SearchHandler handler) {
  // FTW_PHYS means "do not follow symlinks", i.e. symlinks will be overwritten
  // by client files if they have the same name. 32 is the max directory depth
  // until things get slower.
  global_ftw_data = {std::move(handler), recursive};
  int result = nftw(dir.c_str(), ftw_handler, 32, FTW_PHYS | FTW_ACTIONRETVAL);
  absl::Status status = global_ftw_data.status;
  global_ftw_data = GlobalFtwData();

  // ENOENT means the directory does not exist. This is fine.
  if (result < 0 && errno != ENOENT) {
    return MakeStatus("Failed to search '%s': nftw() failed: %s.", dir,
                      strerror(errno));
  }
  if (result > 0) {
    // Should only happen if ftw_handler returns FTW_STOP.
    assert(!status.ok());
    assert(result == FTW_STOP);
    return WrapStatus(status, "Failed to search '%s': Search handler failed",
                      dir);
  }

  return absl::OkStatus();
}
#endif

absl::Status SearchFiles(const std::string& pattern, bool recursive,
                         SearchHandler handler) {
#if PLATFORM_WINDOWS
  std::wstring wchar_pattern = Util::Utf8ToWideStr(pattern);

  // If |pattern| is a directory, search that directory. Otherwise, assume it's
  // a file or some file pattern.
  std::wstring dir = wchar_pattern;
  std::wstring filename_pattern;
  DWORD ftyp = GetFileAttributes(dir.c_str());
  bool reset_pattern = false;

  if (ftyp != INVALID_FILE_ATTRIBUTES && (ftyp & FILE_ATTRIBUTE_DIRECTORY)) {
    // It's a directory.
    if (!recursive) {
      return SearchFilesWin(dir, filename_pattern, recursive, handler,
                            reset_pattern);
    }
    if (dir.back() == L'\\') {
      filename_pattern = L"*";
    } else {
      // On Linux, source directory without trailing \\ is traversed.
      // To enable the same behavior on windows: pattern=dir_to_copy
      // and it will be reset after the first-level traversal.
      // Otherwise, server_dirs and client_dirs would differ.
      reset_pattern = true;
      while (!dir.empty() && dir.back() != L'\\') {
        dir.pop_back();
      }
      filename_pattern = wchar_pattern.substr(dir.size());
    }
  } else {
    // Assume it's a file or file pattern.
    while (!dir.empty() && dir.back() != L'\\') {
      dir.pop_back();
    }
    filename_pattern = wchar_pattern.substr(dir.size());
  }
  return SearchFilesWin(dir, filename_pattern, recursive, handler,
                        reset_pattern);
#else
  return SearchFilesLinux(pattern, recursive, handler);
#endif
}

absl::Status StreamReadFileContents(FILE* file, size_t buffer_size,
                                    StreamReadFileHandler handler) {
  Buffer buffer(buffer_size);
  return StreamReadFileContents(file, &buffer, handler);
}

absl::Status StreamReadFileContents(FILE* file, Buffer* buffer,
                                    StreamReadFileHandler handler) {
  if (!buffer->size()) {
    return absl::FailedPreconditionError("Given buffer is empty");
  }

  for (;;) {
    const size_t num_read =
        fread_nolock(buffer->data(), 1, buffer->size(), file);

    // Note: The handler might read from file, so check feof_nolock now.
    bool eof = num_read != buffer->size() && feof_nolock(file);
    if (num_read > 0) {
      absl::Status status = handler(buffer->data(), num_read);
      if (!status.ok()) {
        return WrapStatus(status, "Handler failed");
      }
    }
    if (num_read != buffer->size()) {
      if (eof) {
        // Indicate eof.
        absl::Status status = handler(nullptr, 0);
        if (!status.ok()) {
          return WrapStatus(status, "Handler failed");
        }
        return absl::OkStatus();
      }

      return MakeStatus("fread_nolock() failed");
    }
  }
}

absl::Status StreamWriteFileContents(FILE* file,
                                     StreamWriteFileHandler handler) {
  for (;;) {
    const void* data;
    size_t data_size;
    absl::Status status = handler(&data, &data_size);
    if (!status.ok()) {
      return WrapStatus(status, "Handler failed");
    }
    // By returning (nullptr, 0), the handler indicates EOF.
    if (data_size == 0) {
      assert(!data);
      return absl::OkStatus();
    }

    const size_t num_written = fwrite_nolock(data, 1, data_size, file);
    if (num_written != data_size) {
      return MakeStatus("fwrite_nolock() failed");
    }
  }
}

absl::Status ReadFile(const std::string& path, Buffer* data) {
  assert(data);
  data->clear();

  absl::StatusOr<FILE*> file = OpenFile(path, "rb");
  if (!file.ok()) {
    return file.status();
  }

  fseek64_nolock(*file, 0, SEEK_END);
  int64_t size = ftell64(*file);
  data->resize(size);
  fseek64_nolock(*file, 0, SEEK_SET);

  if (fread_nolock(data->data(), 1, data->size(), *file) != data->size()) {
    data->clear();
    return MakeStatus("Failed to read data of size %u from file '%s'",
                      data->size(), path);
  }

  fclose(*file);
  return absl::OkStatus();
}

absl::StatusOr<size_t> ReadFile(const std::string& path, void* data,
                                size_t offset, size_t len) {
  assert(data);
  if (len == 0) {
    return 0;
  }

  absl::StatusOr<FILE*> file = OpenFile(path, "rb");
  if (!file.ok()) {
    return file.status();
  }
  if (offset != 0) {
    fseek64_nolock(*file, offset, SEEK_SET);
  }
  size_t read_len = fread_nolock(data, 1, len, *file);

  if (read_len != len && !feof_nolock(*file)) {
    fclose(*file);
    return MakeStatus("fread_nolock failed: %s", Util::GetLastStrError());
  }
  fclose(*file);
  return read_len;
}

absl::StatusOr<std::string> ReadFile(const std::string& path) {
  absl::StatusOr<FILE*> file = OpenFile(path, "rb");
  if (!file.ok()) {
    return file.status();
  }

  fseek64_nolock(*file, 0, SEEK_END);
  int64_t size = ftell64(*file);
  std::string data;
  data.resize(size);
  fseek64_nolock(*file, 0, SEEK_SET);

  if (fread_nolock(data.data(), 1, data.size(), *file) != data.size()) {
    data.clear();
    return MakeStatus("Failed to read data of size %u from file '%s'",
                      data.size(), path);
  }

  fclose(*file);
  return data;
}

absl::Status ReadAllLines(const std::string& path,
                          std::vector<std::string>* lines, ReadFlags flags) {
  Buffer data;
  absl::Status status = ReadFile(path, &data);
  if (!status.ok()) {
    return status;
  }

  std::string all_lines =
      std::string(reinterpret_cast<char*>(data.data()), data.size());

  *lines = absl::StrSplit(all_lines, ByNewline());

  if ((flags & ReadFlags::kTrimWhitespace) != ReadFlags::kNone) {
    for (std::string& line : *lines) {
      absl::StripAsciiWhitespace(&line);
    }
  }

  if ((flags & ReadFlags::kRemoveEmpty) != ReadFlags::kNone) {
    lines->erase(
        std::remove_if(lines->begin(), lines->end(),
                       [](const std::string& line) { return line.empty(); }),
        lines->end());
  }

  return absl::OkStatus();
}

absl::Status GetStats(const std::string& path, Stats* stats) {
  struct __stat64 os_stats;
  bool result;
#if PLATFORM_WINDOWS
  result = _wstat64(Util::Utf8ToWideStr(path).c_str(), &os_stats) == 0;
#elif PLATFORM_LINUX
  result = stat64(path.c_str(), &os_stats) == 0;
#endif
  if (!result) {
    int err = errno;
    *stats = Stats();
    return ErrnoToCanonicalStatus(err, "Failed to stat '%s'", path);
  }

  stats->mode = os_stats.st_mode;
  stats->size = os_stats.st_size;
  stats->modified_time = os_stats.st_mtime;
  return absl::OkStatus();
}

absl::Status FileSize(const std::string& path, uint64_t* size) {
  Stats stats;
  absl::Status status = GetStats(path, &stats);
  if (!status.ok()) return status;
  *size = stats.size;
  return absl::OkStatus();
}

absl::Status ChangeMode(const std::string& path, uint16_t mode) {
  if (_chmod(path.c_str(), mode) != 0) {
    return MakeStatus("chmod() failed: %s", Util::GetLastStrError());
  }
  return absl::OkStatus();
}

absl::Status WriteFile(const std::string& path, const Buffer& data) {
  return WriteFile(path, data.data(), data.size());
}

absl::Status WriteFile(const std::string& path, const std::string& data) {
  return WriteFile(path, data.data(), data.size());
}

absl::Status WriteFile(const std::string& path, const void* data, size_t len) {
  absl::StatusOr<FILE*> file = OpenFile(path, "wb");
  if (!file.ok()) {
    return file.status();
  }

  if (fwrite_nolock(data, 1, len, *file) != len) {
    return MakeStatus("Failed to write data of size %u to file '%s'", len,
                      path);
  }

  fclose(*file);
  return absl::OkStatus();
}

absl::Status CreateSymlink(const std::string& target,
                           const std::string& link_path, bool is_dir) {
  std::error_code error_code;
  std::filesystem::path target_u8 = std::filesystem::u8path(target);
  std::filesystem::path link_path_u8 = std::filesystem::u8path(link_path);
  if (is_dir) {
    std::filesystem::create_symlink(target_u8, link_path_u8, error_code);
  } else {
    std::filesystem::create_directory_symlink(target_u8, link_path_u8,
                                              error_code);
  }
  return ErrorCodeToCanonicalStatus(
      error_code, "Failed to create symlink '%s' with target '%s'", link_path,
      target);
}

absl::StatusOr<std::string> GetSymlinkTarget(const std::string& link_path) {
  std::error_code error_code;
  std::filesystem::path link_path_u8 = std::filesystem::u8path(link_path);
  std::filesystem::path symlink_target =
      std::filesystem::read_symlink(link_path_u8, error_code);
  if (error_code) {
    return ErrorCodeToCanonicalStatus(error_code, "Failed to read symlink '%s'",
                                      link_path);
  }
  return symlink_target.u8string();
}

bool DirExists(const std::string& path) {
  Stats stats;
  return GetStats(path, &stats).ok() && (stats.mode & MODE_IFMT) == MODE_IFDIR;
}

bool FileExists(const std::string& path) {
  Stats stats;
  return GetStats(path, &stats).ok() && (stats.mode & MODE_IFMT) == MODE_IFREG;
}

bool Exists(const std::string& path) {
  Stats stats;
  return GetStats(path, &stats).ok();
}

absl::Status CreateDir(const std::string& path) {
  std::error_code error_code;
  std::filesystem::create_directory(std::filesystem::u8path(path), error_code);
  return ErrorCodeToCanonicalStatus(error_code,
                                    "Failed to create directory '%s'", path);
}

absl::Status CreateDirRec(const std::string& path) {
  std::error_code error_code;
  std::filesystem::create_directories(std::filesystem::u8path(path),
                                      error_code);
  return ErrorCodeToCanonicalStatus(error_code,
                                    "Failed to create directory '%s'", path);
}

absl::Status RenameFile(const std::string& from_path,
                        const std::string& to_path) {
  if (rename(from_path.c_str(), to_path.c_str()) != 0) {
    return MakeStatus("rename() failed: %s.", Util::GetLastStrError());
  }
  return absl::OkStatus();
}

absl::Status CopyFileRec(const std::string& from_path,
                         const std::string& to_path) {
  std::error_code error;
  std::filesystem::copy(std::filesystem::u8path(from_path),
                        std::filesystem::u8path(to_path),
                        std::filesystem::copy_options::recursive |
                            std::filesystem::copy_options::overwrite_existing,
                        error);
  if (error) {
    return MakeStatus("Failed to copy '%s' to '%s': '%s'", from_path, to_path,
                      error.message());
  }
  return absl::OkStatus();
}

absl::Status RemoveDirRec(const std::string& path) {
  std::error_code error;
  std::filesystem::remove_all(std::filesystem::u8path(path), error);
  if (error) {
    return MakeStatus("Failed to recursively remove directory %s: %s", path,
                      error.message());
  }
  return absl::OkStatus();
}

absl::Status RemoveFile(const std::string& path) {
#if PLATFORM_WINDOWS
  // On Linux, remove() removes also empty directories.
  // On Windows, special handling of empty-directory deletion is required.
  if (DirExists(path)) {
    if (_rmdir(path.c_str()) != 0 && errno != ENOENT) {
      return MakeStatus("_rmdir() failed: %s.", Util::GetLastStrError());
    }
    return absl::OkStatus();
  }
#endif

  if (remove(path.c_str()) != 0 && errno != ENOENT) {
    return MakeStatus("remove() failed: %s.", Util::GetLastStrError());
  }
  return absl::OkStatus();
}

absl::Status ReplaceFile(const std::string& path,
                         const std::string& replacement_path) {
  absl::Status status;

  // Delete old file.
  status = RemoveFile(path);
  if (!status.ok()) {
    return WrapStatus(status, "RemoveFile() failed for '%s'", path);
  }

  // Rename new file to old file.
  status = RenameFile(replacement_path, path);
  if (!status.ok()) {
    return WrapStatus(status, "Rename() failed from '%s' to '%s'",
                      replacement_path, path);
  }

  return absl::OkStatus();
}

absl::Status GetFileTime(const std::string& path, time_t* timestamp) {
#if PLATFORM_WINDOWS
  std::wstring wchar_path = Util::Utf8ToWideStr(path);
  HANDLE handle = CreateFileW(
      wchar_path.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS, NULL);
  if (handle == INVALID_HANDLE_VALUE) {
    return MakeStatus("Failed to open '%s': %s", path,
                      Util::GetLastWin32Error());
  }

  FILETIME ft;
  int res = GetFileTime(handle, (LPFILETIME)NULL, (LPFILETIME)NULL, &ft);
  CloseHandle(handle);
  if (res == 0) {
    return MakeStatus("Failed to get file time for '%s': %s", path,
                      Util::GetLastWin32Error());
  }
  *timestamp = FileTimeToTimeT(ft);
#elif PLATFORM_LINUX
  Stats st;
  RETURN_IF_ERROR(GetStats(path, &st));
  *timestamp = static_cast<time_t>(st.modified_time);
#endif
  return absl::OkStatus();
}

absl::Status SetFileTime(const std::string& path, time_t timestamp) {
  // Would be great to use std::filesystem::last_write_time here, but
  // apparently there's no reliable conversion from time_t available in C++17,
  // see
  // https://developercommunity.visualstudio.com/t/stdfilesystemfile-time-type-does-not-allow-easy-co/251213
#if PLATFORM_WINDOWS
  HANDLE handle =
      CreateFileW(Util::Utf8ToWideStr(path).c_str(), FILE_WRITE_ATTRIBUTES,
                  FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                  FILE_ATTRIBUTE_NORMAL, NULL);
  if (handle == INVALID_HANDLE_VALUE) {
    return MakeStatus("Failed to open '%s': %s", path,
                      Util::GetLastWin32Error());
  }

  FILETIME ft = UnixTimeToWindowsFileTime(timestamp);
  int res = SetFileTime(handle, (LPFILETIME)NULL, &ft, &ft);
  CloseHandle(handle);
  if (res == 0) {
    return MakeStatus("Failed to set file time for '%s': %s", path,
                      Util::GetLastWin32Error());
  }
#elif PLATFORM_LINUX
  // Set both actime and modtime to save a stat operation to get the actime.
  struct utimbuf utb;
  utb.actime = timestamp;
  utb.modtime = timestamp;
  if (utime(path.c_str(), &utb) != 0) {
    return MakeStatus("Failed to set file time for '%s': %s", path,
                      Util::GetLastStrError());
  }
#endif

  return absl::OkStatus();
}

std::filesystem::path ToCanonical(const std::string& path) {
  std::string str_path = GetFullPath(path);
#if PLATFORM_WINDOWS
  // Normalize drive prefix if present.
  std::string prefix = GetDrivePrefix(str_path);
  for (auto& c : prefix) {
    c = toupper(c);
  }
  str_path = prefix + str_path.substr(prefix.length());
#endif  // if PLATFORM_WINDOWS
  return std::filesystem::u8path(str_path);
}

bool AreEqual(std::string path1, std::string path2) {
  return ToCanonical(path1).compare(ToCanonical(path2)) == 0;
}
}  // namespace path
}  // namespace cdc_ft
