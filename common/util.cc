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

#include "common/util.h"

#include <errno.h>

#include <cassert>

#include "absl/random/random.h"
#include "absl/strings/str_format.h"

#if PLATFORM_LINUX
#include <sys/ioctl.h>  // struct winsize, TIOCGWINSZ
#include <unistd.h>     // usleep, STDOUT_FILENO, isatty
#elif PLATFORM_WINDOWS
#include <io.h>  // _isatty

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

namespace cdc_ft {
namespace {
constexpr char kGuidFormat[] = "%08lX-%04hX-%04hX-%04hX-%04hX%08lX";
}

#if PLATFORM_WINDOWS

// static
std::string Util::WideToUtf8Str(const std::wstring& wchar_str) {
  int wchar_size = static_cast<int>(wchar_str.size());
  int utf8_size = WideCharToMultiByte(CP_UTF8, 0, wchar_str.c_str(), wchar_size,
                                      nullptr, 0, nullptr, nullptr);
  assert(utf8_size != 0 || wchar_size == 0);

  std::string utf8_str(utf8_size, 0);
  WideCharToMultiByte(
      CP_UTF8, 0, wchar_str.c_str(), static_cast<int>(wchar_str.size()),
      const_cast<LPSTR>(utf8_str.data()), utf8_size, nullptr, 0);
  return utf8_str;
}
#endif  // #if PLATFORM_WINDOWS

#if PLATFORM_WINDOWS
// static
std::wstring Util::Utf8ToWideStr(const std::string& utf8_str) {
  int utf8_size = static_cast<int>(utf8_str.size());
  int wchar_size =
      MultiByteToWideChar(CP_UTF8, 0, utf8_str.c_str(), utf8_size, nullptr, 0);
  assert(wchar_size != 0 || utf8_size == 0);

  std::wstring wchar_str(wchar_size, 0);
  MultiByteToWideChar(CP_UTF8, 0, utf8_str.c_str(), utf8_size,
                      const_cast<LPWSTR>(wchar_str.data()), wchar_size);
  return wchar_str;
}
#endif  // #if PLATFORM_WINDOWS

#if PLATFORM_WINDOWS
// static
std::string Util::GetWin32Error(uint32_t error) {
  LPVOID lpMsgBuf;

  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
                    FORMAT_MESSAGE_IGNORE_INSERTS,
                NULL, error, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                reinterpret_cast<LPTSTR>(&lpMsgBuf), 0, NULL);

  std::string message = WideToUtf8Str((LPCTSTR)lpMsgBuf);
  LocalFree(lpMsgBuf);

  while (!message.empty() &&
         (message.back() == '\r' || message.back() == '\n')) {
    message.pop_back();
  }

  return message;
}
#endif  // #if PLATFORM_WINDOWS

#if PLATFORM_WINDOWS
// static
std::string Util::GetLastWin32Error() { return GetWin32Error(GetLastError()); }
#endif  // #if PLATFORM_WINDOWS

// static
std::string Util::GetStrError(int err) {
#if PLATFORM_WINDOWS
  char msg[80] = {0};
  strerror_s(msg, sizeof(msg) - 1, err);
  return msg;
#elif PLATFORM_LINUX
  return std::strerror(err);
#endif
}

// static
std::string Util::GetLastStrError() { return GetStrError(errno); }

// static
int64_t Util::GetPid() {
#if PLATFORM_WINDOWS
  return ::GetCurrentProcessId();
#elif PLATFORM_LINUX
  return getpid();
#endif
}

// static
int Util::GetConsoleWidth() {
  static constexpr int kDefaultConsoleWidth = 80;

#if PLATFORM_WINDOWS
  CONSOLE_SCREEN_BUFFER_INFO info;
  if (!GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &info)) {
    return kDefaultConsoleWidth;
  }
  return info.srWindow.Right - info.srWindow.Left;
#elif PLATFORM_LINUX
  struct winsize size;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &size) != 0) {
    return kDefaultConsoleWidth;
  }
  return size.ws_col;
#endif
}

// static
bool Util::IsTTY() {
#if PLATFORM_WINDOWS
  return _isatty(_fileno(stdout));
#elif PLATFORM_LINUX
  return isatty(STDOUT_FILENO);
#endif
}

// static
bool Util::IsExecutable(const void* data, size_t size) {
  static constexpr uint8_t kElfMagic[] = {0x7fu, 0x45u, 0x4cu, 0x46u};
  static constexpr uint8_t kSheBangMagic[] = {'#', '!', '/'};
  static constexpr uint8_t kMzMagic[] = {0x4du, 0x5au};

  if (size < std::max(std::max(sizeof kElfMagic, sizeof kSheBangMagic),
                      sizeof kMzMagic)) {
    return false;
  }
  if (memcmp(data, kElfMagic, sizeof(kElfMagic)) == 0) return true;
  if (memcmp(data, kSheBangMagic, sizeof(kSheBangMagic)) == 0) return true;
  if (memcmp(data, kMzMagic, sizeof(kMzMagic)) == 0) return true;
  return false;
}

// static
void Util::WaitForDebugger() {
#if PLATFORM_LINUX
  static int s = 0;
  while (s == 0) {
    Sleep(1);
  }
#elif PLATFORM_WINDOWS
  __debugbreak();
#endif
}

// static
void Util::Sleep(uint32_t duration_ms) {
#if PLATFORM_LINUX
  usleep(duration_ms * 1000);
#elif PLATFORM_WINDOWS
  ::Sleep(duration_ms);
#endif
}

// static
int Util::Utf8CodePointLen(const char* str) {
  // UTF-8 code points are encoded as follows:
  // 1-byte: 0xxxxxxx
  // 2-byte: 110xxxxx 10xxxxxx
  // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx
  // 4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx

#define Is10(index) ((ustr[index] & 0xC0) == 0x80)
  // Note that this is guaranteed to never access memory beyond \0.
  const uint8_t* ustr = reinterpret_cast<const uint8_t*>(str);
  if (ustr[0] == 0) return 0;
  if ((ustr[0] & 0x80) == 0x00) return 1;
  if ((ustr[0] & 0xE0) == 0xC0) return Is10(1) ? 2 : 0;
  if ((ustr[0] & 0xF0) == 0xE0) return Is10(1) && Is10(2) ? 3 : 0;
  if ((ustr[0] & 0xF8) == 0xF0) return Is10(1) && Is10(2) && Is10(3) ? 4 : 0;
  return 0;
#undef Is10
}

std::string Util::GenerateUniqueId() {
  absl::BitGen gen;
  return absl::StrFormat(
      kGuidFormat, absl::Uniform<uint32_t>(gen), absl::Uniform<uint16_t>(gen),
      absl::Uniform<uint16_t>(gen), absl::Uniform<uint16_t>(gen),
      absl::Uniform<uint16_t>(gen), absl::Uniform<uint32_t>(gen));
}

std::vector<absl::string_view> SplitString(absl::string_view s,
                                           const char delim, bool keep_empty) {
  if (s.empty()) return std::vector<absl::string_view>();

  // Count no. of parts so that we can pre-allocate the target vector.
  int count = 1;
  size_t pos = 0;
  size_t last_pos = std::string::npos;
  for (pos = s.find(delim, pos); pos != std::string::npos;
       pos = s.find(delim, pos + 1)) {
    if (keep_empty || pos - 1 != last_pos) ++count;
    last_pos = pos;
  }

  // Collect all parts
  std::vector<absl::string_view> parts;
  parts.reserve(count);
  size_t first = 0, last = 0;
  for (last = s.find(delim, first); last != std::string::npos;
       last = s.find(delim, first)) {
    if (keep_empty || first < last) {
      parts.push_back(absl::string_view(s.data() + first, last - first));
    }
    first = last + 1;
  }
  // Append the last part, if needed
  if (keep_empty || first < s.size()) {
    parts.push_back(absl::string_view(s.data() + first, s.size() - first));
  }
  return parts;
}

std::vector<absl::string_view> SplitString(const std::string& s,
                                           const char delim, bool keep_empty) {
  absl::string_view sv(s.c_str(), s.size());
  return SplitString(sv, delim, keep_empty);
}

std::string HumanBytes(double size, int precision) {
  const double threshold = 2048;
  if (size < 1024)
    return absl::StrFormat("%d bytes", static_cast<size_t>(size));
  size /= 1024.0;
  std::string units = "KB";
  if (size > threshold) {
    size /= 1024.0;
    units = "MB";
  }
  if (size > threshold) {
    size /= 1024.0;
    units = "GB";
  }
  if (size > threshold) {
    size /= 1024.0;
    units = "TB";
  }
  if (size > threshold) {
    size /= 1024.0;
    units = "PB";
  }
  return absl::StrFormat("%.*f %s", precision, size, units);
}

std::string HumanDuration(const absl::Duration& d) {
  auto sec = absl::ToInt64Seconds(d);
  return absl::StrFormat("%02d:%02d", sec / 60, std::abs(sec) % 60);
}

std::string JoinStrings(const std::vector<absl::string_view>& parts,
                        size_t first, size_t last, const char delim) {
  std::string ret;
  for (size_t i = first; i < last && i < parts.size(); ++i) {
    if (i != first) ret.push_back(delim);
    ret.append(parts[i]);
  }
  return ret;
}

std::string JoinStrings(const std::vector<absl::string_view>& parts,
                        const char delim) {
  if (parts.empty()) return std::string();
  return JoinStrings(parts, 0, parts.size(), delim);
}

}  // namespace cdc_ft
