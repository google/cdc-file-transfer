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

#ifndef COMMON_UTIL_H_
#define COMMON_UTIL_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "common/platform.h"

namespace cdc_ft {

// Assorted utilities
class Util {
 public:
#if PLATFORM_WINDOWS
  // Converts a wide character string to a UTF8 string.
  // Illegal sequences are replaced by U+FFFD.
  static std::string WideToUtf8Str(const std::wstring& wchar_str);

  // Converts a UTF8 string to a wide character string.
  // Illegal sequences are replaced by U+FFFD.
  static std::wstring Utf8ToWideStr(const std::string& utf8_str);

  // Returns the string that corresponds to the Windows error id |error|.
  static std::string GetWin32Error(uint32_t error);

  // Returns the string that corresponds to the Windows error id GetLastError().
  static std::string GetLastWin32Error();
#endif

  // Returns the C runtime error for |err|.
  static std::string GetStrError(int err);

  // Returns the last C runtime error.
  static std::string GetLastStrError();

  // Returns the id of the current process.
  static int64_t GetPid();

  // Returns the width or kDefaultConsoleWidth if not running in console mode.
  static int GetConsoleWidth();

  // Returns true if stdout is associated with a terminal (alias TTY).
  static bool IsTTY();

  // Determines whether the given |data| matches PE/elf/shebang magic headers.
  // The data should be the beginning of a file and at least 4 bytes long.
  // The detection might yield false positives, but no false negatives.
  static bool IsExecutable(const void* data, size_t size);

  // On Windows, waits for a debugger to be attached and starts debugging.
  // On Linux, this just runs an infinite loop. To get into the debugger, set
  // breakpoint in the loop and move the instruction pointer (yellow arrow in
  // Visual Studio) out of the loop.
  static void WaitForDebugger();

  // Sleeps for |duration_ms| milliseconds.
  static void Sleep(uint32_t duration_ms);

  // Returns the number of bytes of the first UTF8 code point in str.
  // Returns 0 if the code point is not valid.
  static int Utf8CodePointLen(const char* str);

  // Generates a unique identifier. The identifier is a sequence of base-16
  // digits in the following format: '01234567-89AB-CDEF-0123-456789AB'.
  static std::string GenerateUniqueId();
};

// Splits the given string |s| at the given delimiter |delim| and returns the
// parts as a vector of string_views. Since a string_view does not copy the
// data, the caller is responsible for keeping the string memory allocated as
// long as the referenced parts need to be accessed.
//
// If |keep_empty| is false, empty parts will not be included in the result,
// otherwise empty string_views might be inserted.
//
// Returns an empty list if the given string is empty. If the delimiter is not
// found in |s|, a list with a single element containing |s| is returned.
std::vector<absl::string_view> SplitString(absl::string_view s,
                                           const char delim,
                                           bool keep_empty = true);
std::vector<absl::string_view> SplitString(const std::string& s,
                                           const char delim,
                                           bool keep_empty = true);

// Combines the given string |parts| to one string, separated by |delim|. Does
// not check if the previous part already ended with |delim|.
std::string JoinStrings(const std::vector<absl::string_view>& parts,
                        const char delim);

// Combines the given string |parts| in the range [first, last) to one string,
// all separated by |delim|. Does not check if the previous part already ended
// with |delim|. Ignores indices that are out of bounds.
std::string JoinStrings(const std::vector<absl::string_view>& parts,
                        size_t first, size_t last, const char delim);

// Prints a human-readable representation of the given |size| and decimal
// |precision|, such as "4 KB" or "2.34 MB".
std::string HumanBytes(double size, int precision = 0);

// Prints a human-readable representation of a duration as minutes and seconds
// in the format "mm:ss" (with leading zero for > 10 minutes).
std::string HumanDuration(const absl::Duration& d);

// FinalSetter sets a given |receiver| variable to a given |value| once it gets
// out of scope.
template <typename T>
class FinalSetter {
 public:
  FinalSetter(T* receiver, T value) : receiver_(receiver), value_(value) {}
  ~FinalSetter() { *receiver_ = std::move(value_); }

 private:
  T* receiver_;
  T value_;
};

}  // namespace cdc_ft

#endif  // COMMON_UTIL_H_
