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

#include <limits>

#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

using StringList = std::vector<absl::string_view>;

#if PLATFORM_WINDOWS
constexpr wchar_t kWcharString[] = L"Hey Google, where's the next ⛽?";
constexpr char kUtf8String[] = u8"Hey Google, where's the next ⛽?";

// Invalid character (\uFFFD) + terminator in oth cases.
constexpr wchar_t kInvalidWcharString[] = {65533, 0};
constexpr char kInvalidUtf8String[] = {-17, -65, -67, 0};
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, WideToUtf8Str_Valid) {
  std::string utf8_string = Util::WideToUtf8Str(kWcharString);
  EXPECT_EQ(utf8_string, kUtf8String);
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, WideToUtf8Str_Empty) {
  std::string utf8_string = Util::WideToUtf8Str(L"");
  EXPECT_TRUE(utf8_string.empty());
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, WideToUtf8Str_Invalid) {
  std::wstring wchar_string = Util::Utf8ToWideStr(kInvalidUtf8String);
  EXPECT_EQ(wchar_string, kInvalidWcharString);
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, Utf8ToWideStr_Valid) {
  std::wstring wchar_string = Util::Utf8ToWideStr(kUtf8String);
  EXPECT_EQ(wchar_string, kWcharString);
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, Utf8ToWideStr_Empty) {
  std::wstring wchar_string = Util::Utf8ToWideStr("");
  EXPECT_TRUE(wchar_string.empty());
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, Utf8ToWideStr_Invalid) {
  std::string utf8_string = Util::WideToUtf8Str(kInvalidWcharString);
  EXPECT_EQ(utf8_string, kInvalidUtf8String);
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, GetErrorString) {
  EXPECT_EQ(Util::GetWin32Error(0), "The operation completed successfully.");
  EXPECT_EQ(Util::GetWin32Error(1), "Incorrect function.");
}
#endif

#if PLATFORM_WINDOWS
TEST(UtilTest, GetLastErrorString) {
  // Just get some coverage. We don't know the last error.
  EXPECT_NE(Util::GetLastWin32Error(), "");
}
#endif

TEST(UtilTest, GetStrError) {
#if PLATFORM_WINDOWS
  EXPECT_EQ(Util::GetStrError(0), "No error");
#else
  EXPECT_EQ(Util::GetStrError(0), "Success");
#endif
  EXPECT_EQ(Util::GetStrError(1), "Operation not permitted");
}

TEST(UtilTest, GetLastStrError) {
  // Just get some coverage. We don't know the last error.
  EXPECT_NE(Util::GetLastStrError(), "");
}

TEST(UtilTest, GetPid) {
  // Just get some coverage. Processes are guaranteed not to return int64 min.
  // On Linux, it is an int32, on Windows an uint32.
  EXPECT_NE(Util::GetPid(), std::numeric_limits<int64_t>::min());
}

TEST(UtilTest, GetConsoleWidth) {
  // Could be run from a console window, we don't know.
  EXPECT_GT(Util::GetConsoleWidth(), 0);
}

TEST(UtilTest, IsTTY) {
  // Just exercise code, we don't know how the test is run.
  Util::IsTTY();
}

TEST(UtilTest, IsExecutable) {
  static constexpr uint8_t kWindowsExe[] = {0x4d, 0x5a, 0x90, 0x00, 0x03, 0x00,
                                            0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
                                            0xff, 0xff, 0x00, 0x00};
  static constexpr uint8_t kLinuxElf[] = {0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01,
                                          0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                                          0x00, 0x00, 0x00, 0x00};
  static constexpr uint8_t kBashScript[] = {0x23, 0x21, 0x2f, 0x62, 0x69, 0x6e,
                                            0x2f, 0x73, 0x68, 0x0d, 0x0a, 0x0d,
                                            0x0a, 0x65, 0x63, 0x68};
  static constexpr uint8_t kSecretFile[] = {0x44, 0x45, 0x46, 0x47, 0x41, 0x41,
                                            0x48, 0x48, 0x48, 0x48, 0x41, 0x48,
                                            0x48, 0x48, 0x48, 0x41};

  EXPECT_TRUE(Util::IsExecutable(kWindowsExe, sizeof(kWindowsExe)));
  EXPECT_TRUE(Util::IsExecutable(kLinuxElf, sizeof(kLinuxElf)));
  EXPECT_TRUE(Util::IsExecutable(kBashScript, sizeof(kBashScript)));
  EXPECT_FALSE(Util::IsExecutable(kSecretFile, sizeof(kSecretFile)));
}

TEST(UtilTest, Sleep) {
  // Just get some coverage.
  Util::Sleep(0);
}

TEST(UtilTest, Utf8CodePointLen) {
  EXPECT_EQ(Util::Utf8CodePointLen(u8""), 0);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"a"), 1);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"ab"), 1);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"\u0200"), 2);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"\u5000"), 3);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"\U00010000"), 4);
  EXPECT_EQ(Util::Utf8CodePointLen(u8"\u0200 only the first char counts"), 2);

  // Corrupt some UTF8 character.
  char broken[] = u8"\U00010000";
  broken[strlen(broken) - 1] = 'd';
  EXPECT_EQ(Util::Utf8CodePointLen(broken), 0);
}

TEST(UtilTest, SplitStringSkipEmpty) {
  EXPECT_EQ(SplitString(std::string(""), '/', false), StringList());
  EXPECT_EQ(SplitString(std::string("a"), '/', false), StringList({"a"}));
  EXPECT_EQ(SplitString(std::string("a/b"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("a//b"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("/a/b"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("a/b/"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("/a/b/"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("//a//b//"), '/', false),
            StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("aa/bb/cc"), '/', false),
            StringList({"aa", "bb", "cc"}));
}

TEST(UtilTest, SplitStringKeepEmpty) {
  EXPECT_EQ(SplitString(std::string(""), ',', true), StringList());
  EXPECT_EQ(SplitString(std::string("a"), ',', true), StringList({"a"}));
  EXPECT_EQ(SplitString(std::string("a,b"), ',', true), StringList({"a", "b"}));
  EXPECT_EQ(SplitString(std::string("a,b,"), ',', true),
            StringList({"a", "b", ""}));
  EXPECT_EQ(SplitString(std::string("a,,c"), ',', true),
            StringList({"a", "", "c"}));
  EXPECT_EQ(SplitString(std::string(",b,c"), ',', true),
            StringList({"", "b", "c"}));
  EXPECT_EQ(SplitString(std::string(",,"), ',', true),
            StringList({"", "", ""}));
  EXPECT_EQ(SplitString(std::string("aa,bb,"), ',', true),
            StringList({"aa", "bb", ""}));
  EXPECT_EQ(SplitString(std::string("aa,,cc"), ',', true),
            StringList({"aa", "", "cc"}));
  EXPECT_EQ(SplitString(std::string(",bb,cc"), ',', true),
            StringList({"", "bb", "cc"}));
}

TEST(UtilTest, JoinStrings) {
  EXPECT_EQ(JoinStrings(StringList(), ','), std::string());
  EXPECT_EQ(JoinStrings(StringList({"a"}), ','), "a");
  EXPECT_EQ(JoinStrings(StringList({"a", "b"}), ','), "a,b");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), ','), "a,b,c");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), ' '), "a b c");
  EXPECT_EQ(JoinStrings(StringList({",", ","}), ','), ",,,");
  EXPECT_EQ(JoinStrings(StringList({"", ""}), ','), ",");
}

TEST(UtilTest, JoinStringsPartial) {
  EXPECT_EQ(JoinStrings(StringList(), 1, 2, ','), std::string());
  EXPECT_EQ(JoinStrings(StringList(), 2, 1, ','), std::string());
  EXPECT_EQ(JoinStrings(StringList({"a"}), 0, 0, ','), "");
  EXPECT_EQ(JoinStrings(StringList({"a"}), 0, 1, ','), "a");
  EXPECT_EQ(JoinStrings(StringList({"a"}), 0, 2, ','), "a");
  EXPECT_EQ(JoinStrings(StringList({"a"}), 1, 0, ','), "");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 0, 1, ','), "a");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 1, 2, ','), "b");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 2, 3, ','), "c");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 0, 2, ','), "a,b");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 0, 3, ','), "a,b,c");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 1, 3, ','), "b,c");
  EXPECT_EQ(JoinStrings(StringList({"a", "b", "c"}), 0, 10, ','), "a,b,c");
}

TEST(UtilTest, HumanBytes) {
  EXPECT_EQ(HumanBytes(42), "42 bytes");
  EXPECT_EQ(HumanBytes(42ull << 10), "42 KB");
  EXPECT_EQ(HumanBytes(42ull << 20), "42 MB");
  EXPECT_EQ(HumanBytes(42ull << 30), "42 GB");
  EXPECT_EQ(HumanBytes(42ull << 40), "42 TB");
  EXPECT_EQ(HumanBytes(42ull << 50), "42 PB");

  EXPECT_EQ(HumanBytes(42.5 * 1024), "42 KB");
  EXPECT_EQ(HumanBytes(42.5 * 1024, 0), "42 KB");
  EXPECT_EQ(HumanBytes(42.5 * 1024, 1), "42.5 KB");
  EXPECT_EQ(HumanBytes(42.5 * 1024, 2), "42.50 KB");
  // Special case: values returned in byte and the precision is ignored, as it
  // does not make sense for human-readable bytes to be fractional.
  EXPECT_EQ(HumanBytes(42.5, 2), "42 bytes");
}

TEST(UtilTest, HumanDuration) {
  EXPECT_EQ(HumanDuration(absl::Duration()), "00:00");
  EXPECT_EQ(HumanDuration(absl::Seconds(5)), "00:05");
  EXPECT_EQ(HumanDuration(absl::Seconds(59)), "00:59");
  EXPECT_EQ(HumanDuration(absl::Seconds(359)), "05:59");
  EXPECT_EQ(HumanDuration(absl::Seconds(12 * 60 + 34)), "12:34");
  EXPECT_EQ(HumanDuration(absl::Minutes(2)), "02:00");
  EXPECT_EQ(HumanDuration(absl::Minutes(42)), "42:00");
  EXPECT_EQ(HumanDuration(absl::Minutes(123)), "123:00");
}

TEST(UtilTest, FinalSetter) {
  int i = 0;
  {
    FinalSetter<int> fs(&i, 42);
    EXPECT_EQ(i, 0);
  }
  EXPECT_EQ(i, 42);

  std::string s = "foo";
  {
    FinalSetter<std::string> fs(&s, "bar");
    EXPECT_EQ(s, "foo");
  }
  EXPECT_EQ(s, "bar");
}

TEST(UtilTest, GenerateUniqueId) {
  std::string id = Util::GenerateUniqueId();
  EXPECT_EQ(id.size(), 36);
  for (int i = 0; i < 36; ++i) {
    // Check that dashes are on correct positions.
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      EXPECT_EQ(id[i], '-');
    } else {
      EXPECT_TRUE(id[i] >= '0' && id[i] <= '9' || id[i] >= 'A' && id[i] <= 'F');
    }
  }

  EXPECT_NE(Util::GenerateUniqueId(), id);
}

// Can't cover Util::WaitForDebugger(), obviously.

}  // namespace
}  // namespace cdc_ft
