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

#include "common/ansi_filter.h"

#include "absl/strings/ascii.h"
#include "gtest/gtest.h"

namespace cdc_ft {
namespace {

// Actual sample output from running SSH with -tt on Windows.
// Note the \0 after cmd.exe.
constexpr char kSshOutput[] =
    "\x1b[2J\x1b[?25l\x1b[m\x1b["
    "H\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n"
    "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\x1b[H\x1b]0;c:"
    "\\windows\\system32\\cmd.exe\0\a\x1b[?25h\x1b[?25'l\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b["
    "120C\r\n\x1b[120X\x1b[120C\r\n\x1b[120X\x1b[120C\x1b[H\x1b[?25h "
    "\x1b[H\x1b[?25l\r\nfoo";

TEST(AnsiFilterTest, DoesNotExplodeOnEmptyString) {
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences(""), "");
}

TEST(AnsiFilterTest, KeepsUnescapedString) {
  constexpr char kStr[] = "Lorem ipsum";
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences(kStr), kStr);
}

TEST(AnsiFilterTest, RemovesDeviceControlString) {
  // Special commands for the device.
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x1bPparams\x1b\\bar"),
            "foobar");
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x90params\x9c"
                                               "bar"),
            "foobar");
}

TEST(AnsiFilterTest, RemovesControlSequenceIntroducer) {
  // E.g. the well-known regular ANSI color codes.
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x1b[01;32mbar"), "foobar");
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x9b"
                                               "01;32mbar"),
            "foobar");
}

TEST(AnsiFilterTest, RemovesOperatingSystemCommand) {
  // E.g. setting the Window title.
  // Not cool: OS commands can contain null-terminated string.
  std::string str = "foo\x1b]0;c:\\path\\to\\foo.exe";
  str.append(1, '\0');
  str.append("\abar");
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences(str), "foobar");
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x9dstring\x1b\\bar"),
            "foobar");
}

TEST(AnsiFilterTest, RemovesRestIfNotTerminated) {
  EXPECT_EQ(ansi_filter::RemoveEscapeSequences("foo\x1b[01;32"), "foo");
}

TEST(AnsiFilterTest, RemovesSequencesFromActualSshOutput) {
  // Note: Can't just say str = kSshOutput because of the \0 in the string.
  std::string str = std::string(kSshOutput, sizeof(kSshOutput) - 1);
  std::string res = std::string(
      absl::StripAsciiWhitespace(ansi_filter::RemoveEscapeSequences(str)));
  EXPECT_EQ(res, "foo");
}

}  // namespace
}  // namespace cdc_ft
