// Copyright 2023 Google LLC
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

namespace cdc_ft {
namespace ansi_filter {
namespace {
enum class State {
  kNotInSequence,
  kDCS,  // Starting with kESC + P or kDCSI, Device Control String.
  kCS,   // Starting with kESC + [ or kCSI,  Control Sequence.
  kOSC,  // Starting with kESC + ] or kOSCI, Operating System Command.
};

constexpr uint8_t kBEL = 0x07;   // Terminal bell.
constexpr uint8_t kESC = 0x1B;   // ANSI escape character.
constexpr uint8_t kST = 0x9C;    // String Terminator.
constexpr uint8_t kDCSI = 0x90;  // Device Control String Introducer.
constexpr uint8_t kCSI = 0x9B;   // Control Sequence Introducer.
constexpr uint8_t kOSCI = 0x9D;  // Operating System Command Introducer

}  // namespace

std::string RemoveEscapeSequences(const std::string& input) {
  State state = State::kNotInSequence;
  std::string result;

  for (size_t n = 0; n < input.size(); ++n) {
    uint8_t ch = static_cast<uint8_t>(input[n]);
    uint8_t next_ch =
        static_cast<uint8_t>(n + 1 < input.size() ? input[n + 1] : 0);

    switch (state) {
      case State::kNotInSequence:
        // Device Control String.
        if ((ch == kESC && next_ch == 'P') || ch == kDCSI) {
          n += ch == kESC ? 1 : 0;
          state = State::kDCS;
          break;
        }

        // Control Sequence.
        if ((ch == kESC && next_ch == '[') || ch == kCSI) {
          n += ch == kESC ? 1 : 0;
          state = State::kCS;
          break;
        }

        // Operating System Command.
        if ((ch == kESC && next_ch == ']') || ch == kOSCI) {
          n += ch == kESC ? 1 : 0;
          state = State::kOSC;
          break;
        }

        // Char does not belong to control sequence.
        result.push_back(ch);
        break;

      case State::kDCS:
        // Device control strings are ended by kST or ESC + \.
        if (ch == kST || (ch == kESC && next_ch == '\\')) {
          n += ch == kESC ? 1 : 0;
          state = State::kNotInSequence;
        }
        break;

      case State::kCS:
        // Control sequence initializer are ended by a byte in 0x40�0x7E.
        // https://en.wikipedia.org/wiki/ANSI_escape_code#CSIsection
        if (ch >= 0x40 && ch <= 0x7E) {
          state = State::kNotInSequence;
        }
        break;

      case State::kOSC:
        // Operating system commands are ended by kBEL, kST or ESC + \.
        // https://invisible-island.net/xterm/ctlseqs/ctlseqs.html#h3-Operating-System-Commands
        if (ch == kBEL || ch == kST || (ch == kESC && next_ch == '\\')) {
          n += ch == kESC ? 1 : 0;
          state = State::kNotInSequence;
        }
        break;
    }
  }

  return result;
}

}  // namespace ansi_filter
}  // namespace cdc_ft
