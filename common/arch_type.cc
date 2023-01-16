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

#include "common/arch_type.h"

#include <cassert>

#include "common/platform.h"

namespace cdc_ft {

static constexpr char kUnhandledArchType[] = "Unhandled arch type";

ArchType GetLocalArchType() {
  // TODO(ljusten): Take CPU architecture into account.
#if PLATFORM_WINDOWS
  return ArchType::kWindows_x86_64;
#elif PLATFORM_LINUX
  return ArchType::kLinux_x86_64;
#endif
}

bool IsWindowsArchType(ArchType arch_type) {
  switch (arch_type) {
    case ArchType::kWindows_x86_64:
      return true;
    case ArchType::kLinux_x86_64:
      return false;
    default:
      assert(!kUnhandledArchType);
      return false;
  }
}

bool IsLinuxArchType(ArchType arch_type) {
  switch (arch_type) {
    case ArchType::kWindows_x86_64:
      return false;
    case ArchType::kLinux_x86_64:
      return true;
    default:
      assert(!kUnhandledArchType);
      return false;
  }
}

const char* GetArchTypeStr(ArchType arch_type) {
  switch (arch_type) {
    case ArchType::kWindows_x86_64:
      return "Windows x86_64";
    case ArchType::kLinux_x86_64:
      return "Linux x86_64";
    default:
      assert(!kUnhandledArchType);
      return "Unknown";
  }
}

}  // namespace cdc_ft
