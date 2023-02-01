/*
 * Copyright 2023 Google LLC
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

#ifndef COMMON_ARCH_TYPE_H_
#define COMMON_ARCH_TYPE_H_

namespace cdc_ft {

enum class ArchType {
  kWindows_x86_64 = 0,
  kLinux_x86_64 = 1,
};

// Returns the arch type of the current process.
ArchType GetLocalArchType();

// Returns true if |arch_type| is a Windows operating system.
bool IsWindowsArchType(ArchType arch_type);

// Returns true if |arch_type| is a Linux operating system.
bool IsLinuxArchType(ArchType arch_type);

// Returns a human readable string for |arch_type|.
const char* GetArchTypeStr(ArchType arch_type);

}  // namespace cdc_ft

#endif  // COMMON_ARCH_TYPE_H_
