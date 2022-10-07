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

#ifndef COMMON_SCOPED_HANDLE_WIN_H_
#define COMMON_SCOPED_HANDLE_WIN_H_

using HANDLE = void*;

namespace cdc_ft {

// In Windows, some functions return nullptr as invalid handles, some
// INVALID_HANDLE_VALUE. This class attempts to minimize this pain.
class ScopedHandle {
 public:
  ScopedHandle();

  explicit ScopedHandle(HANDLE handle);

  ScopedHandle(ScopedHandle&& other);

  ~ScopedHandle();

  ScopedHandle(const ScopedHandle&) = delete;
  ScopedHandle& operator=(const ScopedHandle&) = delete;

  bool IsValid() const;

  ScopedHandle& operator=(ScopedHandle&& other);

  void Set(HANDLE handle);

  HANDLE Get() const;

  // Transfers ownership away from this object.
  HANDLE Release();

  // Explicitly closes the owned handle.
  void Close();

 private:
  static bool IsHandleValid(HANDLE handle);

  HANDLE handle_;
};

}  // namespace cdc_ft

#endif  // COMMON_SCOPED_HANDLE_WIN_H_
