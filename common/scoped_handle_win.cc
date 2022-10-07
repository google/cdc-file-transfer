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

#include "common/scoped_handle_win.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <cassert>
#include <memory>

namespace cdc_ft {

ScopedHandle::ScopedHandle() : handle_(nullptr) {}

ScopedHandle::ScopedHandle(HANDLE handle) : handle_(nullptr) { Set(handle); }

ScopedHandle::ScopedHandle(ScopedHandle&& other) : handle_(nullptr) {
  Set(other.Release());
}

ScopedHandle::~ScopedHandle() { Close(); }

bool ScopedHandle::IsValid() const { return IsHandleValid(handle_); }

ScopedHandle& ScopedHandle::operator=(ScopedHandle&& other) {
  Set(other.Release());
  return *this;
}

void ScopedHandle::Set(HANDLE handle) {
  if (handle_ != handle) {
    // Preserve old error code.
    uint32_t last_error = GetLastError();
    Close();

    if (IsHandleValid(handle)) {
      handle_ = handle;
    }
    SetLastError(last_error);
  }
}

HANDLE ScopedHandle::Get() const {
  assert(IsValid());
  return handle_;
}

HANDLE ScopedHandle::Release() {
  HANDLE temp = handle_;
  handle_ = nullptr;
  return temp;
}

void ScopedHandle::Close() {
  if (IsHandleValid(handle_)) {
    CloseHandle(handle_);
    handle_ = nullptr;
  }
}

bool ScopedHandle::IsHandleValid(HANDLE handle) {
  return handle != nullptr && handle != INVALID_HANDLE_VALUE;
}

}  // namespace cdc_ft
