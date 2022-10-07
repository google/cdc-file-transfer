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

#include "common/buffer.h"

#include <cstring>
#include <utility>

namespace cdc_ft {

Buffer::Buffer() = default;

Buffer::Buffer(size_t size) : size_(size) {
  reserve(size);
  resize(size);
}

Buffer::Buffer(std::initializer_list<char> list) {
  reserve(list.size());
  resize(list.size());
  memcpy(data_, list.begin(), list.size());
}

Buffer::Buffer(Buffer&& other) noexcept { *this = std::move(other); }

Buffer& Buffer::operator=(Buffer&& other) noexcept {
  if (data_) {
    free(data_);
  }

  size_ = other.size_;
  capacity_ = other.capacity_;
  data_ = other.data_;

  other.size_ = 0;
  other.capacity_ = 0;
  other.data_ = nullptr;

  return *this;
}

Buffer::~Buffer() {
  if (data_) {
    free(data_);
    data_ = nullptr;
    capacity_ = 0;
  }
}

bool Buffer::operator==(const Buffer& other) const {
  return size_ == other.size_ && memcmp(data_, other.data_, size_) == 0;
}

bool Buffer::operator!=(const Buffer& other) const { return !(*this == other); }

void Buffer::resize(size_t new_size) {
  size_ = new_size;
  if (capacity_ < size_) {
    capacity_ = size_ + size_ / 2;
    data_ = static_cast<char*>(realloc(data_, capacity_));
  }
}

void Buffer::reserve(size_t capacity) {
  if (capacity_ < capacity) {
    capacity_ = capacity;
    data_ = static_cast<char*>(realloc(data_, capacity_));
  }
}

void Buffer::append(const void* data, size_t data_size) {
  size_t prev_size = size_;
  resize(prev_size + data_size);
  memcpy(data_ + prev_size, data, data_size);
}

}  // namespace cdc_ft
