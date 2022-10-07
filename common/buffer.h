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

#ifndef COMMON_BUFFER_H_
#define COMMON_BUFFER_H_

#include <cstdlib>
#include <initializer_list>

namespace cdc_ft {

// Buffer for generic data blobs. It is a managed and dynamically sized memory
// buffer with an interface similar to that of std::vector. It can be used as an
// in-place replacement in some use cases like reading files.
// The class solves a performance problem on Linux where resize() can be
// relatively expensive since a vector default-constructs elements. This can
// be a big performance bottleneck in some cases.
class Buffer {
 public:
  Buffer();
  explicit Buffer(size_t size);
  Buffer(std::initializer_list<char> list);

  ~Buffer();

  // Non-copyable, non-assignable for safety.
  Buffer(const Buffer& other) = delete;
  Buffer& operator=(const Buffer& other) = delete;

  Buffer(Buffer&& other) noexcept;
  Buffer& operator=(Buffer&& other) noexcept;

  bool operator==(const Buffer& other) const;
  bool operator!=(const Buffer& other) const;

  // Resizes the buffer. Only reallocates if the size increases. In that case,
  // allocates 1.5x more than required. If size shrinks, keeps the buffer.
  // Does not initialize the buffer.
  void resize(size_t new_size);

  // Makes sure the buffer capacity is at least the given number of bytes.
  void reserve(size_t capacity);

  // Returns the current size of the buffer.
  size_t size() const { return size_; }

  // Returns true if the size is zero.
  bool empty() const { return size_ == 0; }

  // Returns the currently allocated buffer size.
  size_t capacity() const { return capacity_; }

  // Resizes the buffer to 0.
  void clear() { size_ = 0; }

  // Returns the data pointer.
  char* data() { return data_; }

  // Returns the data pointer.
  const char* data() const { return data_; }

  // Appends |data| of size |data_size| to the end of the buffer. The behavior
  // is undefined if the provided memory range overlaps with the buffer's data.
  void append(const void* data, size_t data_size);

 private:
  char* data_ = nullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
};

}  // namespace cdc_ft

#endif  // COMMON_BUFFER_H_
