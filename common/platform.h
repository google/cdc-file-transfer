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

#ifndef COMMON_PLATFORM_H_
#define COMMON_PLATFORM_H_

#undef PLATFORM_LINUX
#undef PLATFORM_WINDOWS
#ifdef __linux__
#define PLATFORM_LINUX 1
#define PLATFORM_NAME "Linux"
#elif _WIN32
#define PLATFORM_WINDOWS 1
#define PLATFORM_NAME "Windows"
#endif

#endif  // COMMON_PLATFORM_H_
