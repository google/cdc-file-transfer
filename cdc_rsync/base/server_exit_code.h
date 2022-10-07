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

#ifndef CDC_RSYNC_BASE_SERVER_EXIT_CODE_H_
#define CDC_RSYNC_BASE_SERVER_EXIT_CODE_H_

namespace cdc_ft {

// Since the client cannot distinguish between stderr and stdout (ssh.exe sends
// both to stdout), the server marks the beginning and ending of error messages
// with this marker char. The client interprets everything in between as an
// error message.
constexpr char kServerErrorMarker = 0x1e;

enum ServerExitCode {
  // Pick a range of exit codes that does not overlap with unrelated exit codes
  // like bash exit codes.
  // - 126: error from bash when binary can't be started (permission denied).
  // - 127: error from bash when binary isn't found
  // - 255: ssh.exe error code.
  // Note that codes must be <= 255.

  // KEEP UPDATED!
  kServerExitCodeMin = 50,

  // Generic error on startup, before out-of-date check, e.g. bad args.
  kServerExitCodeGenericStartup = 50,

  // A gamelet component is outdated and needs to be re-uploaded.
  kServerExitCodeOutOfDate = 51,

  //
  // All other exit codes must be strictly bigger than kServerErrorOutOfDate.
  // They are guaranteed to be past the out-of-date check.
  //

  // Unspecified error.
  kServerExitCodeGeneric = 52,

  // Binding to the forward port failed, probably because there's another
  // instance of cdc_rsync running.
  kServerExitCodeAddressInUse = 53,

  // KEEP UPDATED!
  kServerExitCodeMax = 53,
};

}  // namespace cdc_ft

#endif  // CDC_RSYNC_BASE_SERVER_EXIT_CODE_H_
