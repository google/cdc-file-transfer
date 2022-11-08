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

#ifndef CDC_RSYNC_ERROR_MESSAGES_H_
#define CDC_RSYNC_ERROR_MESSAGES_H_

namespace cdc_ft {

// Server connection timed out. SSH probably stale.
constexpr char kMsgFmtConnectionTimeout[] =
    "Server connection timed out. Verify that host '%s' and port '%i' are "
    "correct, or specify a larger timeout with --contimeout.";

// Server connection timed out and IP was not passed in. Probably network error.
constexpr char kMsgConnectionTimeoutWithIp[] =
    "Server connection timed out. Check your network connection.";

// Receiving pipe end was shut down unexpectedly.
constexpr char kMsgConnectionLost[] =
    "The connection to the instance was shut down unexpectedly.";

// Binding to the port failed.
constexpr char kMsgAddressInUse[] =
    "Failed to establish a connection to the instance. All ports are already "
    "in use. This can happen if another instance of this command is running. "
    "Currently, only 10 simultaneous connections are supported.";

// Deployment failed even though gamelet components were copied successfully.
constexpr char kMsgDeployFailed[] =
    "Failed to deploy the instance components for unknown reasons. "
    "Please report this issue.";

}  // namespace cdc_ft

#endif  // CDC_RSYNC_ERROR_MESSAGES_H_
