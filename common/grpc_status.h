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

#ifndef COMMON_GRPC_STATUS_H_
#define COMMON_GRPC_STATUS_H_

#include "absl/status/status.h"
#include "grpcpp/grpcpp.h"

namespace cdc_ft {

//
// gRPC status conversion convenience methods.
//

inline grpc::Status ToGrpcStatus(const absl::Status& status) {
  if (status.ok()) return grpc::Status::OK;
  return grpc::Status(static_cast<grpc::StatusCode>(status.code()),
                      std::string(status.message()));
}

inline absl::Status ToAbslStatus(const grpc::Status& status) {
  if (status.ok()) return absl::OkStatus();
  return absl::Status(static_cast<absl::StatusCode>(status.error_code()),
                      std::string(status.error_message()));
}

#define RETURN_GRPC_IF_ERROR(expr)                               \
  do {                                                           \
    absl::Status __absl_status = (expr);                         \
    if (!__absl_status.ok()) return ToGrpcStatus(__absl_status); \
  } while (0)

#define RETURN_ABSL_IF_ERROR(expr)                               \
  do {                                                           \
    grpc::Status __grpc_status = (expr);                         \
    if (!__grpc_status.ok()) return ToAbslStatus(__grpc_status); \
  } while (0)

}  // namespace cdc_ft

#endif  // COMMON_GRPC_STATUS_H_
