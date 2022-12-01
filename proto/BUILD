load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@rules_cc//cc:defs.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "manifest_proto",
    srcs = ["manifest.proto"],
)

cc_proto_library(
    name = "manifest_cc_proto",
    deps = [":manifest_proto"],
)

proto_library(
    name = "asset_stream_service_proto",
    srcs = [
        "asset_stream_service.proto",
    ],
    visibility = ["//visibility:private"],
    deps = [":manifest_proto"],
)

cc_proto_library(
    name = "asset_stream_service_cc_proto",
    deps = [":asset_stream_service_proto"],
)

cc_grpc_library(
    name = "asset_stream_service_grpc_proto",
    srcs = [":asset_stream_service_proto"],
    grpc_only = True,
    deps = [
        ":asset_stream_service_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "background_service_proto",
    srcs = [
        "background_service.proto",
    ],
    deps = [
        "@com_google_protobuf//:empty_proto",
    ],
)

cc_proto_library(
    name = "background_service_cc_proto",
    deps = [":background_service_proto"],
)

cc_grpc_library(
    name = "background_service_grpc_proto",
    srcs = [":background_service_proto"],
    grpc_only = True,
    deps = [
        ":background_service_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_protobuf//:protobuf",
    ],
)

proto_library(
    name = "local_assets_stream_manager_proto",
    srcs = ["local_assets_stream_manager.proto"],
    deps = ["@com_google_protobuf//:empty_proto"],
)

cc_proto_library(
    name = "local_assets_stream_manager_cc_proto",
    deps = [":local_assets_stream_manager_proto"],
)

cc_grpc_library(
    name = "local_assets_stream_manager_grpc_proto",
    srcs = [":local_assets_stream_manager_proto"],
    grpc_only = True,
    deps = [
        ":local_assets_stream_manager_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_protobuf//:protobuf",
    ],
)
