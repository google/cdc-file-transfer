load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

package(default_visibility = ["//:__subpackages__"])

cc_binary(
    name = "cdc_fuse_fs",
    srcs = ["main.cc"],
    deps = [
        ":cdc_fuse_fs_lib",
        ":constants",
        "//absl_helper:jedec_size_flag",
        "//common:gamelet_component",
        "//common:log",
        "//data_store:data_provider",
        "//data_store:disk_data_store",
        "//data_store:grpc_reader",
        "@com_google_absl//absl/flags:parse",
    ],
)

# Dependencies for cdc_fuse_fs_lib, except for FUSE.
cdc_fuse_fs_lib_shared_deps = [
    ":asset",
    ":asset_stream_client",
    ":config_stream_client",
    "//common:log",
    "//common:path",
    "//common:platform",
    "//common:util",
    "//common:threadpool",
    "@com_github_jsoncpp//:jsoncpp",
]

cc_library(
    name = "cdc_fuse_fs_lib",
    srcs = ["cdc_fuse_fs.cc"],
    hdrs = ["cdc_fuse_fs.h"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = cdc_fuse_fs_lib_shared_deps + ["@com_github_fuse//:fuse_shared"],
)

cc_library(
    name = "cdc_fuse_fs_lib_mocked",
    srcs = ["cdc_fuse_fs.cc"],
    hdrs = ["cdc_fuse_fs.h"],
    copts = ["-DUSE_MOCK_LIBFUSE=1"],
    deps = cdc_fuse_fs_lib_shared_deps + [":mock_libfuse"],
)

cc_test(
    name = "cdc_fuse_fs_test",
    srcs = ["cdc_fuse_fs_test.cc"],
    deps = [
        ":cdc_fuse_fs_lib_mocked",
        ":mock_config_stream_client",
        "//common:status_test_macros",
        "//data_store",
        "//data_store:mem_data_store",
        "//manifest:fake_manifest_builder",
        "//manifest:manifest_builder",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "mock_libfuse",
    srcs = ["mock_libfuse.cc"],
    hdrs = ["mock_libfuse.h"],
    deps = ["//common:platform"],
)

cc_library(
    name = "constants",
    hdrs = ["constants.h"],
)

cc_library(
    name = "asset_stream_client",
    srcs = ["asset_stream_client.cc"],
    hdrs = ["asset_stream_client.h"],
    deps = [
        "//common:log",
        "//common:status_macros",
        "//common:stopwatch",
        "//manifest:manifest_proto_defs",
        "//proto:asset_stream_service_grpc_proto",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "asset",
    srcs = ["asset.cc"],
    hdrs = ["asset.h"],
    deps = [
        "//common:buffer",
        "//common:status",
        "//data_store",
        "//manifest:content_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "asset_test",
    srcs = ["asset_test.cc"],
    deps = [
        ":asset",
        "//common:path",
        "//common:platform",
        "//common:status_test_macros",
        "//data_store:mem_data_store",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "config_stream_client",
    srcs = ["config_stream_client.cc"],
    hdrs = ["config_stream_client.h"],
    deps = [
        "//common:grpc_status",
        "//common:log",
        "//manifest:content_id",
        "//proto:asset_stream_service_grpc_proto",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "mock_config_stream_client",
    srcs = ["mock_config_stream_client.cc"],
    hdrs = ["mock_config_stream_client.h"],
    deps = [":config_stream_client"],
)

filegroup(
    name = "all_test_sources",
    srcs = glob(["*_test.cc"]),
)
