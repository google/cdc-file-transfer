load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//:__subpackages__"])

cc_library(
    name = "content_id",
    srcs = ["content_id.cc"],
    hdrs = ["content_id.h"],
    deps = [
        ":manifest_proto_defs",
        "@com_github_blake3//:blake3",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "content_id_test",
    srcs = ["content_id_test.cc"],
    deps = [
        ":content_id",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "manifest_proto_defs",
    hdrs = ["manifest_proto_defs.h"],
    deps = ["//proto:manifest_cc_proto"],
)

cc_library(
    name = "fake_manifest_builder",
    srcs = ["fake_manifest_builder.cc"],
    hdrs = ["fake_manifest_builder.h"],
    deps = [
        ":manifest_proto_defs",
        "//common:path",
        "//data_store:mem_data_store",
        "//fastcdc",
    ],
)

cc_test(
    name = "fake_manifest_builder_test",
    srcs = ["fake_manifest_builder_test.cc"],
    deps = [
        ":fake_manifest_builder",
        "//common:status_test_macros",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "manifest_builder",
    srcs = [
        "asset_builder.cc",
        "manifest_builder.cc",
    ],
    hdrs = [
        "asset_builder.h",
        "manifest_builder.h",
    ],
    deps = [
        ":content_id",
        ":manifest_proto_defs",
        "//common:log",
        "//common:path",
        "//common:status",
        "//common:status_macros",
        "//common:util",
        "//data_store",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "manifest_builder_test",
    srcs = ["manifest_builder_test.cc"],
    deps = [
        ":manifest_builder",
        ":manifest_iterator",
        ":manifest_printer",
        "//common:status_test_macros",
        "//data_store:mem_data_store",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "manifest_iterator",
    srcs = ["manifest_iterator.cc"],
    hdrs = ["manifest_iterator.h"],
    deps = [
        "//common:log",
        "//common:path",
        "//data_store",
    ],
)

cc_library(
    name = "manifest_printer",
    srcs = ["manifest_printer.cc"],
    hdrs = ["manifest_printer.h"],
    deps = [
        ":content_id",
        ":manifest_proto_defs",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "manifest_updater",
    srcs = [
        "manifest_updater.cc",
        "pending_assets_queue.cc",
    ],
    hdrs = [
        "manifest_updater.h",
        "pending_assets_queue.h",
    ],
    # Tests don't work under Linux, but we only need it on Windows, anyway.
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":file_chunk_map",
        ":manifest_builder",
        ":manifest_iterator",
        ":manifest_proto_defs",
        ":stats_printer",
        "//common:log",
        "//common:path",
        "//common:stopwatch",
        "//common:threadpool",
        "//common:util",
        "//data_store",
        "//fastcdc",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "stats_printer",
    srcs = ["stats_printer.cc"],
    hdrs = ["stats_printer.h"],
    copts = select({
        "//tools:windows": ["/wd4324"],  # "structure was padded" from flat_hash_map
        "//conditions:default": [],
    }),
    deps = [
        "//common:path",
        "//common:stopwatch",
        "//fastcdc",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "file_chunk_map",
    srcs = ["file_chunk_map.cc"],
    hdrs = ["file_chunk_map.h"],
    copts = select({
        "//tools:windows": ["/wd4324"],  # "structure was padded" from flat_hash_map
        "//conditions:default": [],
    }),
    deps = [
        ":manifest_proto_defs",
        ":stats_printer",
        "//manifest:content_id",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "file_chunk_map_test",
    srcs = ["file_chunk_map_test.cc"],
    deps = [
        ":file_chunk_map",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "manifest_test_base",
    srcs = ["manifest_test_base.cc"],
    hdrs = ["manifest_test_base.h"],
    deps = [
        ":manifest_iterator",
        ":manifest_printer",
        ":manifest_updater",
        "//common:path",
        "//common:status_test_macros",
        "//data_store:mem_data_store",
        "@com_google_googletest//:gtest",
    ],
)

# This test only succeeds on Windows if the timezone is set to the local host's
# timezone, but Bazel by default sets the test timezone to UTC.
#
# Run this test as follows to preserve the host's timezone:
#   bazel test --action_env=TZ=Local
cc_test(
    name = "manifest_updater_test",
    srcs = ["manifest_updater_test.cc"],
    data = [":all_test_data"],
    deps = [
        ":manifest_test_base",
        ":manifest_updater",
        "//common:test_main",
        "//data_store:mem_data_store",
        "@com_google_googletest//:gtest",
    ],
)

filegroup(
    name = "all_test_sources",
    srcs = glob(["*_test.cc"]),
)

filegroup(
    name = "all_test_data",
    srcs = glob(["testdata/**"]),
)
