load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//:__subpackages__"])

cc_library(
    name = "data_store",
    srcs = [
        "data_store_reader.cc",
        "data_store_writer.cc",
    ],
    hdrs = [
        "data_store_reader.h",
        "data_store_writer.h",
    ],
    deps = [
        "//common:buffer",
        "//common:status",
        "//common:status_macros",
        "//manifest:content_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "data_provider",
    srcs = ["data_provider.cc"],
    hdrs = ["data_provider.h"],
    deps = [
        ":data_store",
        "//common:clock",
        "//common:log",
        "//common:status",
        "//common:stopwatch",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "disk_data_store",
    srcs = ["disk_data_store.cc"],
    hdrs = ["disk_data_store.h"],
    deps = [
        ":data_store",
        "//common:clock",
        "//common:log",
        "//common:path",
        "//common:platform",
        "//common:status_macros",
        "//manifest:content_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "disk_data_store_test",
    srcs = ["disk_data_store_test.cc"],
    deps = [
        ":disk_data_store",
        "//common:status_test_macros",
        "//common:testing_clock",
        "//manifest:content_id",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "data_provider_test",
    srcs = ["data_provider_test.cc"],
    deps = [
        ":data_provider",
        ":disk_data_store",
        ":mem_data_store",
        "//common:status_test_macros",
        "//common:testing_clock",
        "//common:util",
        "//manifest:content_id",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "grpc_reader",
    srcs = ["grpc_reader.cc"],
    hdrs = ["grpc_reader.h"],
    deps = [
        ":data_store",
        "//cdc_fuse_fs:asset_stream_client",
        "//common:buffer",
        "//common:status",
        "//common:status_macros",
        "//manifest:content_id",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "mem_data_store",
    srcs = ["mem_data_store.cc"],
    hdrs = ["mem_data_store.h"],
    deps = [
        ":data_store",
        "//common:status",
    ],
)

cc_test(
    name = "mem_data_store_test",
    srcs = ["mem_data_store_test.cc"],
    deps = [
        ":mem_data_store",
        "//common:status_test_macros",
        "//manifest:content_id",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

filegroup(
    name = "all_test_sources",
    srcs = glob(["*_test.cc"]),
)
