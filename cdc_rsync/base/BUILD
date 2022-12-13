load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = [
    "//:__subpackages__",
])

cc_library(
    name = "cdc_interface",
    srcs = ["cdc_interface.cc"],
    hdrs = ["cdc_interface.h"],
    deps = [
        ":message_pump",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:buffer",
        "//common:log",
        "//common:path",
        "//common:status",
        "//common:threadpool",
        "//fastcdc",
        "@com_github_blake3//:blake3",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "cdc_interface_test",
    srcs = ["cdc_interface_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/cdc_interface/**"]),
    deps = [
        ":cdc_interface",
        ":fake_socket",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "fake_socket",
    srcs = ["fake_socket.cc"],
    hdrs = ["fake_socket.h"],
    deps = [
        "//cdc_rsync/base:socket",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "message_pump",
    srcs = ["message_pump.cc"],
    hdrs = ["message_pump.h"],
    deps = [
        ":socket",
        "//common:buffer",
        "//common:log",
        "//common:status",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_test(
    name = "message_pump_test",
    srcs = ["message_pump_test.cc"],
    deps = [
        ":fake_socket",
        ":message_pump",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "server_exit_code",
    hdrs = ["server_exit_code.h"],
)

cc_library(
    name = "socket",
    srcs = ["socket.cc"],
    hdrs = ["socket.h"],
    deps = [
        "//common:log",
        "//common:platform",
        "//common:status",
        "//common:util",
        "@com_google_absl//absl/status",
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
