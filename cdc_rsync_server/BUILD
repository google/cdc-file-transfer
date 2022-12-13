load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

package(default_visibility = [
    "//:__subpackages__",
])

cc_library(
    name = "file_deleter_and_sender",
    srcs = ["file_deleter_and_sender.cc"],
    hdrs = ["file_deleter_and_sender.h"],
    deps = [
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:path",
        "//common:status",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "file_deleter_and_sender_test",
    srcs = ["file_deleter_and_sender_test.cc"],
    deps = [
        ":file_deleter_and_sender",
        "//cdc_rsync/base:fake_socket",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "file_finder",
    srcs = ["file_finder.cc"],
    hdrs = ["file_finder.h"],
    deps = [
        ":file_info",
        "//common:path",
        "//common:path_filter",
        "//common:status",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "file_finder_test",
    srcs = ["file_finder_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/file_finder/**"]),
    deps = [
        ":file_finder",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "file_diff_generator",
    srcs = ["file_diff_generator.cc"],
    hdrs = ["file_diff_generator.h"],
    deps = [
        ":file_info",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:log",
        "//common:path",
        "//common:util",
    ],
)

cc_test(
    name = "file_diff_generator_test",
    srcs = ["file_diff_generator_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/file_diff_generator/**"]),
    deps = [
        ":file_diff_generator",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_binary(
    name = "cdc_rsync_server",
    srcs = [
        "cdc_rsync_server.cc",
        "cdc_rsync_server.h",
        "main.cc",
    ],
    copts = select({
        #":debug_build": ["-fstandalone-debug"],
        "//conditions:default": [],
    }),
    deps = [
        ":file_deleter_and_sender",
        ":file_diff_generator",
        ":file_finder",
        ":file_info",
        ":server_socket",
        ":unzstd_stream",
        "//cdc_rsync/base:cdc_interface",
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync/base:server_exit_code",
        "//common:clock",
        "//common:gamelet_component",
        "//common:log",
        "//common:path_filter",
        "//common:status",
        "//common:stopwatch",
        "//common:threadpool",
        "//common:util",
    ],
)

config_setting(
    name = "debug_build",
    values = {
        "compilation_mode": "dbg",
    },
)

cc_library(
    name = "file_info",
    hdrs = ["file_info.h"],
)

cc_library(
    name = "server_socket",
    srcs = ["server_socket.cc"],
    hdrs = ["server_socket.h"],
    linkopts = select({
        "//tools:windows": [
            "/DEFAULTLIB:Ws2_32.lib",  # Sockets, e.g. recv, send, WSA*.
        ],
        "//conditions:default": [],
    }),
    deps = [
        "//cdc_rsync/base:socket",
        "//common:log",
        "//common:status",
        "//common:util",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "unzstd_stream",
    srcs = ["unzstd_stream.cc"],
    hdrs = ["unzstd_stream.h"],
    deps = [
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync/base:socket",
        "//common:status",
        "@com_github_zstd//:zstd",
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
