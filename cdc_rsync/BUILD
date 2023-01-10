load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

package(default_visibility = [
    "//:__subpackages__",
])

cc_binary(
    name = "cdc_rsync",
    srcs = ["main.cc"],
    deps = [
        ":cdc_rsync_client",
        ":params",
    ],
)

cc_library(
    name = "client_file_info",
    hdrs = ["client_file_info.h"],
)

cc_library(
    name = "client_socket",
    srcs = ["client_socket.cc"],
    hdrs = ["client_socket.h"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        "//cdc_rsync/base:socket",
        "//common:log",
        "//common:status",
        "//common:util",
    ],
)

cc_library(
    name = "file_finder_and_sender",
    srcs = ["file_finder_and_sender.cc"],
    hdrs = ["file_finder_and_sender.h"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":client_file_info",
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:log",
        "//common:path",
        "//common:path_filter",
        "//common:platform",
        "//common:util",
    ],
)

cc_test(
    name = "file_finder_and_sender_test",
    srcs = ["file_finder_and_sender_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/file_finder_and_sender/**"]),
    deps = [
        ":file_finder_and_sender",
        "//cdc_rsync/base:fake_socket",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "cdc_rsync_client",
    srcs = ["cdc_rsync_client.cc"],
    hdrs = ["cdc_rsync_client.h"],
    linkopts = select({
        "//tools:windows": [
            "/DEFAULTLIB:Ws2_32.lib",  # Sockets, e.g. recv, send, WSA*.
        ],
        "//conditions:default": [],
    }),
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":client_socket",
        ":file_finder_and_sender",
        ":parallel_file_opener",
        ":progress_tracker",
        ":server_arch",
        ":zstd_stream",
        "//cdc_rsync/base:cdc_interface",
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync/base:server_exit_code",
        "//cdc_rsync/base:socket",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:gamelet_component",
        "//common:log",
        "//common:path",
        "//common:path_filter",
        "//common:platform",
        "//common:port_manager",
        "//common:process",
        "//common:remote_util",
        "//common:status",
        "//common:status_macros",
        "//common:threadpool",
        "//common:util",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "parallel_file_opener",
    srcs = ["parallel_file_opener.cc"],
    hdrs = ["parallel_file_opener.h"],
    data = ["testdata/root.txt"] + glob(["testdata/parallel_file_opener/**"]),
    deps = [
        ":client_file_info",
        "//common:path",
        "//common:platform",
        "//common:threadpool",
    ],
)

cc_test(
    name = "parallel_file_opener_test",
    srcs = ["parallel_file_opener_test.cc"],
    deps = [
        ":parallel_file_opener",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "params",
    srcs = ["params.cc"],
    hdrs = ["params.h"],
    deps = [
        ":cdc_rsync_client",
        "//common:port_range_parser",
        "@com_github_zstd//:zstd",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "params_test",
    srcs = ["params_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/params/**"]),
    deps = [
        ":params",
        "//common:test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "progress_tracker",
    srcs = ["progress_tracker.cc"],
    hdrs = ["progress_tracker.h"],
    deps = [
        ":file_finder_and_sender",
        "//cdc_rsync/base:cdc_interface",
        "//common:stopwatch",
        "@com_github_jsoncpp//:jsoncpp",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "progress_tracker_test",
    srcs = ["progress_tracker_test.cc"],
    deps = [
        ":progress_tracker",
        "//cdc_rsync/protos:messages_cc_proto",
        "//common:test_main",
        "//common:testing_clock",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "server_arch",
    srcs = ["server_arch.cc"],
    hdrs = ["server_arch.h"],
    deps = [
        "//common:path",
        "//common:remote_util",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "server_arch_test",
    srcs = ["server_arch_test.cc"],
    deps = [
        ":server_arch",
        "//common:test_main",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "zstd_stream",
    srcs = ["zstd_stream.cc"],
    hdrs = ["zstd_stream.h"],
    deps = [
        ":client_socket",
        "//common:buffer",
        "//common:status",
        "//common:status_macros",
        "//common:stopwatch",
        "@com_github_zstd//:zstd",
    ],
)

cc_test(
    name = "zstd_stream_test",
    srcs = ["zstd_stream_test.cc"],
    deps = [
        ":zstd_stream",
        "//cdc_rsync/base:fake_socket",
        "//cdc_rsync_server:unzstd_stream",
        "//common:status_test_macros",
        "//common:test_main",
        "@com_github_zstd//:zstd",
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
