load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "buffer",
    srcs = ["buffer.cc"],
    hdrs = ["buffer.h"],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    deps = [
        ":buffer",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "clock",
    srcs = ["clock.cc"],
    hdrs = ["clock.h"],
    deps = [
        ":platform",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "dir_iter",
    srcs = ["dir_iter.cc"],
    hdrs = ["dir_iter.h"],
    copts = select({
        "//tools:windows": [
            # Additional warnings from @com_github_dirent
            "/wd4505",  # unreferenced function with internal linkage has been removed
        ],
        "//conditions:default": [],
    }),
    deps = [
        ":path",
        ":platform",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ] + select({
        "//tools:windows": ["@com_github_dirent//:dirent"],
        "//conditions:default": [],
    }),
)

cc_test(
    name = "dir_iter_test",
    srcs = ["dir_iter_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/dir_iter/**"]),
    deps = [
        ":dir_iter",
        ":status_test_macros",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "errno_mapping",
    srcs = ["errno_mapping.cc"],
    hdrs = ["errno_mapping.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "errno_mapping_test",
    srcs = ["errno_mapping_test.cc"],
    deps = [
        ":errno_mapping",
        ":status_test_macros",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "file_watcher",
    srcs = [
        "file_watcher_win.cc",
    ],
    hdrs = [
        "file_watcher_win.h",
    ],
    # Required for ReadDirectoryChangesExW (requires Win10 1709).
    copts = ["/D_WIN32_WINNT=0x0A00"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":log",
        ":path",
        ":platform",
        ":scoped_handle",
        ":status",
        ":stopwatch",
        ":util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "file_watcher_test",
    srcs = ["file_watcher_win_test.cc"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":file_watcher",
        ":path",
        ":platform",
        ":status_test_macros",
        ":util",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "grpc_status",
    hdrs = ["grpc_status.h"],
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "log",
    srcs = ["log.cc"],
    hdrs = ["log.h"],
    deps = [
        ":clock",
        ":platform",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "log_test",
    srcs = ["log_test.cc"],
    deps = [
        ":log",
        ":path",
        ":status_test_macros",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "path",
    srcs = ["path.cc"],
    hdrs = ["path.h"],
    linkopts = select({
        "//tools:windows": [
            "/DEFAULTLIB:ole32.lib",  # CoTaskMemFree
            "/DEFAULTLIB:shell32.lib",  # SHGetKnownFolderPath
        ],
        "//conditions:default": [],
    }),
    deps = [
        ":buffer",
        ":errno_mapping",
        ":log",
        ":platform",
        ":status",
        ":status_macros",
        ":util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "path_test",
    srcs = ["path_test.cc"],
    env_inherit = select({
        "//tools:windows": [
            "ProgramFiles(x86)",
            "USERPROFILE",
        ],
        "//conditions:default": [],
    }),
    deps = [
        ":path",
        ":status_test_macros",
        ":test_main",
        ":util",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "path_filter",
    srcs = ["path_filter.cc"],
    hdrs = ["path_filter.h"],
)

cc_test(
    name = "path_filter_test",
    srcs = ["path_filter_test.cc"],
    deps = [
        ":path_filter",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "platform",
    hdrs = ["platform.h"],
)

cc_library(
    name = "port_manager",
    srcs = ["port_manager_win.cc"],
    hdrs = ["port_manager.h"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":remote_util",
        ":status",
        ":stopwatch",
        ":util",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "port_manager_test",
    srcs = ["port_manager_test.cc"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":port_manager",
        ":status_test_macros",
        ":stub_process",
        ":test_main",
        ":testing_clock",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "port_range_parser",
    srcs = ["port_range_parser.cc"],
    hdrs = ["port_range_parser.h"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "port_range_parser_test",
    srcs = ["port_range_parser_test.cc"],
    deps = [
        ":port_range_parser",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "process",
    srcs = ["process_win.cc"],
    hdrs = ["process.h"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":log",
        ":scoped_handle",
        ":status",
        ":status_test_macros",
        ":stopwatch",
        ":util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "process_test",
    srcs = ["process_test.cc"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":process",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "status_macros",
    hdrs = ["status_macros.h"],
    deps = [
        ":status",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "status_test_macros",
    hdrs = ["status_test_macros.h"],
    deps = [
        ":platform",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "stub_process",
    srcs = ["stub_process.cc"],
    hdrs = ["stub_process.h"],
    deps = [
        ":process",
        ":status_macros",
    ],
)

cc_library(
    name = "remote_util",
    srcs = ["remote_util.cc"],
    hdrs = ["remote_util.h"],
    deps = [
        ":platform",
        ":process",
        ":sdk_util",
        ":util",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "remote_util_test",
    srcs = ["remote_util_test.cc"],
    deps = [
        ":remote_util",
        ":test_main",
    ],
)

cc_library(
    name = "semaphore",
    srcs = ["semaphore.cc"],
    hdrs = ["semaphore.h"],
)

cc_test(
    name = "semaphore_test",
    srcs = ["semaphore_test.cc"],
    deps = [
        ":semaphore",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "gamelet_component",
    srcs = ["gamelet_component.cc"],
    hdrs = ["gamelet_component.h"],
    deps = [
        ":path",
        ":platform",
        ":status",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "gamelet_component_test",
    srcs = ["gamelet_component_test.cc"],
    data = ["testdata/root.txt"] + glob(["testdata/gamelet_component/**"]),
    deps = [
        ":gamelet_component",
        ":status_test_macros",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "stats_collector",
    srcs = ["stats_collector.cc"],
    hdrs = ["stats_collector.h"],
    deps = [
        ":log",
        ":stopwatch",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "status",
    srcs = ["status.cc"],
    hdrs = ["status.h"],
    deps = [
        ":platform",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "stopwatch",
    srcs = ["stopwatch.cc"],
    hdrs = ["stopwatch.h"],
    deps = [
        ":clock",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "stopwatch_test",
    srcs = ["stopwatch_test.cc"],
    deps = [
        ":stopwatch",
        ":test_main",
        ":testing_clock",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "sdk_util",
    srcs = ["sdk_util.cc"],
    hdrs = ["sdk_util.h"],
    target_compatible_with = ["@platforms//os:windows"],
    deps = [
        ":path",
        ":platform",
        ":status_macros",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "sdk_util_test",
    srcs = ["sdk_util_test.cc"],
    deps = [
        ":sdk_util",
        ":status_test_macros",
        ":test_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "threadpool",
    srcs = ["threadpool.cc"],
    hdrs = ["threadpool.h"],
    deps = ["@com_google_absl//absl/synchronization"],
)

cc_test(
    name = "threadpool_test",
    srcs = ["threadpool_test.cc"],
    deps = [
        ":semaphore",
        ":test_main",
        ":threadpool",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "testing_clock",
    srcs = ["testing_clock.cc"],
    hdrs = ["testing_clock.h"],
    deps = [
        ":clock",
        ":stopwatch",
    ],
)

cc_library(
    name = "test_main",
    srcs = ["test_main.cc"],
    hdrs = ["test_main.h"],
    deps = [
        ":path",
        "@bazel_tools//tools/cpp/runfiles",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "thread_safe_map",
    hdrs = ["thread_safe_map.h"],
    deps = ["@com_google_absl//absl/synchronization"],
)

cc_test(
    name = "thread_safe_map_test",
    srcs = ["thread_safe_map_test.cc"],
    deps = [
        ":thread_safe_map",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "scoped_handle",
    srcs = ["scoped_handle_win.cc"],
    hdrs = ["scoped_handle_win.h"],
    target_compatible_with = ["@platforms//os:windows"],
)

cc_library(
    name = "util",
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    deps = [
        ":platform",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "util_test",
    srcs = ["util_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "url",
    srcs = ["url.cc"],
    hdrs = ["url.h"],
    deps = [
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "url_test",
    srcs = ["url_test.cc"],
    deps = [
        ":status_test_macros",
        ":url",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
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
