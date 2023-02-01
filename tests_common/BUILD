load("@rules_cc//cc:defs.bzl", "cc_binary")

package(default_visibility = ["//visibility:public"])

# Using a cc_binary rule here to avoid that this test is included when running
# test from bazel directly. This target is just a convenience target for all the
# unit tests in the scope run from Visual Studio, thus running the following
# command would execute all tests twice:
#
# bazel test --build_tests_only //...
#
# To run this target with Bazel directly, just use "bazel run" instead of
# "bazel test".
cc_binary(
    name = "tests_common",
    testonly = True,
    srcs = [
        "//common:all_test_sources",
    ],
    data = [
        "//common:all_test_data",
    ],
    deps = [
        "//common:ansi_filter",
        "//common:arch_type",
        "//common:buffer",
        "//common:dir_iter",
        "//common:file_watcher",
        "//common:gamelet_component",
        "//common:log",
        "//common:path",
        "//common:path_filter",
        "//common:platform",
        "//common:port_manager",
        "//common:port_range_parser",
        "//common:process",
        "//common:remote_util",
        "//common:sdk_util",
        "//common:semaphore",
        "//common:status_test_macros",
        "//common:stopwatch",
        "//common:stub_process",
        "//common:test_main",
        "//common:testing_clock",
        "//common:thread_safe_map",
        "//common:threadpool",
        "//common:url",
        "//common:util",
    ],
)
