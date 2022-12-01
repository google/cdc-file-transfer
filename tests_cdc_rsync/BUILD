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
    name = "tests_cdc_rsync",
    testonly = True,
    srcs = [
        "//cdc_rsync:all_test_sources",
        "//cdc_rsync/base:all_test_sources",
        "//cdc_rsync_server:all_test_sources",
    ],
    data = [
        "//cdc_rsync:all_test_data",
        "//cdc_rsync/base:all_test_data",
        "//cdc_rsync_server:all_test_data",
    ],
    deps = [
        "//cdc_rsync:file_finder_and_sender",
        "//cdc_rsync:parallel_file_opener",
        "//cdc_rsync:params",
        "//cdc_rsync:progress_tracker",
        "//cdc_rsync:zstd_stream",
        "//cdc_rsync/base:cdc_interface",
        "//cdc_rsync/base:fake_socket",
        "//cdc_rsync/base:message_pump",
        "//cdc_rsync_server:file_deleter_and_sender",
        "//cdc_rsync_server:file_diff_generator",
        "//cdc_rsync_server:file_finder",
        "//cdc_rsync_server:unzstd_stream",
        "//common:path_filter",
        "//common:test_main",
        "//common:testing_clock",
    ],
)
