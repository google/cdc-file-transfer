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
    name = "tests_cdc_stream",
    testonly = True,
    srcs = [
        "//cdc_fuse_fs:all_test_sources",
        "//cdc_stream:all_test_sources",
        "//data_store:all_test_sources",
        "//manifest:all_test_sources",
        "//metrics:all_test_sources",
    ],
    data = [
        "//cdc_stream:all_test_data",
        "//manifest:all_test_data",
    ],
    deps = [
        "//cdc_fuse_fs:asset",
        "//cdc_fuse_fs:cdc_fuse_fs_lib_mocked",
        "//cdc_fuse_fs:mock_config_stream_client",
        "//cdc_stream:metrics_recorder",
        "//cdc_stream:multi_session",
        "//common:test_main",
        "//common:testing_clock",
        "//data_store:data_provider",
        "//data_store:disk_data_store",
        "//data_store:mem_data_store",
        "//manifest:content_id",
        "//manifest:fake_manifest_builder",
        "//manifest:manifest_builder",
        "//manifest:manifest_iterator",
        "//manifest:manifest_printer",
        "//manifest:manifest_proto_defs",
        "//manifest:manifest_test_base",
        "//manifest:manifest_updater",
        "//metrics",
        "//metrics:enums",
        "//metrics:messages",
    ],
)
