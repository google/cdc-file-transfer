package(default_visibility = [
    "//:__subpackages__",
])

cc_binary(
    name = "cdc_rsync",
    srcs = ["main.cc"],
    deps = [
        ":params",
        "//cdc_rsync",
    ],
)

cc_library(
    name = "params",
    srcs = ["params.cc"],
    hdrs = ["params.h"],
    deps = [
        "//cdc_rsync",
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

filegroup(
    name = "all_test_sources",
    srcs = glob(["*_test.cc"]),
)

filegroup(
    name = "all_test_data",
    srcs = glob(["testdata/**"]),
)
