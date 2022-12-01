load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//:__subpackages__"])

cc_library(
    name = "metrics",
    srcs = [
        "metrics.cc",
    ],
    hdrs = [
        "metrics.h",
    ],
    deps = [
        ":enums",
        ":messages",
        "//common:log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "enums",
    hdrs = ["enums.h"],
)

cc_library(
    name = "messages",
    srcs = ["messages.cc"],
    hdrs = ["messages.h"],
    deps = [
        ":enums",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "messages_test",
    srcs = ["messages_test.cc"],
    deps = [
        ":messages",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

filegroup(
    name = "all_test_sources",
    srcs = glob(["*_test.cc"]),
)
