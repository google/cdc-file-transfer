load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "cdc_indexer",
    srcs = ["main.cc"],
    deps = [
        ":indexer_lib",
        "//absl_helper:jedec_size_flag",
        "//common:path",
        "@com_google_absl//absl/flags:config",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "indexer_lib",
    srcs = ["indexer.cc"],
    hdrs = ["indexer.h"],
    deps = [
        "//common:dir_iter",
        "//common:path",
        "//common:status_macros",
        "//fastcdc",
        "@com_github_blake3//:blake3",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)
