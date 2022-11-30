load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "jedec_size_flag",
    srcs = ["jedec_size_flag.cc"],
    hdrs = ["jedec_size_flag.h"],
    deps = [
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:marshalling",
        "@com_google_absl//absl/strings",
    ],
)
