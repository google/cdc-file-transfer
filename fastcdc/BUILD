load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//visibility:public"])

# ------------------------------------------------------------------------------
# Public libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "fastcdc",
    hdrs = ["fastcdc.h"],
)

# ------------------------------------------------------------------------------
# Unit tests
# ------------------------------------------------------------------------------

cc_test(
    name = "fastcdc_test",
    srcs = ["fastcdc_test.cc"],
    deps = [
        ":fastcdc",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
