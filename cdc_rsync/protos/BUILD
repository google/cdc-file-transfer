load("@rules_cc//cc:defs.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = [
    "//:__subpackages__",
])

proto_library(
    name = "messages_proto",
    srcs = ["messages.proto"],
    visibility = ["//visibility:private"],
)

cc_proto_library(
    name = "messages_cc_proto",
    deps = [":messages_proto"],
)
