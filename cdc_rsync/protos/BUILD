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
