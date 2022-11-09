package(default_visibility = [
    "//:__subpackages__",
])

cc_binary(
    name = "cdc_stream",
    srcs = ["main.cc"],
    deps = [
        ":local_assets_stream_manager_client",
        "//common:log",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "local_assets_stream_manager_client",
    srcs = ["local_assets_stream_manager_client.cc"],
    hdrs = ["local_assets_stream_manager_client.h"],
    deps = [
        "//proto:local_assets_stream_manager_grpc_proto",
        "@com_google_absl//absl/status",
    ],
)
