workspace(name = "cdc_file_transfer")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_skylib",
    sha256 = "f7be3474d42aae265405a592bb7da8e171919d74c16f082a5457840f06054728",
    urls = ["https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz"],
)

http_archive(
    name = "rules_pkg",
    sha256 = "451e08a4d78988c06fa3f9306ec813b836b1d076d0f055595444ba4ff22b867f",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.1/rules_pkg-0.7.1.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.1/rules_pkg-0.7.1.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

http_archive(
    name = "com_googlesource_code_re2",
    sha256 = "f89c61410a072e5cbcf8c27e3a778da7d6fd2f2b5b1445cd4f4508bee946ab0f",
    strip_prefix = "re2-2022-06-01",
    url = "https://github.com/google/re2/archive/refs/tags/2022-06-01.tar.gz",
)

http_archive(
    name = "com_github_zstd",
    build_file = "@//third_party/zstd:BUILD.bazel",
    sha256 = "f7de13462f7a82c29ab865820149e778cbfe01087b3a55b5332707abf9db4a6e",
    strip_prefix = "zstd-1.5.2",
    url = "https://github.com/facebook/zstd/archive/refs/tags/v1.5.2.tar.gz",
)

http_archive(
    name = "com_github_blake3",
    build_file = "@//third_party/blake3:BUILD.bazel",
    sha256 = "112becf0983b5c83efff07f20b458f2dbcdbd768fd46502e7ddd831b83550109",
    strip_prefix = "BLAKE3-1.3.1",
    url = "https://github.com/BLAKE3-team/BLAKE3/archive/refs/tags/1.3.1.tar.gz",
)

http_archive(
    name = "com_github_fuse",
    build_file = "@//third_party/fuse:BUILD",
    patch_args = ["-p1"],
    patches = ["@//third_party/fuse:disable_symbol_versioning.patch"],
    sha256 = "832432d1ad4f833c20e13b57cf40ce5277a9d33e483205fc63c78111b3358874",
    strip_prefix = "fuse-2.9.7",
    url = "https://github.com/libfuse/libfuse/releases/download/fuse-2.9.7/fuse-2.9.7.tar.gz",
)

http_archive(
    name = "com_github_jsoncpp",
    sha256 = "f409856e5920c18d0c2fb85276e24ee607d2a09b5e7d5f0a371368903c275da2",
    strip_prefix = "jsoncpp-1.9.5",
    url = "https://github.com/open-source-parsers/jsoncpp/archive/refs/tags/1.9.5.tar.gz",
)

# Only required for //cdc_indexer.
http_archive(
    name = "com_github_dirent",
    build_file = "@//third_party/dirent:BUILD.bazel",
    sha256 = "f72d39e3c39610b6901e391b140aa69b51e0eb99216939ed5e547b5dad03afb1",
    strip_prefix = "dirent-1.23.2",
    url = "https://github.com/tronkko/dirent/archive/refs/tags/1.23.2.tar.gz",
)

http_archive(
    name = "com_github_lyra",
    build_file = "@//third_party/lyra:BUILD.bazel",
    sha256 = "a93f247ed89eba11ca36eb24c4f8ba7be636bf24e74aaaa8e1066e0954bec7e3",
    strip_prefix = "Lyra-1.6.1",
    url = "https://github.com/bfgroup/Lyra/archive/refs/tags/1.6.1.tar.gz",
)

local_repository(
    name = "com_google_absl",
    path = "third_party/absl",
)

local_repository(
    name = "com_google_googletest",
    path = "third_party/googletest",
)

local_repository(
    name = "com_google_protobuf",
    path = "third_party/protobuf",
)

local_repository(
    name = "com_github_grpc_grpc",
    path = "third_party/grpc",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()
