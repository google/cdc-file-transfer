# Yeti's custom BUILD file
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

package(default_visibility = ["//visibility:public"])

licenses(["restricted"])  # GPL (binary), LGPL (library)

exports_files(["COPYING"])

# ------------------------------------------------------------------------------
# Public libraries
# ------------------------------------------------------------------------------

DEFINES = [
    "_FILE_OFFSET_BITS=64",
    "FUSE_USE_VERSION=26",
]

COPTS = [
    "-DHAVE_CONFIG_H=1",
    "-DFUSERMOUNT_DIR=\\\"$(BINDIR)\\\"",
    "-D_REENTRANT",
    "-pthread",
    "-fno-strict-aliasing",
    "-Wno-use-after-free",  # Looks like a false positive.
    "-iquote",
    "third_party/fuse",
    "-fvisibility=default",  # override -fvisibility=hidden from Yeti toolchain
]

cc_library(
    name = "fuse_shared",
    srcs = [":libfuse.so"],
    copts = COPTS,
    deps = [":fuse_headers"],
)

cc_binary(
    name = "libfuse.so",
    linkopts = ["-Wl,-soname,libfuse.so"],
    linkshared = 1,
    linkstatic = 0,
    deps = ["fuse_internal"],
)

cc_library(
    name = "fuse_internal",
    srcs = [
        "lib/buffer.c",
        "lib/cuse_lowlevel.c",
        "lib/fuse.c",
        "lib/fuse_i.h",
        "lib/fuse_kern_chan.c",
        "lib/fuse_loop.c",
        "lib/fuse_loop_mt.c",
        "lib/fuse_lowlevel.c",
        "lib/fuse_misc.h",
        "lib/fuse_mt.c",
        "lib/fuse_opt.c",
        "lib/fuse_session.c",
        "lib/fuse_signals.c",
        "lib/helper.c",
        "lib/modules/iconv.c",
        "lib/modules/subdir.c",
        "lib/mount.c",
        "lib/mount_util.c",
        "lib/mount_util.h",
    ],
    copts = COPTS,
    linkopts = [
        "-lpthread",
        "-ldl",
    ],
    linkstatic = 1,  # Required to make symbols show up in libfuse.so above.
    visibility = ["//visibility:private"],
    deps = [":fuse_headers"],
    alwayslink = 1,
)

cc_library(
    name = "fuse_headers",
    hdrs = glob(["include/*.h"]) + ["include/fuse/fuse.h"],
    copts = COPTS,
    defines = DEFINES,
    includes = ["include"],
    visibility = ["//visibility:private"],
    deps = [":fuse_config"],
)

cc_library(
    name = "ulockmgr",
    srcs = [
        "lib/ulockmgr.c",
    ],
    hdrs = [
        "include/ulockmgr.h",
    ],
    copts = COPTS,
    defines = DEFINES,
    includes = ["include"],
    linkopts = [
        "-lpthread",
        "-ldl",
    ],
)

# ------------------------------------------------------------------------------
# Genrules
# ------------------------------------------------------------------------------

# A crude hack to make #include <fuse/fuse.h> work.
genrule(
    name = "fuse_fuse_h",
    outs = ["include/fuse/fuse.h"],
    cmd = "echo '#include <fuse.h>' > $@",
    visibility = ["//visibility:private"],
)

genrule(
    name = "config_h",
    srcs = ["@//third_party/fuse:linux_config"],
    outs = ["config.h"],
    cmd = "cp $< $@",
    visibility = ["//visibility:private"],
)

cc_library(
    name = "fuse_config",
    srcs = ["config.h"],
    includes = ["."],
)

filegroup(
    name = "linux_config",
    srcs = ["config.h.linux"],
)
