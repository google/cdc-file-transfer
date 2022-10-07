# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This is a simple cc_windows_shared_library rule for builing a DLL on Windows
that other cc rules can depend on.

Example useage:
  cc_windows_shared_library(
      name = "hellolib",
      srcs = [
          "hello-library.cpp",
      ],
      hdrs = ["hello-library.h"],
      # Use this to distinguish compiling vs. linking against the DLL.
      copts = ["/DCOMPILING_DLL"],
  )

Define COMPILING_DLL to export symbols in the header when compiling the DLL as
follows:

  #ifdef COMPILING_DLL
  #define DLLEXPORT __declspec(dllexport)
  #else
  #define DLLEXPORT __declspec(dllimport)
  #endif

  DLLEXPORT void foo();

For more information and sample usage, see:
https://github.com/bazelbuild/bazel/blob/master/examples/windows/dll/
"""

load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_import", "cc_library")

def cc_windows_shared_library(
        name,
        srcs = [],
        deps = [],
        hdrs = [],
        visibility = None,
        **kwargs):
    """A simple cc_windows_shared_library rule for builing a Windows DLL."""
    dll_name = name + ".dll"
    import_lib_name = name + "_import_lib"
    import_target_name = name + "_dll_import"

    # Building a shared library requires a cc_binary with linkshared = 1 set.
    cc_binary(
        name = dll_name,
        srcs = srcs + hdrs,
        deps = deps,
        linkshared = 1,
        **kwargs
    )

    # Get the import library for the dll
    native.filegroup(
        name = import_lib_name,
        srcs = [":" + dll_name],
        output_group = "interface_library",
    )

    # Because we cannot directly depend on cc_binary from other cc rules in deps attribute,
    # we use cc_import as a bridge to depend on the dll.
    cc_import(
        name = import_target_name,
        interface_library = ":" + import_lib_name,
        shared_library = ":" + dll_name,
    )

    # Create a new cc_library to also include the headers needed for the shared library
    cc_library(
        name = name,
        hdrs = hdrs,
        visibility = visibility,
        deps = deps + [
            ":" + import_target_name,
        ],
    )
