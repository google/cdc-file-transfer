# CDC File Transfer

This repository contains tools for synching and streaming files. They are based
on Content Defined Chunking (CDC), in particular
[FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia),
to split up files into chunks.

## CDC RSync

CDC RSync is a ool to sync files to a remote machine, similar to the standard
Linux [rsync](https://linux.die.net/man/1/rsync). It is basically a copy tool,
but optimized for the case where there is already an old version of the files
available in the target directory.
* It skips files quickly if timestamp and file size match.
* It uses fast compression for all data transfer.
* If a file changed, it determines which parts changed and only transfers the
  differences.

The remote diffing algorithm is based on CDC. In our tests, it is up to 30x
faster than the one used in rsync (1500 MB/s vs 50 MB/s).

## CDC Stream

CDC Stream is a tool to stream files and directories from a Windows machine to a
Linux device. Conceptually, it is similar to [sshfs](https://github.com/libfuse/sshfs),
but it is optimized for read speed.
* It caches streamed data on the Linux device.
* If a file is re-read on Linux after it changed on Windows, only the
  differences are streamed again. The rest is read from cache.
* Stat operations are very fast since the directory metadata (filenames,
  permissions etc.) is provided in a streaming-friendly way.

To efficiently determine which parts of a file changed, the tool uses the same
CDC-based diffing algorithm as CDC RSync. Changes to Windows files are almost
immediately reflected on Linux, with a delay of roughly (0.5s + 0.7s x total
size of changed files in GB).

The tool does not support writing files back from Linux to Windows; the Linux
directory is readonly.

# Getting Started

The project has to be built both on Windows and Linux.

## Prerequisites

The following steps have to be executed on **both Windows and Linux**.

* Download and install Bazel from https://bazel.build/install.
* Clone the repository.
  ```
  git clone https://github.com/google/cdc-file-transfer
  ```
* Initialize submodules.
  ```
  cd cdc-file-transfer
  git submodule update --init --recursive
  ```

Finally, install an SSH client on the Windows device if not present.
The file transfer tools require `ssh.exe` and `scp.exe`.

## Building

The two tools can be built and used independently.

### CDC Sync

* Build Linux components
  ```
  bazel build --config linux --compilation_mode=opt //cdc_rsync_server
  ```
* Build Windows components
  ```
  bazel build --config windows --compilation_mode=opt //cdc_rsync
  ```
* Copy the Linux build output file `cdc_rsync_server` from 
  `bazel-bin/cdc_rsync_server` on the Linux system to `bazel-bin\cdc_rsync`
  on the Windows machine.

### CDC Stream

* Build Linux components
  ```
  bazel build --config linux --compilation_mode=opt //cdc_fuse_fs
  ```
* Build Windows components
  ```
  bazel build --config windows --compilation_mode=opt //asset_stream_manager
  ```
* Copy the Linux build output files `cdc_fuse_fs` and `libfuse.so` from 
  `bazel-bin/cdc_fuse_fs` on the Linux system to `bazel-bin\asset_stream_manager`
  on the Windows machine.

## Usage

### CDC Sync
To copy the contents of the Windows directory `C:\path\to\assets` to `~/assets`
on the Linux device `linux.machine.com`, run
```
cdc_rsync --ssh-command=C:\path\to\ssh.exe --scp-command=C:\path\to\scp.exe C:\path\to\assets\* user@linux.machine.com:~/assets -vr
```
Depending on your setup, you may have to specify additional arguments for the
ssh and scp commands, including proper quoting, e.g.
```
cdc_rsync --ssh-command="\"C:\path with space\to\ssh.exe\" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile=\"\"\"known_hosts_file\"\"\"" --scp-command="\"C:\path with space\to\scp.exe\" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile=\"\"\"known_hosts_file\"\"\"" C:\path\to\assets\* user@linux.machine.com:~/assets -vr
```
Lengthy ssh/scp commands that rarely change can also be put into environment
variables `CDC_SSH_COMMAND` and `CDC_SCP_COMMAND`, e.g.
```
set CDC_SSH_COMMAND="C:\path with space\to\ssh.exe" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""known_hosts_file"""

set CDC_SCP_COMMAND="C:\path with space\to\scp.exe" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""known_hosts_file"""

cdc_rsync C:\path\to\assets\* user@linux.machine.com:~/assets -vr
```

### CDC Stream
