# CDC File Transfer

This repository contains tools for synching and streaming files. They are based
on Content Defined Chunking (CDC), in particular
[FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia),
to split up files into chunks.

## CDC RSync
Tool to sync files to a remote machine, similar to the standard Linux
[rsync](https://linux.die.net/man/1/rsync). It supports fast compression and
uses a higher performing remote diffing approach based on CDC.

## Asset Streaming
Tool to stream assets from a Windows machine to a Linux device.

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

* Build Linux components
  ```
  bazel build --config linux --compilation_mode=opt //cdc_rsync_server //cdc_fuse_fs
  ```
* Build everything with Bazel
  ```
  bazel build --config windows --compilation_mode=opt //cdc_rsync //asset_stream_manager
  ```
* Copy the Linux build output files `cdc_fuse_fs` and `libfuse.so` from 
  `bazel-bin/cdc_fuse_fs` on the Linux system to `bazel-bin\asset_stream_manager`
  on the Windows machine.
* Copy the Linux build output file `cdc_rsync_server` from 
  `bazel-bin/cdc_rsync_server` on the Linux system to `bazel-bin\cdc_rsync`
  on the Windows machine.

## Usage
To copy the contents of the Windows directory `C:\path\to\assets` to
`~/assets` on the Linux device `linux.machine.com`, try running
```
cdc_rsync --ssh-command=C:\path\to\ssh.exe --scp-command=C:\path\to\scp.exe C:\path\to\assets\* user@linux.machine.com:~/assets -vr
```
Depending on your setup, you may have to specify additional arguments for the
ssh and scp commands, including proper quoting, e.g.
```
cdc_rsync --ssh-command="\"C:\path with space\to\ssh.exe\" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile=\"\"\"known_hosts_file\"\"\"" --scp-command="\"C:\path with space\to\scp.exe\" -F ssh_config_file -i id_rsa_file -oStrictHostKeyChecking=yes -oUserKnownHostsFile=\"\"\"known_hosts_file\"\"\"" C:\path\to\assets\* user@linux.machine.com:~/assets -vr
```
Lengthy ssh/scp commands that rarely change can also be put into environment
variables `CDC_SSH_COMMAND` and `CDC_SCP_COMMAND`.
