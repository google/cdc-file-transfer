# CDC File Transfer

Born from the ashes of Stadia, this repository contains tools for synching and
streaming files from Windows to Linux. They are based on Content Defined
Chunking (CDC), in particular
[FastCDC](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia),
to split up files into chunks.

## History

At Stadia, game developers had access to Linux cloud instances to run games.
Most developers wrote their games on Windows, though. Therefore, they needed a
way to make them available on the Linux instance.

Initially, we only provided `scp` to copy games from Windows to Linux instances.
As games are usually several dozens of GBs in size, this quickly became
impractical, especially during the pandemic where many developers worked from
home with sub-par internet connections. `scp` always copies full files, there is
no "delta mode" to copy only the things that changed, and there is no fast
compression.

To help this situation, we developed two tools `cdc_rsync` and `cdc_stream`, to
enable developers to quickly iterate on their games without repeatedly incurring
the cost of transmitting dozens of GBs.

## CDC RSync

`cdc_rsync` is a tool to sync files from a Windows machine to a Linux device,
similar to the standard Linux [rsync](https://linux.die.net/man/1/rsync). It is
basically a copy tool, but optimized for the case where there is already an old
version of the files available in the target directory.
* It quickly skips files if timestamp and file size match.
* It uses fast compression for all data transfer.
* If a file changed, it determines which parts changed and only transfers the
  differences.

<p align="center">
  <img src="docs/cdc_rsync_recursive_upload_demo.gif" alt="cdc_rsync demo" width="688" />
</p>

The remote diffing algorithm is based on CDC. In our tests, it is up to 30x
faster than the one used in rsync (1500 MB/s vs 50 MB/s).

The following chart shows a comparison of `cdc_rsync` and Linux rsync running
under Cygwin on Windows. The test data consists of 58 development builds
of some game provided to us for evaluation purposes. The builds are 40-45 GB
large. We uploaded the first build, then synced the second build with each of
the two tools and measured the time. For instance, syncing from build 1 to build
2 took 210 seconds with the Linux rsync, but only 75 seconds with
`cdc_rsync`. The three outliers are probably feature drops from another
development branch, where the delta was much higher. Overall, `cdc_rsync` syncs
files about **3 times faster** than Linux rsync.

<p align="center">
  <img src="docs/cdc_rsync_vs_cygwin_rsync.png" alt="Comparison of cdc_rsync and Linux rsync running in Cygwin" width="753" />
</p>

## CDC Stream

`cdc_stream` is a tool to stream files and directories from a Windows machine to a
Linux device. Conceptually, it is similar to [sshfs](https://github.com/libfuse/sshfs),
but it is optimized for read speed.
* It caches streamed data on the Linux device.
* If a file is re-read on Linux after it changed on Windows, only the
  differences are streamed again. The rest is read from cache.
* Stat operations are very fast since the directory metadata (filenames,
  permissions etc.) is provided in a streaming-friendly way.

To efficiently determine which parts of a file changed, the tool uses the same
CDC-based diffing algorithm as `cdc_rsync`. Changes to Windows files are almost
immediately reflected on Linux, with a delay of roughly (0.5s + 0.7s x total
size of changed files in GB).

<p align="center">
  <img src="docs/cdc_stream_demo.gif" alt="cdc_stream demo" width="688" />
</p>

The tool does not support writing files back from Linux to Windows; the Linux
directory is readonly.

The following chart compares times from starting a game to reaching the menu.
In one case, the game is streamed via `sshfs`, in the other case we use
`cdc_stream`. Overall, we see a **2x to 5x speedup**.

<p align="center">
  <img src="docs/cdc_stream_vs_sshfs.png" alt="Comparison of cdc_stream and sshfs" width="752" />
</p>

# Getting Started

Precompiled binaries can be downloaded from the
[Github homepage](https://github.com/google/cdc-file-transfer/tags).
We currently provide Linux binaries compiled on
[Github's latest Ubuntu](https://github.com/actions/runner-images) version.
If the binaries work for you, you can skip the following two sections.

Alternatively, the project can be built from source. Some binaries have to be
built on Windows, some on Linux.

## Prerequisites

To build the tools from source, the following steps have to be executed on
**both Windows and Linux**.

* Download and install Bazel from [here](https://bazel.build/install).
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

### CDC RSync

* Build Linux components
  ```
  bazel build --config linux --compilation_mode=opt --linkopt=-Wl,--strip-all --copt=-fdata-sections --copt=-ffunction-sections --linkopt=-Wl,--gc-sections //cdc_rsync_server
  ```
* Build Windows components
  ```
  bazel build --config windows --compilation_mode=opt --copt=/GL //cdc_rsync
  ```
* Copy the Linux build output file `cdc_rsync_server` from 
  `bazel-bin/cdc_rsync_server` on the Linux system to `bazel-bin\cdc_rsync`
  on the Windows machine.

### CDC Stream

* Build Linux components
  ```
  bazel build --config linux --compilation_mode=opt --linkopt=-Wl,--strip-all --copt=-fdata-sections --copt=-ffunction-sections --linkopt=-Wl,--gc-sections //cdc_fuse_fs
  ```
* Build Windows components
  ```
  bazel build --config windows --compilation_mode=opt --copt=/GL //asset_stream_manager
  ```
* Copy the Linux build output files `cdc_fuse_fs` and `libfuse.so` from 
  `bazel-bin/cdc_fuse_fs` on the Linux system to `bazel-bin\asset_stream_manager`
  on the Windows machine.

## Usage

The tools require a setup where you can use SSH and SCP from the Windows machine
to the Linux device without entering a password, e.g. by using key-based
authentication.

### Configuring SSH and SCP

By default, the tools search `ssh.exe` and `scp.exe` from the path environment
variable. If you can run the following commands in a Windows cmd without
entering your password, you are all set:
```
ssh user@linux.device.com
scp somefile.txt user@linux.device.com:
```
Here, `user` is the Linux user and `linux.device.com` is the Linux host to
SSH into resp. copy the file to.

If `ssh.exe` or `scp.exe` cannot be found, or if additional arguments are
required, it is recommended to set the environment variables `CDC_SSH_COMMAND`
and `CDC_SCP_COMMAND`. The following example specifies a custom path to the SSH
and SCP binaries, a custom SSH config file, a key file and a known hosts file:
```
set CDC_SSH_COMMAND="C:\path with space\to\ssh.exe" -F C:\path\to\ssh_config -i C:\path\to\id_rsa -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""C:\path\to\known_hosts"""
set CDC_SCP_COMMAND="C:\path with space\to\scp.exe" -F C:\path\to\ssh_config -i C:\path\to\id_rsa -oStrictHostKeyChecking=yes -oUserKnownHostsFile="""C:\path\to\known_hosts"""
```

#### Google Specific
You can use these tools to sync or stream data from a gWindows device to a
gLinux device. To set up SSH authentication, set the following environment
variables:
```
set CDC_SSH_COMMAND=C:\gnubby\bin\ssh.exe
set CDC_SCP_COMMAND=C:\gnubby\bin\scp.exe
```
Note that you will have to touch your security key multiple times during the
first run. Subsequent runs only require a single touch.

### CDC RSync
`cdc_rsync` is used similar to `scp` or the Linux `rsync`. To sync a single
Windows file `C:\path\to\file.txt` to the home directory `~` on the Linux device
`linux.device.com`, run
```
cdc_rsync C:\path\to\file.txt user@linux.device.com:~
```
`cdc_rsync` understands the usual Windows wildcards `*` and `?`.
```
cdc_rsync C:\path\to\*.txt user@linux.device.com:~
```
To sync the contents of the Windows directory `C:\path\to\assets` recursively to
`~/assets` on the Linux device, run
```
cdc_rsync C:\path\to\assets\* user@linux.device.com:~/assets -r
```
To get per file progress, add `-v`:
```
cdc_rsync C:\path\to\assets\* user@linux.device.com:~/assets -vr
```

### CDC Stream

`cdc_stream` consists of a background service called `asset_stream_manager`,
which has to be started in advance with
```
asset_stream_manager
```
The service logs to `%APPDATA%\cdc-file-transfer\logs` by default. Try
`asset_stream_manager --helpfull` to get a list of available flags.

To stream the Windows directory `C:\path\to\assets` to `~/assets` on the Linux
device, run
```
cdc_stream start C:\path\to\assets user@linux.device.com:~/assets
```
This makes all files and directories of `C:\path\to\assets` available on
`~/assets` immediately, as if it were a local copy. However, data is streamed
from Windows to Linux as files are accessed.

To stop the streaming session, enter
```
cdc_stream stop user@linux.device.com:~/assets
```

## Troubleshooting

`cdc_rsync` always logs to the console. By default, the `asset_stream_manager`
service logs to a timestamped file in `%APPDATA%\cdc-file-transfer\logs`. It can
be switched to log to console by starting it with `--log_to_stdout`:
```
asset_stream_manager --log_to_stdout
```

Both `cdc_rsync` and `asset_stream_manager` support command line flags to control log
verbosity. Passing `-vvv` prints debug logs, `-vvvv` prints verbose logs. The
debug logs contain all SSH and SCP commands that are attempted to run, which is
very useful for troubleshooting.

`cdc_stream` is just a thin client for the asset streaming service. Nothing ever
goes wrong with it <sup>[citation needed]</sup>.
