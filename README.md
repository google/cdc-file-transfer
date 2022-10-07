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