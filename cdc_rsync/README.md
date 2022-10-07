# CDC RSync

CDC RSync is a command line tool / library for uploading files to a remote machine in an rsync-like
fashion. It quickly skips files with matching timestamp and size, and only transfers deltas for
existing files.
