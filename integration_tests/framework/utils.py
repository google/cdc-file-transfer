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

# Lint as: python3
"""Utils for file transfer tests."""

import hashlib
import logging
import os
import pathlib
import random
import shutil
import string
import subprocess
import time
import sys

CDC_RSYNC_PATH = None
CDC_STREAM_PATH = None
USER_HOST = None

SHA1_LEN = 40
SHA1_BUF_SIZE = 65536
RANDOM = random.Random()


def initialize(cdc_rsync_path, cdc_stream_path, user_host):
  """Sets global variables."""
  global CDC_RSYNC_PATH, CDC_STREAM_PATH, USER_HOST

  CDC_RSYNC_PATH = cdc_rsync_path
  CDC_STREAM_PATH = cdc_stream_path
  USER_HOST = user_host


def initialize_random():
  """Sets random seed."""
  global RANDOM
  seed = int(time.time())
  logging.debug('Use random seed %i', seed)
  RANDOM.seed(seed)


def _remove_carriage_return_lines(text):
  r"""Removes *\r, keeps only *\r\n lines.

  Args:
      text (string): Text to remove lines from (usually cdc_rsync output).

  Returns:
      string: Text with lines removed.
  """

  # Some lines have \r\r\n, treat them properly.
  ret = ''
  for line in text.replace('\r\r', '\r').split('\r\n'):
    ret += line.split('\r')[-1] + '\r\n'
  return ret


def target(dir):
  """Prepends user@host: to dir."""
  return USER_HOST + ":" + dir


def run_rsync(*args):
  """Runs cdc_rsync with given args.

  The last positional argument is assumed to be the destination. The user/host
  prefix [user@]host: is optional. If it does not have one, then it is prefixed
  by |USER_HOST|:.

  Args:
      *args (string): cdc_rsync arguments.

  Returns:
      CompletedProcess: cdc_rsync process info with exit code and stdout/stderr.
  """

  # Prefix last positional argument with [user@]host: if it doesn't have such
  # a prefix yet. Note that this won't work in all cases, e.g. if
  # '--exclude', 'file' is passed. Use '--exclude=file' instead.
  args_list = list(filter(None, args))
  for n in range(len(args_list) - 1, 0, -1):
    if args_list[n][0] != '-' and not ':' in args_list[n]:
      args_list[n] = target(args_list[n])
      break

  command = [CDC_RSYNC_PATH, *args_list]

  # Workaround issue with unicode logging.
  logging.debug(
      'Executing %s ',
      ' '.join(command).encode('utf-8').decode('ascii', 'backslashreplace'))
  res = subprocess.run(command, capture_output=True)
  # Remove lines ending with \r since those are temp display lines.
  res.stdout = _remove_carriage_return_lines(res.stdout.decode('ascii'))
  if res.stdout.strip():
    logging.debug('\r\n%s', res.stdout)
  return res


def run_stream(*args):
  """Runs cdc_stream with given args.

  Args:
      *args (string): cdc_stream arguments.

  Returns:
      CompletedProcess: cdc_stream process info with exit code and stdout/stderr.
  """

  command = [CDC_STREAM_PATH, *filter(None, args)]

  # Workaround issue with unicode logging.
  logging.debug(
      'Executing %s ',
      ' '.join(command).encode('utf-8').decode('ascii', 'backslashreplace'))
  return subprocess.run(command)


def files_count_is(cdc_rsync_res,
                   missing=0,
                   missing_dir=0,
                   changed=0,
                   matching=0,
                   matching_dir=0,
                   extraneous=0,
                   extraneous_dir=0):
  r"""Verifies that the output of cdc_rsync indicates the given file counts.

  Args:
      cdc_rsync_res (CompletedProcess): Completed cdc_rsync process
      missing (int, optional): Number of missing files. Defaults to 0.
      missing_dir (int, optional): Number of missing folders. Defaults to 0.
      changed (int, optional): Number of changed files. Defaults to 0.
      matching (int, optional): Number of matching files. Defaults to 0.
      matching_dir (int, optional): Number of matching folders. Defaults to 0.
      extraneous (int, optional): Number of extraneous files. Defaults to 0.
      extraneous_dir (int, optional): Number of extraneous folders. \ Defaults
        to 0.

  Returns:
      bool: True if all file counts match.
  """
  missing_ok = '%i file(s) and %i folder(s) are not present' % (
      missing, missing_dir) in cdc_rsync_res.stdout
  changed_ok = '%i file(s) changed' % (changed) in cdc_rsync_res.stdout
  matching_ok = '%i file(s) and %i folder(s) match' % (
      matching, matching_dir) in cdc_rsync_res.stdout or """%i file(s) and %i \
folder(s) have matching modified time and size""" % (
          matching, matching_dir) in cdc_rsync_res.stdout
  extraneous_ok = """%i file(s) and %i folder(s) on the instance do not exist \
on this machine""" % (extraneous, extraneous_dir) in cdc_rsync_res.stdout
  return missing_ok and changed_ok and matching_ok and extraneous_ok


def sha1sum_local(filepath):
  """Computes the sha1 hash of a local file.

  Args:
      filepath (string): Path of the local (Windows) file

  Returns:
      string: sha1 hash
  """
  sha1 = hashlib.sha1()
  with open(filepath, 'rb') as f:
    while True:
      data = f.read(SHA1_BUF_SIZE)
      if not data:
        break
      sha1.update(data)
  return sha1.hexdigest()


def sha1sum_remote(filepath):
  """Computes the sha1 hash of a remote file.

  Args:
      filepath (string): Path of the remote (Linux) file

  Returns:
      string: sha1 hash
  """
  return get_ssh_command_output('sha1sum %s' % filepath)[0:SHA1_LEN]


def sha1_matches(local_path, remote_path):
  """Compares the sha1 hashes of a local and a remote file.

  Args:
      local_path (string): Path of the local (Windows) file
      remote_path (string): Path of the remote (Linux) file

  Returns:
      bool: True if the sha1 hashes match
  """

  sha1_local = sha1sum_local(local_path)
  sha1_remote = sha1sum_remote(remote_path)
  return sha1_local == sha1_remote


def create_test_file(local_path, size, printable_data=True, append=False):
  """Creates a test file with random text of given size.

  Args:
      local_path (string): Local path of the file to create.
      size (integer): Size of the file to create (bytes).
      printable_data (bool, optional): If the data should be printable. Writing
          a file with printable data is slower, for 1GB of data this takes ~5
          minutes, in comparison to ~2 seconds for non printable data. Defaults
          to True.
      append (bool, optional): If append mode should be used. Defaults to False.
  """
  pathlib.Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)

  mode = None
  random_bytes = None
  if printable_data:
    mode = 'at' if append else 'wt'
    random_bytes = ''.join(
        RANDOM.choices(string.ascii_uppercase + string.digits, k=size))
  else:
    mode = 'ab' if append else 'wb'
    random_bytes = os.urandom(size)

  with open(local_path, mode) as f:
    if size > 0:
      f.write(random_bytes)


def remove_test_file(local_path):
  """Deletes a test file.

  Args:
      local_path (string): Local path of the file to delete.
  """
  os.remove(local_path)


def create_test_directory(local_path):
  """Creates a directory.

  Args:
      local_path (string): Local path of the directory to create.
  """
  pathlib.Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)


def remove_test_directory(local_path):
  """Removes a directory with its content.

  Args:
      local_path (string): Local path of the directory to remove.
  """
  shutil.rmtree(pathlib.Path(os.path.dirname(local_path)), ignore_errors=True)


def does_directory_exist_remotely(path):
  """Checks if a directory exists on the remote instance.

  Args:
      path (string): Path of the remote (Linux) directory

  Returns:
      bool: True if a directory exists.
  """
  return 'yes' in get_ssh_command_output('test -d %s && echo "yes"' % path)


def does_file_exist_remotely(path):
  """Checks if a file exists on the remote instance.

  Args:
      path (string): Path of the remote (Linux) file

  Returns:
      bool: True if a file exists.
  """
  return 'yes' in get_ssh_command_output('test -f %s && echo "yes"' % path)


def change_modified_time(path):
  """Changes the modified time of the given file.

  Args:
      path (string): Path of the local file
  """
  stats = os.stat(path)
  os.utime(path, (stats.st_atime, stats.st_mtime + 1))


def get_ssh_command_output(cmd):
  """Runs an SSH command using the command from the CDC_SSH_COMMAND env var.

  Args:
      cmd (string): Command that is being run remotely

  Returns:
      string: The output of the ssh command.
  """
  ssh_command = os.environ.get('CDC_SSH_COMMAND') or "ssh"
  full_ssh_cmd = '%s -tt "%s" -- %s' % (ssh_command, USER_HOST,
                                        quote_argument(cmd))
  res = subprocess.run(full_ssh_cmd, capture_output=True)
  if res.returncode != 0:
    logging.warning('SSH command %s failed with code %i, stderr: %s', cmd,
                    res.returncode, res.stderr)
  return res.stdout.decode('ascii', errors='replace')


def quote_argument(argument):
  # This isn't fully generic, but does the job... It doesn't handle when the
  # argument already escapes quotes, for instance.
  return '"' + argument.replace('"', '\\"') + '"'


def get_sorted_files(remote_dir, pattern='"*.[t|d]*"'):
  """Returns a sorted list of files in the remote_dir.

  Args:
      remote_dir (string): Remote directory.
      pattern (string, optional): Pattern for matching file names.

  Returns:
      string: Sorted list of files found in the remote directory.
  """
  find_res = get_ssh_command_output('cd %s && find -name %s -print' %
                                    (remote_dir, pattern))

  found = sorted(
      filter(lambda item: item and item != '.', find_res.split('\r\n')))
  return found
