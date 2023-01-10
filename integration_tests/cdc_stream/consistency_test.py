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
"""cdc_stream consistency test."""

import glob
import logging
import os
import queue
import re
import string
import time

from integration_tests.framework import utils
from integration_tests.cdc_stream import test_base


class ConsistencyTest(test_base.CdcStreamTest):
  """cdc_stream test class for CDC FUSE consistency."""

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    logging.debug('ConsistencyTest -> setUpClass')

    config_json = '{\"debug\":1, \"check\":1, \"verbosity\":3}'
    cls._start_service(config_json)

  def _wait_until_remote_dir_matches(self, files, dirs, counter=20):
    """Wait until the directory content has changed.

    Args:
        files (list of strings): List of relative file paths.
        dirs (list of strings): List of relative directory paths.
        counter (int): The number of retries.
    Returns:
        bool: Whether the content of the remote directory matches the local one.
    """
    dirs = [directory.replace('\\', '/').rstrip('/') for directory in dirs]
    files = [file.replace('\\', '/') for file in files]
    sha1_local = self.sha1sum_local_batch(files)
    for _ in range(counter):
      utils.get_ssh_command_output('ls -al %s' % self.remote_base_dir)
      found = utils.get_sorted_files(self.remote_base_dir, '"*"')
      expected = sorted(['./' + f for f in files + dirs])
      if found == expected:
        if not files:
          return True
        sha1_remote = self.sha1sum_remote_batch()
        if sha1_local == sha1_remote:
          return True
      time.sleep(1)
    return False

  def _generate_random_name(self, depth):
    """Generate a random name for a file/directory name.

    Args:
        depth (int): Depth of the directory structure.
    Returns:
        string: Random string.
    """
    max_path_len = 260  # Windows limitation for a path length.
    # 4 symbols are reserved for file extension .txt.
    max_path_len_no_root = max_path_len - len(self.local_base_dir) - 4
    # As a Windows path is limited to 260 symbols it is necesary to consider the
    # depth of the full path.
    # +1 is for the last file, -2: for a path separator + down rounding.
    max_file_name_len = int(max_path_len_no_root / (depth + 1) - 2)
    length = utils.RANDOM.randint(1, max_file_name_len)

    # Consider only upper case and digits, as 1.txt and 1.TXT result in 1 file
    # on Windows.
    name = ''.join(
        utils.RANDOM.choice(string.ascii_uppercase + string.digits)
        for i in range(length))
    return name

  def _generate_dir_list(self, depth, num_leaf_dirs):
    """Generate a list of directories.

    Args:
        depth (int): Depth of the directory structure.
        num_leaf_dirs (int): How many leaf directories should be generated.
    Returns:
        queue of list of strings: Relative paths of directories to be created.
    """
    dirs = queue.Queue(maxsize=0)
    if depth == 0:
      return dirs
    top_num = utils.RANDOM.randint(1, 1 + num_leaf_dirs)
    for _ in range(top_num):
      directory = self._generate_random_name(depth)
      dirs.put([directory])
    new_dirs = queue.Queue(maxsize=0)
    for _ in range(depth - 1):
      while not dirs.empty():
        curr_set = dirs.get()
        missing_dirs = num_leaf_dirs - new_dirs.qsize() - dirs.qsize()
        if missing_dirs > 0:
          num_dir = utils.RANDOM.randint(1, missing_dirs)
          for _ in range(num_dir):
            name = self._generate_random_name(depth)
            path = curr_set.copy()
            path.append(name)
            new_dirs.put(path)
        else:
          new_dirs.put(curr_set)
      new_dirs, dirs = dirs, new_dirs
    for _ in range(num_leaf_dirs - dirs.qsize()):
      dirs.put([self._generate_random_name(depth)])
    return dirs

  def _generate_files(self, dirs, size, depth, min_file_num, max_file_num):
    """Create files in given directories.

    Args:
        dirs (set of strings): Relative paths for directories.
        size (int): Total size of files to be created.
        depth (int): Depth of the directory hierarchy.
        min_file_num (int): Minimal number of files, which can be created in a
        directory.
        max_file_num (int): Maximal number of files, which can be created in a
        directory.
    Returns:
        list of strings: Set of relative paths of created files.
    """
    files = set()
    for directory in dirs:
      number_of_files = utils.RANDOM.randint(min_file_num, max_file_num)
      for _ in range(number_of_files):
        # Add a file extension not to compare if a similar directory exists.
        file_name = self._generate_random_name(depth=depth) + '.txt'
        if file_name not in files:
          file_path = os.path.join(directory, file_name)
          files.add(file_path)
          # Do not create files larger than 1 GB.
          file_size = utils.RANDOM.randint(0, min(1024 * 1024 * 1024, size))
          size -= file_size
          utils.create_test_file(
              os.path.join(self.local_base_dir, file_path), file_size)
        if size <= 0:
          return files
    # Create files for the remaining size.
    if size > 0:
      number_of_files = utils.RANDOM.randint(min_file_num, max_file_num)
      for _ in range(number_of_files):
        file_name = self._generate_random_name(depth)
        files.add(file_name)
        utils.create_test_file(
            os.path.join(self.local_base_dir, file_name),
            int(size / number_of_files))
    return files

  def _generate_dir_paths(self, dirs):
    """Create directories.

    Args:
        dirs (queue of lists of strings): Relative paths for directories.
    Returns:
        set of strings: Relative paths for created directories.
    """
    paths = set()
    for dir_set in dirs.queue:
      curr_path = ''
      for name in dir_set:
        # It is necessary to add the last separator.
        # Otherwise, the leaf directory will not be created.
        curr_path = os.path.join(curr_path, name) + '\\'
        paths.add(curr_path)
      utils.create_test_directory(os.path.join(self.local_base_dir, curr_path))
    return paths

  def _generate_streamed_dir(self, size, depth, min_file_num=1, max_file_num=1):
    """Generate a streamed directory.

    Args:
        size (int): Total size of files to create in the directory.
        depth (int): Depth of the directory hierarchy.
        min_file_num (int): Minimal number of files, which can be created in a
        single directory.
        max_file_num (int): Maximal number of files, which can be created in a
        single directory.
    Returns:
        two sets of strings: Relative paths for created files and directories.
    """
    num_leaf_dirs = 0
    if depth > 0:
      num_leaf_dirs = utils.RANDOM.randint(0, 100)
    logging.debug(('CdcStreamConsistencyTest -> _generate_streamed_dir'
                   ' of depth %i and number of leaf directories %i'), depth,
                  num_leaf_dirs)
    dirs = self._generate_dir_paths(
        self._generate_dir_list(depth, num_leaf_dirs))
    files = self._generate_files(
        dirs=dirs,
        size=size,
        depth=depth,
        min_file_num=min_file_num,
        max_file_num=max_file_num)
    logging.debug(
        ('CdcStreamConsistencyTest -> _generate_streamed_dir: generated'
         ' %i files, %i directories, depth %i'), len(files), len(dirs), depth)
    return files, dirs

  def _recreate_data(self, files, dirs):
    """Recreate test data and check that it can be read on the remote instance.

    Args:
        files (list of strings): List of relative file paths.
        dirs (list of strings): List of relative directory paths.
    """
    logging.debug('CdcStreamConsistencyTest -> _recreate_data')
    self._create_test_data(files=files, dirs=dirs)
    self.assertTrue(self._wait_until_remote_dir_matches(files=files, dirs=dirs))
    self._assert_cdc_fuse_mounted()

  def _assert_inode_consistency_line(self, line, updated_proto=0, updated=0):
    """Assert if the numbers of inodes specific states are correct.

    Args:
        line (string): Statement like Initialized=X, updated_proto=X, updated=X,
        invalid=X.
        updated_proto(int): Expected number of inodes whose protos were updated.
        updated(int): Expected number of inodes whose contents were updated.
    """
    self.assertIn(('Initialized=0, updated_proto=%i,'
                   ' updated=%i, invalid=0') % (updated_proto, updated), line)

  def _assert_consistency_line(self, line):
    """Assert if there are no invalid and initialized nodes.

    Args:
        line (string): Statement like Initialized=X, updated_proto=X, updated=X,
        invalid=X.
    """
    self.assertIn(('Initialized=0,'), line)
    self.assertIn(('invalid=0'), line)

  def _assert_inode_consistency(self, update_map, log_file):
    """Assert that the amount of updated inodes is correct.

    Args:
        update_map (dict): Mapping of inodes' types to their amount.
        log_file (string): Absolute path to the log file.
    """
    with open(log_file) as file:
      success_count = 0
      for line in file:
        if 'Initialized=' in line:
          self._assert_inode_consistency_line(
              line,
              updated_proto=update_map[success_count][0],
              updated=update_map[success_count][1])
        if 'FUSE consistency check succeeded' in line:
          success_count += 1
        self.assertNotIn('FUSE consistency check:', line)

  def _assert_consistency(self, log_file):
    """Assert that there is no error consistency messages in the log.

    Args:
        log_file (string): Absolute path to the log file.
    """

    def assert_initialized_line(line):
      self.assertNotIn('FUSE consistency check:', line)
      if 'Initialized=' in line:
        self._assert_consistency_line(line)

    joined_line = ''
    with open(log_file) as file:
      for line in file:
        # Matches log lines with a log level
        # 2022-01-23 05:18:12.401 DEBUG   process_win.cc(546): LogOutput():
        #            cdc_fuse_fs_stdout: DEBUG   cdc_fuse_fs.cc(1165):
        #            CheckFUSEConsistency(): Initialized=
        # Matches log lines without log level
        # 2022-01-23 05:18:12.401 INFO    process_win.cc(536): LogOutput():
        #            cdc_fuse_fs_stdout: 0, updated_proto=437, updated=563,
        #            invalid
        match = re.match(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s+'
            r'[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+\s+'
            r'[A-Z]+\s+'
            r'(?:[._a-zA-Z0-9()]+:\s+){2}'
            r'cdc_fuse_fs_stdout:\s+'
            r'((?:DEBUG|INFO|WARNING|ERROR)\s+)?(.*)', line)
        if match is None:
          continue
        log_level = match.group(1)
        log_msg = match.group(2)
        # A client side log level marks the beginning of a new log line
        if log_level:
          assert_initialized_line(joined_line)
          joined_line = log_msg.rstrip('\r\n')
        else:
          joined_line += log_msg.rstrip('\r\n')
      assert_initialized_line(joined_line)

  def _get_log_file(self):
    """Find the newest log file for asset streaming 3.0.

    Returns:
        string: Absolute file path for the log file.
    """
    log_dir = os.path.join(os.environ['APPDATA'], 'cdc-file-transfer', 'logs')
    log_files = glob.glob(os.path.join(log_dir, 'cdc_stream*.log'))
    latest_file = max(log_files, key=os.path.getctime)
    logging.debug(('CdcStreamConsistencyTest -> _get_log_file:'
                   ' the current log file is %s'), latest_file)
    return latest_file

  def _mount_with_data(self, files, dirs):
    """Mount a directory, check the content.

    Args:
        files (list of strings): List of relative file paths.
        dirs (list of strings): List of relative directory paths.
    """
    self._start()
    self._test_random_dir_content(files=files, dirs=dirs)
    self._assert_cache()
    self._assert_cdc_fuse_mounted()

  def test_consistency_fixed_data(self):
    """Execute consistency check on a small directory.

    Streamed directory layout:
       |-- rootdir
       |   |-- dir1
       |      |-- emptydir2
       |      |-- file1_1.txt
       |      |-- file1_2.txt
       |   |-- dir2
       |      |-- file2_1.txt
       |   |-- emptydir1
       |   |-- file0.txt
    """
    files = [
        'dir1\\file1_1.txt', 'dir1\\file1_2.txt', 'dir2\\file2_1.txt',
        'file0.txt'
    ]
    dirs = ['dir1\\emptydir2\\', 'emptydir1\\', 'dir1\\', 'dir2\\']
    self._create_test_data(files=files, dirs=dirs)
    self._mount_with_data(files, dirs)

    # Recreate test data.
    log_file = self._get_log_file()
    self._recreate_data(files=files, dirs=dirs)

    # In total there should be 2 checks:
    # - For initial manifest when no data was read,
    # - Two additional caused by the directory change.
    self._assert_inode_consistency([[0, 0], [2, 6], [2, 6]], log_file)

  def _test_consistency_random(self, files, dirs):
    """Mount and check consistency, recreate the data and re-check consistency.

    Args:
        files (list of strings): List of relative file paths.
        dirs (list of strings): List of relative directory paths.
    """
    self._mount_with_data(files=files, dirs=dirs)

    # Recreate test data.
    log_file = self._get_log_file()
    self._recreate_data(files=files, dirs=dirs)
    self._assert_consistency(log_file)

  def sha1sum_local_batch(self, files):
    """Calculate sha1sum of files in the streamed directory on the workstation.

    Args:
        files (list of strings): List of relative file paths to check.
    Returns:
        string: Concatenated sha1 hashes with relative posix file names.
    """
    files.sort()
    sha1sum_local = ''
    for file in files:
      full_path = os.path.join(self.local_base_dir, file.replace('/', '\\'))
      sha1sum_local += utils.sha1sum_local(full_path) + file
    return sha1sum_local

  def sha1sum_remote_batch(self):
    """Calculate sha1sum of files in the remote streamed directory.

    Returns:
        string: Concatenated sha1 hashes with relative posix file names.
    """
    sha1sum_remote = utils.get_ssh_command_output(
        'find %s -type f -exec sha1sum \'{}\' + | sort -k 2' %
        self.remote_base_dir)
    # Example:
    # original: d664613df491478095fa201fac435112
    # /tmp/_cdc_stream_test/E8KPXXS1MYKLIQGAI4I6/M0
    # final: d664613df491478095fa201fac435112E8KPXXS1MYKLIQGAI4I6/M0
    sha1sum_remote = sha1sum_remote.replace(self.remote_base_dir, '').replace(
        ' ', '').replace('\r', '').replace('\n', '').replace('\t', '')
    return sha1sum_remote

  def _test_random_dir_content(self, files, dirs):
    """Check the streamed randomly generated remote directory's content.

    Args:
        files (list of strings): List of relative file paths to check.
        dirs (list of strings): List of relative dir paths to check.
    """
    dirs = [directory.replace('\\', '/').rstrip('/') for directory in dirs]
    files = [file.replace('\\', '/') for file in files]

    utils.get_ssh_command_output('ls -al %s' % self.remote_base_dir)
    self._assert_remote_dir_matches(files + dirs)
    if not files:
      return

    sha1_local = self.sha1sum_local_batch(files)
    sha1_remote = self.sha1sum_remote_batch()
    self.assertEqual(sha1_local, sha1_remote)

  def test_consistency_random_100MB_10files_per_dir(self):
    """Consistency check: modification, 100MB, 10 files/directory."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024,
        depth=utils.RANDOM.randint(0, 10),
        max_file_num=10)
    self._test_consistency_random(files=files, dirs=dirs)

  def test_consistency_random_100MB_exact_1000files_no_dir(self):
    """Consistency check: modification, 100MB, 1000 files/root."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024, depth=0, min_file_num=1000, max_file_num=1000)
    self._test_consistency_random(files=files, dirs=dirs)

  def test_consistency_random_100MB_1000files_per_dir_one_level(self):
    """Consistency check: modification, 100MB, max. 1000 files/dir, depth 1."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024, depth=1, max_file_num=1000)
    self._test_consistency_random(files=files, dirs=dirs)

  def _test_consistency_random_delete(self, files, dirs):
    """Remove and recreate a streamed directory.

    Args:
        files (list of strings): List of relative file paths.
        dirs (list of strings): List of relative directory paths.
    """
    self._mount_with_data(files, dirs)

    # Remove directory on workstation => empty remote directory.
    utils.get_ssh_command_output(self.ls_cmd)
    utils.remove_test_directory(self.local_base_dir)

    self.assertTrue(self._wait_until_remote_dir_matches(files=[], dirs=[]))
    self._assert_cdc_fuse_mounted()

    log_file = self._get_log_file()
    self._recreate_data(files=files, dirs=dirs)
    self._assert_consistency(log_file)

  def test_consistency_random_delete_100MB_10files_per_dir(self):
    """Consistency check: removal, 100MB, 10 files/directory."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024,
        depth=utils.RANDOM.randint(0, 10),
        max_file_num=10)
    self._test_consistency_random_delete(files=files, dirs=dirs)

  def test_consistency_random_delete_100MB_exact_1000files_no_dir(self):
    """Consistency check: removal, 100MB, 1000 files/root."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024, depth=0, min_file_num=1000, max_file_num=1000)
    self._test_consistency_random_delete(files=files, dirs=dirs)

  def test_consistency_random_delete_100MB_1000files_per_dir_one_level(self):
    """Consistency check: removal, 100MB, max. 1000 files/directory, depth 1."""
    files, dirs = self._generate_streamed_dir(
        size=100 * 1024 * 1024, depth=1, max_file_num=1000)
    self._test_consistency_random_delete(files=files, dirs=dirs)


if __name__ == '__main__':
  test_base.test_base.main()
