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
"""cdc_rsync upload test."""

import json
import logging
import os
import subprocess
import time

from integration_tests.framework import utils
from integration_tests.cdc_rsync import test_base


class UploadTest(test_base.CdcRsyncTest):
  """cdc_rsync upload test class."""

  def test_single_uncompressed(self):
    """Uploads and syncs a file uncompressed."""

    self._do_test_single(compressed=False)

  def test_upload_compressed(self):
    """Uploads and syncs a file compressed."""

    self._do_test_single(compressed=True)

  def _do_test_single(self, compressed):
    """Runs rsync 3 times and validates results.

       1) Uploads a file, checks sha1 hashes.
       2) Uploads the same file again, checks nothing changed.
       3) Modifies the file and uploads again. Checks sha1 hashes.

    Args:
        compressed (bool): Whether to append '--compress' or not.
    """
    compressed_arg = '--compress' if compressed else None

    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, matching=1))

    utils.create_test_file(self.local_data_path, 2534)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))

  def test_backslash_in_source_folder(self):
    r"""Verifies uploading from /source/folder."""

    filepath = os.path.join(self.local_base_dir, 'file1.txt')
    utils.create_test_file(filepath, 1)
    filepath = filepath.replace('\\', '/')
    res = utils.run_rsync(filepath, self.remote_base_dir)
    self.assertTrue(utils.files_count_is(res, missing=1))
    self._assert_remote_dir_contains(['file1.txt'])

  def test_single_unicode(self):
    """Uploads a file with a non-ascii unicode path and checks sha1 signatures."""

    nonascii_local_data_path = self.local_base_dir + '⛽⛽⛽⛽⛽⛽⛽⛽.dat'
    nonascii_remote_data_path = self.remote_base_dir + '⛽⛽⛽⛽⛽⛽⛽⛽.dat'
    utils.create_test_file(nonascii_local_data_path, 1024)
    # In order to check that non-ascii characters are not considered as
    # wildcard
    # ?  characters, create a second file.  Only 1 file should be uploaded.
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(nonascii_local_data_path, self.remote_base_dir, None)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1))
    self.assertTrue(
        utils.sha1_matches(nonascii_local_data_path, nonascii_remote_data_path))

  def test_uncompressed_no_empty_folders(self):
    """Uploads and syncs multiple files uncompressed in different folders."""

    self._do_test_no_empty_folders(compressed=False)

  def test_compressed_no_empty_folders(self):
    """Uploads and syncs multiple files compressed in different folders."""

    self._do_test_no_empty_folders(compressed=True)

  def _do_test_no_empty_folders(self, compressed):
    """Runs rsync with(out) -r for a non-trivial directory and validates results.

       1) Uploads a source directory with -r, checks sha1 hashes.
       |-- rootdir
       |   |-- dir1
       |      |-- file1_1.txt
       |      |-- file1_2.txt
       |   |-- dir2
       |      |-- file2_1.txt
       |   |-- file0.txt
       2) Uploads the same source directory again without -r,
       checks nothing has changed. The directory should be just skipped.
       3) Uploads the same source directory with --delete option and with -r.
       Nothing should change.
       4) Removes dir1 and dir2 locally.
       Uploads the same source directory with --delete option and with -r.
       dir1 and dir2 should be removed from the remote instance.

    Args:
        compressed (bool): Whether to append '--compress' or not.
    """
    compressed_arg = '--compress' if compressed else None
    local_root_path = self.local_base_dir + 'rootdir'
    remote_root_path = self.remote_base_dir + 'rootdir/'
    utils.create_test_file(local_root_path + '\\dir1\\file1_1.txt', 1024)
    utils.create_test_file(local_root_path + '\\dir1\\file1_2.txt', 1024)
    utils.create_test_file(local_root_path + '\\dir2\\file2_1.txt', 1024)
    utils.create_test_file(local_root_path + '\\file0.txt', 1024)
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=4, missing_dir=3))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir1\\file1_1.txt',
                           remote_root_path + 'dir1/file1_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir1\\file1_2.txt',
                           remote_root_path + 'dir1/file1_2.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir2\\file2_1.txt',
                           remote_root_path + 'dir2/file2_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\file0.txt',
                           remote_root_path + 'file0.txt'))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, extraneous_dir=1))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, matching=4, matching_dir=3))

    utils.remove_test_directory(local_root_path + '\\dir1\\')
    utils.remove_test_directory(local_root_path + '\\dir2\\')
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(
            res, matching=1, extraneous=3, matching_dir=1, extraneous_dir=2))
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'dir1'))
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'dir2'))

  def _do_test_no_empty_folders_with_backslash(self, compressed):
    """Runs rsync with(out) -r for a non-trivial directory with a trailing backslash.

       1) Uploads a source directory with -r, checks sha1 hashes.
       Everything from rootdir should be copied except rootdir itself.
       |-- rootdir
       |   |-- dir1
       |      |-- file1_1.txt
       |      |-- file1_2.txt
       |   |-- dir2
       |      |-- file2_1.txt
       |   |-- file0.txt
       2) Uploads the same source directory again without -r,
       checks nothing has changed. The directory should be just skipped.
       3) Uploads the same source directory with --delete option and with -r.
       Nothing should change.
       4) Removes dir1 and dir2 locally.
       Uploads the same source directory with --delete option and with -r.
       dir1 and dir2 should be removed from the remote instance.

    Args:
        compressed (bool): Whether to append '--compress' or not.
    """
    compressed_arg = '--compress' if compressed else None
    local_root_path = self.local_base_dir + 'rootdir\\'
    utils.create_test_file(local_root_path + 'dir1\\file1_1.txt', 1024)
    utils.create_test_file(local_root_path + 'dir1\\file1_2.txt', 1024)
    utils.create_test_file(local_root_path + 'dir2\\file2_1.txt', 1024)
    utils.create_test_file(local_root_path + 'file0.txt', 1024)
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=4, missing_dir=2))
    self.assertTrue(
        utils.sha1_matches(local_root_path + 'dir1\\file1_1.txt',
                           self.remote_base_dir + 'dir1/file1_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + 'dir1\\file1_2.txt',
                           self.remote_base_dir + 'dir1/file1_2.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + 'dir2\\file2_1.txt',
                           self.remote_base_dir + 'dir2/file2_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + 'file0.txt',
                           self.remote_base_dir + 'file0.txt'))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(
        res, extraneous=1, extraneous_dir=2))  # file0.txt, dir1, dir2

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, matching=4, matching_dir=2))

    utils.remove_test_directory(local_root_path + '\\dir1\\')
    utils.remove_test_directory(local_root_path + '\\dir2\\')
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(res, matching=1, extraneous=3, extraneous_dir=2))
    self.assertFalse(
        utils.does_directory_exist_remotely(self.remote_base_dir + 'dir1'))
    self.assertFalse(
        utils.does_directory_exist_remotely(self.remote_base_dir + 'dir2'))

  def test_uncompressed_no_empty_folders_with_backslash(self):
    """Uploads multiple files uncompressed from a folder with a trailing backslash."""

    self._do_test_no_empty_folders_with_backslash(compressed=False)

  def test_compressed_no_empty_folders_with_backslash(self):
    """Uploads multiple files compressed from a folder with a trailing backslash."""

    self._do_test_no_empty_folders_with_backslash(compressed=True)

  def test_uncompressed_with_empty_folders(self):
    """Uploads and syncs multiple files uncompressed and empty folders."""

    self._do_test_with_empty_folders(compressed=False)

  def test_compressed_with_empty_folders(self):
    """Uploads and syncs multiple files compress and empty folders."""

    self._do_test_with_empty_folders(compressed=True)

  def _do_test_with_empty_folders(self, compressed):
    """Runs rsync with(out) -r for a non-trivial directory with empty folders.

       1) Uploads a source directory with -r, checks sha1 hashes.
       |-- rootdir
       |   |-- dir1
       |      |-- emptydir2
       |      |-- file1_1.txt
       |      |-- file1_2.txt
       |   |-- dir2
       |      |-- file2_1.txt
       |   |-- emptydir1
       |   |-- file0.txt
       2) Uploads the same source directory again without -r,
       checks nothing has changed. The directory should be just skipped.
       3) Uploads the same source directory with --delete option and with -r.
       Nothing should change.
       4) Removes dir1 and dir2 locally.
       Uploads the same source directory with --delete option and with -r.
       dir1 and dir2 should be removed from the remote instance.

    Args:
        compressed (bool): Whether to append '--compress' or not.
    """
    compressed_arg = '--compress' if compressed else None
    local_root_path = self.local_base_dir + 'rootdir'
    remote_root_path = self.remote_base_dir + 'rootdir/'
    utils.create_test_file(local_root_path + '\\dir1\\file1_1.txt', 1024)
    utils.create_test_file(local_root_path + '\\dir1\\file1_2.txt', 1024)
    utils.create_test_directory(local_root_path + '\\dir1\\emptydir2\\')
    utils.create_test_file(local_root_path + '\\dir2\\file2_1.txt', 1024)
    utils.create_test_file(local_root_path + '\\file0.txt', 1024)
    utils.create_test_directory(local_root_path + '\\emptydir1\\')
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=4, missing_dir=5))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir1\\file1_1.txt',
                           remote_root_path + 'dir1/file1_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir1\\file1_2.txt',
                           remote_root_path + 'dir1/file1_2.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\dir2\\file2_1.txt',
                           remote_root_path + 'dir2/file2_1.txt'))
    self.assertTrue(
        utils.sha1_matches(local_root_path + '\\file0.txt',
                           remote_root_path + 'file0.txt'))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, extraneous_dir=1))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, matching=4, matching_dir=5))

    utils.remove_test_directory(local_root_path + '\\dir1\\')
    utils.remove_test_directory(local_root_path + '\\dir2\\')
    res = utils.run_rsync(local_root_path, self.remote_base_dir, compressed_arg,
                          '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(
            res, matching=1, extraneous=3, matching_dir=2, extraneous_dir=3))
    self.assertIn('3/3 file(s) and 3/3 folder(s) deleted', res.stdout)
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'dir1'))
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'dir2'))

  def test_upload_empty_file(self):
    """Uploads an empty file and checks sha1 signatures."""

    empty_local_data_path = self.local_base_dir + 'emptyfile.dat'
    empty_remote_data_path = self.remote_base_dir + 'emptyfile.dat'
    utils.create_test_file(empty_local_data_path, 0)
    res = utils.run_rsync(empty_local_data_path, self.remote_base_dir, None)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1))
    self.assertTrue(
        utils.sha1_matches(empty_local_data_path, empty_remote_data_path))

  def test_upload_empty_folder_with_backslash(self):
    """Uploads an empty folder with a trailing backslash."""

    self._do_test_upload_empty_folder(with_backslash=True)

  def test_upload_empty_folder_no_backslash(self):
    """Uploads an empty folder without a trailing backslash."""

    self._do_test_upload_empty_folder(with_backslash=False)

  def _do_test_upload_empty_folder(self, with_backslash=False):
    """Uploads an empty folder."""

    local_data_dir = (
        self.local_base_dir +
        'empty_folder\\' if with_backslash else self.local_base_dir +
        'empty_folder')
    res = utils.run_rsync(local_data_dir, self.remote_base_dir, None)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=0))

  def test_whole_file_uncompressed(self):
    """Uploads and syncs a file uncompressed with --whole-file."""

    self._do_test_whole_file(compressed=False)

  def test_whole_file_compressed(self):
    """Uploads and syncs a file compressed with --whole-file."""

    self._do_test_whole_file(compressed=True)

  def _do_test_whole_file(self, compressed):
    """Runs rsync 3 times with --whole-file -v options and validates results.

       1) Uploads a file.
       2) Modifies the file and uploads it with --whole-file and -v options.
       Checks the output contains C100%, not D100%.
       3) Modifies the file and uploads it with -W and -v options.
       Checks the output contains C100%, not D100%.

    Args:
        compressed (bool): Whether to append '--compress' or not.
    """
    compressed_arg = '--compress' if compressed else None

    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg)

    utils.create_test_file(self.local_data_path, 2534)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg, '--whole-file', '-v')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    self.assertIn('will be copied due to -W/--whole-file', str(res.stdout))
    self.assertIn('C100%', str(res.stdout))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))

    utils.create_test_file(self.local_data_path, 3456)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          compressed_arg, '-W', '-v')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    self.assertIn('C100%', str(res.stdout))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))

  def test_keep_file_permissions(self):
    """Verifies that file permissions are kept for changed files."""

    # Upload a file and check permissions.
    utils.create_test_file(self.local_data_path, 1024)
    utils.run_rsync(self.local_data_path, self.remote_base_dir)
    ls_res = utils.get_ssh_command_output('ls -al %s' % self.remote_data_path)
    self.assertIn('-rw-r--r--', ls_res)

    # Add executable bit.
    utils.get_ssh_command_output('chmod a+x %s*' % self.remote_data_path)
    ls_res = utils.get_ssh_command_output('ls -al %s' % self.remote_data_path)
    self.assertIn('-rwxr-xr-x', ls_res)

    # Sync file again and verify permissions don't change.
    utils.create_test_file(self.local_data_path, 1337)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    ls_res = utils.get_ssh_command_output('ls -al %s' % self.remote_data_path)
    self.assertIn('-rwxr-xr-x', ls_res)

  def test_include_exclude(self):
    """Verifies the --include and --exclude options."""

    files = [
        'file1.txt', 'folder1\\file2.txt', 'folder1\\file3.dat',
        'folder1\\folder2\\file4.txt', 'folder3\\file5.txt'
    ]

    for file in files:
      utils.create_test_file(self.local_base_dir + file, 987)

    # Upload file2.txt and file3.dat.
    res = utils.run_rsync(self.local_base_dir + '*', self.remote_base_dir, '-r',
                          '--include=*\\file2.txt', '--exclude=*.txt')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=2, missing_dir=3))
    self._assert_remote_dir_contains(['folder1/file2.txt', 'folder1/file3.dat'])

    # Upload all except *.dat with --delete, make sure file3.dat is kept.
    utils.remove_test_file(self.local_base_dir + 'folder1\\file3.dat')
    res = utils.run_rsync(self.local_base_dir + '*', self.remote_base_dir, '-r',
                          '--delete', '--exclude=*.dat')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(res, missing=3, matching=1, matching_dir=3))
    self._assert_remote_dir_contains([
        'file1.txt', 'folder1/file2.txt', 'folder1/file3.dat',
        'folder1/folder2/file4.txt', 'folder3/file5.txt'
    ])

  def test_exclude_include_from(self):
    """Verifies the --include-from and --exclude-from options."""

    files = [
        'file1.txt', 'folder1\\file2.txt', 'folder1\\file3.dat',
        'folder1\\folder2\\file4.txt', 'folder3\\file5.txt'
    ]

    for file in files:
      utils.create_test_file(self.local_base_dir + file, 987)

    include_file = self.local_base_dir + 'include.txt'
    with open(include_file, 'wt') as f:
      f.writelines(['file1.txt\n', 'folder3\\file5.txt'])

    exclude_file = self.local_base_dir + 'exclude.txt'
    with open(exclude_file, 'wt') as f:
      f.writelines(['*.txt'])

    res = utils.run_rsync('-r', '--include-from', include_file,
                          '--exclude-from', exclude_file,
                          self.local_base_dir + '*', self.remote_base_dir)
    self.assertTrue(utils.files_count_is(res, missing=3, missing_dir=3))
    self._assert_remote_dir_contains(
        ['file1.txt', 'folder1/file3.dat', 'folder3/file5.txt'])

  def test_files_from(self):
    """Verifies the --files-from option."""

    files = [
        'file1.txt', 'folder1\\file2.txt', 'folder1\\file3.dat',
        'folder1\\folder2\\file4.txt', 'folder3\\file5.txt'
    ]

    for file in files:
      utils.create_test_file(self.local_base_dir + file, 987)

    sources_file = self.local_base_dir + 'sources.txt'
    with open(sources_file, 'wt') as f:
      f.writelines([
          'file1.txt\n',
          '\n',
          '  folder1\\file3.dat  \n',
          'folder1\\.\\folder2\\file4.txt\n',  # .\\ = rel path marker
          '  folder3\\file5.txt\n',
          '\n'
      ])

    res = utils.run_rsync('--files-from', sources_file, self.local_base_dir,
                          self.remote_base_dir)
    self.assertTrue(utils.files_count_is(res, missing=4))
    self._assert_remote_dir_contains([
        'file1.txt', 'folder1/file3.dat', 'folder2/file4.txt',
        'folder3/file5.txt'
    ])

    # Upload again to check that nothing changes.
    res = utils.run_rsync('--files-from', sources_file, self.local_base_dir,
                          self.remote_base_dir)
    self.assertTrue(utils.files_count_is(res, matching=4, extraneous_dir=3))

  def test_checksum_file(self):
    """Uploads and syncs a file with --checksum.

       1) Uploads a file.
       2) Uploads a file with --checksum option. As the file was not changed, it
       is recognized as matched. The output should contain D100%.
       3) Uploads the same file with --whole-file --checksum -v.
       Checks the output contains C100%, not D100%.
       4) Modifies the file without changing its content. The file is
       synchronized, the output should contain D100%.

    """

    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          '--checksum', '-v')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, matching=1))
    self.assertIn('D100%', str(res.stdout))
    self.assertIn('will be synced due to -c/--checksum', str(res.stdout))

    utils.create_test_file(self.local_data_path, 2534)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          '--checksum', '-v', '--whole-file')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    self.assertIn('C100%', str(res.stdout))
    self.assertIn('will be copied due to -c/--checksum and -W/--whole-file',
                  str(res.stdout))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))

    utils.change_modified_time(self.local_data_path)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-c',
                          '-v')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, changed=1))
    self.assertIn('D100%', str(res.stdout))

  def test_sync_folder_when_remote_file_non_recursive(self):
    """Non-recursively uploads a folder while there is a remote file with the same name."""

    local_folder = self.local_base_dir + 'foldertocopy\\'
    utils.create_test_directory(local_folder)
    utils.get_ssh_command_output(
        'mkdir -p %s && touch %s' %
        (self.remote_base_dir, self.remote_base_dir + 'foldertocopy'))

    res = utils.run_rsync(self.local_base_dir + 'foldertocopy',
                          self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, extraneous=1))
    self.assertFalse(
        utils.does_directory_exist_remotely(self.remote_base_dir +
                                            'foldertocopy'))
    self.assertTrue(
        utils.does_file_exist_remotely(self.remote_base_dir + 'foldertocopy'))

  def test_sync_folder_when_remote_file_recursive_with_delete(self):
    """Recursively uploads a folder while removing a remote file with the same name with --delete."""

    local_folder = self.local_base_dir + 'foldertocopy\\'
    utils.create_test_directory(local_folder)
    utils.get_ssh_command_output(
        'mkdir -p %s && touch %s' %
        (self.remote_base_dir, self.remote_base_dir + 'foldertocopy'))

    res = utils.run_rsync(self.local_base_dir + 'foldertocopy',
                          self.remote_base_dir, '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, extraneous=1, missing_dir=1))
    self.assertTrue(
        utils.does_directory_exist_remotely(self.remote_base_dir +
                                            'foldertocopy'))
    self.assertFalse(
        utils.does_file_exist_remotely(self.remote_base_dir + 'foldertocopy'))
    self.assertIn('1/1 file(s) and 0/0 folder(s) deleted', str(res.stdout))

  def test_sync_file_when_remote_folder_recursive_with_delete(self):
    """Recursively uploads a file while removing a remote folder with the same name with --delete."""
    utils.create_test_file(self.local_data_path, 1024)
    utils.get_ssh_command_output('mkdir -p %s' % self.remote_data_path)

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          '--delete', '-r')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1, extraneous_dir=1))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))
    self.assertFalse(utils.does_directory_exist_remotely(self.remote_data_path))
    self.assertIn('0/0 file(s) and 1/1 folder(s) deleted', str(res.stdout))

  def test_sync_file_when_remote_folder_empty_non_recursive(self):
    """Non-recursively uploads a file while there is an empty remote folder with the same name."""
    self._do_test_sync_file_when_remote_folder_empty(recursive=False)

  def test_sync_file_when_remote_folder_empty_recursive(self):
    """Recursively uploads a file while there is an empty remote folder with the same name."""
    self._do_test_sync_file_when_remote_folder_empty(recursive=True)

  def _do_test_sync_file_when_remote_folder_empty(self, recursive):
    """Uploads a file while there is an empty remote folder with the same name.

    Args:
        recursive (bool): Whether to append '-r' or not.
    """
    flag = '-r' if recursive else None
    utils.create_test_file(self.local_data_path, 1024)
    utils.get_ssh_command_output('mkdir -p %s' % self.remote_data_path)

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, flag)
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1, extraneous_dir=1))
    self.assertTrue(
        utils.sha1_matches(self.local_data_path, self.remote_data_path))
    self.assertFalse(utils.does_directory_exist_remotely(self.remote_data_path))
    self.assertNotIn('0/0 file(s) and 1/1 folder(s) deleted', str(res.stdout))

  def test_sync_file_when_remote_folder_non_empty_non_recursive(self):
    """Non-recursively uploads a file while there is a non-empty remote folder with the same name."""
    self._do_test_sync_file_when_remote_folder_non_empty(recursive=False)

  def test_sync_file_when_remote_folder_non_empty_recursive(self):
    """Recursively uploads a file while there is a non-empty remote folder with the same name."""
    self._do_test_sync_file_when_remote_folder_non_empty(recursive=True)

  def _do_test_sync_file_when_remote_folder_non_empty(self, recursive):
    """Uploads a file while there is a non-empty remote folder with the same name.

    Args:
        recursive (bool): Whether to append '-r' or not.
    """
    flag = '-r' if recursive else None
    utils.create_test_file(self.local_data_path, 1024)
    utils.get_ssh_command_output('mkdir -p %s' % self.remote_data_path)
    utils.get_ssh_command_output(
        'mkdir -p %s && touch %s' %
        (self.remote_base_dir, self.remote_data_path + '/file1.txt'))

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, flag)
    self.assertIn('remove() failed: Directory not empty.', str(res.stderr))
    if recursive:
      self.assertTrue(
          utils.files_count_is(res, missing=1, extraneous=1, extraneous_dir=1))
    else:
      self.assertTrue(utils.files_count_is(res, missing=1, extraneous_dir=1))
    self.assertTrue(utils.does_directory_exist_remotely(self.remote_data_path))
    self.assertTrue(
        utils.does_file_exist_remotely(self.remote_data_path + '/file1.txt'))
    self.assertFalse(utils.does_file_exist_remotely(self.remote_data_path))

  def test_upload_from_dot(self):
    """Uploads files from the current directory ('.')."""
    utils.create_test_file(self.local_base_dir + 'file1.txt', 1024)
    utils.create_test_file(self.local_base_dir + 'dir\\file2.txt', 1024)

    prev_cwd = os.getcwd()
    os.chdir(self.local_base_dir)
    try:
      # Uploading recursivly should pick up all files and dirs.
      res = utils.run_rsync('.', self.remote_base_dir, '-r')
      self.assertTrue(utils.files_count_is(res, missing=2, missing_dir=1))
      self._assert_remote_dir_contains(['file1.txt', 'dir/file2.txt'])

      # Uploading again should not change anything.
      res = utils.run_rsync('.', self.remote_base_dir, '-r')
      self.assertTrue(utils.files_count_is(res, matching=2, matching_dir=1))

      # Verify that non-recursive uploads do nothing.
      res = utils.run_rsync('.', self.remote_base_dir)
      self.assertTrue(utils.files_count_is(res, extraneous=1, extraneous_dir=1))
    finally:
      os.chdir(prev_cwd)

  def test_upload_from_dotdot(self):
    """Uploads files from the parent directory ('..')."""
    utils.create_test_file(self.local_base_dir + 'file1.txt', 1024)
    utils.create_test_file(self.local_base_dir + 'dir\\file2.txt', 1024)

    prev_cwd = os.getcwd()
    os.chdir(self.local_base_dir + 'dir')
    try:
      # Uploading recursivly should pick up all files and dirs.
      res = utils.run_rsync('..', self.remote_base_dir, '-r')
      self.assertTrue(utils.files_count_is(res, missing=2, missing_dir=1))
      self._assert_remote_dir_contains(['file1.txt', 'dir/file2.txt'])

      # Uploading again should not change anything.
      res = utils.run_rsync('..', self.remote_base_dir, '-r')
      self.assertTrue(utils.files_count_is(res, matching=2, matching_dir=1))

      # Verify that non-recursive uploads do nothing.
      res = utils.run_rsync('..', self.remote_base_dir)
      self.assertTrue(utils.files_count_is(res, extraneous=1, extraneous_dir=1))
    finally:
      os.chdir(prev_cwd)

  def test_existing(self):
    """Runs rsync with --existing for a non-trivial directory.

       1) Uploads a source directory with -r.
       |-- rootdir
       |   |-- dir1
       |      |-- emptydir2
       |      |-- file1_1.txt
       |      |-- file1_2.txt -> rename to file1_3.txt (step 2)
       |      |-- (step2) emptydir3
       |   |-- dir2
       |      |-- file2_1.txt
       |   |-- emptydir1      -> rename emptydir4 (step 2)
       |   |-- file0.txt      -> change (step 2)
       2) Add new files/folders, remove and change some files/folders.
       3) Uploads the same source directory with --existing option and with -r.
       Only files existing on the server are changed, nothing is removed.
       4) Uploads the same source directory with --existing --delete -r.
       Files non-existing on the server are deleted.
    """
    local_root_path = self.local_base_dir + 'rootdir'
    remote_root_path = self.remote_base_dir + 'rootdir/'

    files = [
        '\\dir1\\file1_1.txt', '\\dir1\\file1_2.txt', '\\dir2\\file2_1.txt',
        '\\file0.txt'
    ]
    for file in files:
      utils.create_test_file(local_root_path + file, 1024)
    dirs = ['\\dir1\\emptydir2\\', '\\emptydir1\\']
    for directory in dirs:
      utils.create_test_directory(local_root_path + directory)
    res = utils.run_rsync(local_root_path, self.remote_base_dir, '-r')
    self._assert_rsync_success(res)

    utils.remove_test_file(local_root_path + '\\dir1\\file1_2.txt')
    utils.create_test_file(local_root_path + '\\dir1\\file1_3.txt', 1024)
    utils.create_test_directory(local_root_path + '\\dir1\\emptydir3\\')
    utils.remove_test_directory(local_root_path + '\\emptydir1\\')
    utils.create_test_directory(local_root_path + '\\emptydir4\\')
    utils.create_test_file(local_root_path + '\\file0.txt', 2034)

    res = utils.run_rsync(local_root_path, self.remote_base_dir, '-r',
                          '--existing')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(
            res,
            missing=1,
            missing_dir=2,
            matching=2,
            matching_dir=4,
            changed=1,
            extraneous=1,
            extraneous_dir=1))
    self.assertTrue(
        utils.does_directory_exist_remotely(remote_root_path + 'emptydir1'))
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'emptydir4'))
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path +
                                            'dir1/emptydir3'))
    self.assertTrue(
        utils.does_file_exist_remotely(remote_root_path + 'dir1/file1_2.txt'))
    self.assertFalse(
        utils.does_file_exist_remotely(remote_root_path + 'dir1/file1_3.txt'))

    res = utils.run_rsync(local_root_path, self.remote_base_dir, '-r',
                          '--existing', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(
            res,
            missing=1,
            missing_dir=2,
            matching=3,
            matching_dir=4,
            extraneous=1,
            extraneous_dir=1))
    self.assertIn('1/1 file(s) and 1/1 folder(s) deleted', res.stdout)
    self.assertFalse(
        utils.does_directory_exist_remotely(remote_root_path + 'emptydir1'))
    self.assertFalse(
        utils.does_file_exist_remotely(remote_root_path + 'dir2/file1_2.txt'))

  def test_copy_dest(self):
    r"""Runs rsync with --copy-dest option.

       Copies testdata.dat to 
       Copies the "cdc_rsync_e2e_test" package locally and syncs it with
       --copy-dest. Verifies that the files are actually sync'ed (D), not
       copied (C).

    Raises:
        Exception: On timeout waiting for mount to appear (after 20 seconds)
    """

    copy_dest_dir = self.remote_base_dir + 'copy_dest_dir'

    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, copy_dest_dir)
    self._assert_rsync_success(res)

    # Upload package using --package.
    res = utils.run_rsync('--copy-dest', copy_dest_dir, self.local_data_path,
                          self.remote_base_dir, '-v')
    self._assert_rsync_success(res)
    self.assertIn('D100%', res.stdout)
    self.assertNotIn('C100%', res.stdout)

  def test_upload_executables(self):
    """Uploads executable files and checks that they have the x bit set."""

    # Use the cdc rsync binaries as test executables.
    local_exe_path = utils.CDC_RSYNC_PATH
    local_elf_path = os.path.join(
        os.path.dirname(local_exe_path), 'cdc_rsync_server')

    remote_exe_path = self.remote_base_dir + os.path.basename(local_exe_path)
    remote_elf_path = self.remote_base_dir + os.path.basename(local_elf_path)

    # Copy the files to the remote instance.
    res = utils.run_rsync(local_exe_path, local_elf_path, self.remote_base_dir)
    self._assert_rsync_success(res)

    # Check that both files have the executable bit set.
    stats = utils.get_ssh_command_output('stat -c "%%a" %s %s' %
                                         (remote_exe_path, remote_elf_path))
    self.assertEqual(stats.count('755'), 2, stats)

    # Remove executable bits.
    utils.get_ssh_command_output('chmod -x %s %s' %
                                 (remote_exe_path, remote_elf_path))

    # Sync again, using -c to force a sync.
    res = utils.run_rsync('-c', local_exe_path, local_elf_path,
                          self.remote_base_dir)
    self._assert_rsync_success(res)

    # Validate that the executable bits were restored.
    stats = utils.get_ssh_command_output('stat -c "%%a" %s %s' %
                                         (remote_exe_path, remote_elf_path))
    self.assertEqual(stats.count('755'), 2, stats)

  def _run(self, args):
    logging.debug('Running %s', ' '.join(args))
    res = subprocess.run(args, capture_output=True)
    self.assertEqual(res.returncode, 0, 'Command failed: ' + str(res))
    res.stdout = res.stdout.decode('ascii')
    logging.debug('\r\n%s', res.stdout)
    return res


if __name__ == '__main__':
  test_base.test_base.main()
