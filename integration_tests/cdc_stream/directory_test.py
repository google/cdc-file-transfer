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
"""cdc_stream directory Test."""

import os

from integration_tests.framework import utils
from integration_tests.cdc_stream import test_base


class DirectoryTest(test_base.CdcStreamTest):
  """cdc_stream test class for modifications of streamed directory."""

  def _assert_mount_fails(self, directory):
    """Check that mounting a directory fails.

    Args:
        directory (string): name of a file/directory to be streamed.
    """
    with self.assertRaises(Exception):
      self._start(directory)

  def test_recreate_streamed_dir(self):
    """Survive recreation of a streamed directory.

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
    self._create_test_data(files, dirs)
    self._start()
    self._test_dir_content(files=files, dirs=dirs)
    self._assert_cache()
    self._assert_cdc_fuse_mounted()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Remove directory on workstation => empty remote directory.
    utils.remove_test_directory(self.local_base_dir)
    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=[], dirs=[])
    self._assert_cdc_fuse_mounted()

    original = utils.get_ssh_command_output(self.ls_cmd)
    # Recreate directory, add files => the content becomes visible again.
    self._create_test_data(files, dirs)
    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=files, dirs=dirs)
    self._assert_cdc_fuse_mounted()

  def test_non_existing_streamed_dir_fail(self):
    """Fail if the streamed directory does not exist."""
    streamed_dir = os.path.join(self.local_base_dir, 'non_existing')
    self._assert_mount_fails(streamed_dir)
    self._test_dir_content(files=[], dirs=[])
    self._assert_cdc_fuse_mounted(success=False)

  def test_streamed_dir_as_file_fail(self):
    """Fail if the streamed path is a file."""
    streamed_file = os.path.join(self.local_base_dir, 'file')
    utils.create_test_file(streamed_file, 1024)
    self._assert_mount_fails(streamed_file)
    self._test_dir_content(files=[], dirs=[])
    self._assert_cdc_fuse_mounted(success=False)

  def test_remount_recreated_streamed_dir(self):
    """Remounting a directory, which is currently removed, stops streaming session."""
    files = [
        'dir1\\file1_1.txt', 'dir1\\file1_2.txt', 'dir2\\file2_1.txt',
        'file0.txt'
    ]
    dirs = ['dir1\\emptydir2\\', 'emptydir1\\', 'dir1\\', 'dir2\\']
    self._create_test_data(files, dirs)
    self._start()
    self._test_dir_content(files=files, dirs=dirs)
    self._assert_cache()
    self._assert_cdc_fuse_mounted()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Remove directory on workstation => empty remote directory.
    utils.remove_test_directory(self.local_base_dir)
    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=[], dirs=[])
    self._assert_cdc_fuse_mounted()

    # Remount for the same directory fails and stops an existing session.
    self._assert_mount_fails(self.local_base_dir)
    self._test_dir_content(files=[], dirs=[])

    # Create a new folder and mount -> should succeed.
    test_dir = 'Temp'
    file_name = 'test_file.txt'
    utils.create_test_file(
        os.path.join(self.local_base_dir, test_dir, file_name), 100)
    self._start(os.path.join(self.local_base_dir, test_dir))
    self._assert_remote_dir_matches([file_name])


if __name__ == '__main__':
  test_base.test_base.main()
