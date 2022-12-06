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
"""cdc_rsync dry-run test."""

from integration_tests.framework import utils
from integration_tests.cdc_rsync import test_base


class DryRunTest(test_base.CdcRsyncTest):
  """cdc_rsync dry-run test class."""

  def test_dry_run(self):
    """Verifies --dry-run option.

      1) Uploads file1.txt and file2.txt.
      2) Modifies file2.txt.
      3) Dry-runs file2.txt and file3.txt with --dry-run -r --delete.
      Result: a missing (file3.txt), a changed (file2.txt) and an extraneous
      (file1.txt) file. No files should be changed on the server.

    """

    files = ['file1.txt', 'file2.txt', 'file3.txt']

    for file in files:
      utils.create_test_file(self.local_base_dir + file, 987)

    res = utils.run_rsync(self.local_base_dir + 'file1.txt',
                          self.local_base_dir + 'file2.txt',
                          self.remote_base_dir, '-v')
    self._assert_rsync_success(res)
    self._assert_remote_dir_contains(['file1.txt', 'file2.txt'])

    # Dry-run of uploading changed/new/to delete files.
    utils.create_test_file(self.local_base_dir + 'file2.txt', 2534)
    res = utils.run_rsync(self.local_base_dir + 'file2.txt',
                          self.local_base_dir + 'file3.txt',
                          self.remote_base_dir, '-v', '--dry-run', '--delete',
                          '-r')
    self._assert_rsync_success(res)
    self.assertTrue(
        utils.files_count_is(res, missing=1, changed=1, extraneous=1))
    self._assert_remote_dir_does_not_contain(['file3.txt'])
    self._assert_remote_dir_contains(['file1.txt', 'file2.txt'])
    self.assertIn('file1.txt', str(res.stdout))
    self.assertIn('deleted 1 / 1', str(res.stdout))
    self.assertIn('file2.txt', str(res.stdout))
    self.assertIn('D100%', str(res.stdout))
    self.assertIn('file3.txt', str(res.stdout))
    self.assertIn('C100%', str(res.stdout))
    self.assertFalse(
        utils.sha1_matches(self.local_base_dir + 'file2.txt',
                           self.remote_base_dir + 'file2.txt'))

  def test_dry_run_sync_folder_when_remote_file_recursive_with_delete(self):
    """Dry-runs a recursive upload of a folder while removing a remote file with the same name with --delete."""

    local_folder = self.local_base_dir + 'foldertocopy\\'
    utils.create_test_directory(local_folder)
    utils.get_ssh_command_output(
        'mkdir -p %s && touch %s' %
        (self.remote_base_dir, self.remote_base_dir + 'foldertocopy'))

    res = utils.run_rsync(self.local_base_dir + 'foldertocopy',
                          self.remote_base_dir, '-r', '--dry-run', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, extraneous=1, missing_dir=1))
    self.assertFalse(
        utils.does_directory_exist_remotely(self.remote_base_dir +
                                            'foldertocopy'))
    self.assertTrue(
        utils.does_file_exist_remotely(self.remote_base_dir + 'foldertocopy'))
    self.assertIn('1/1 file(s) and 0/0 folder(s) deleted', str(res.stdout))

  def test_dry_run_sync_file_when_remote_folder_recursive_with_delete(self):
    """Dry-runs a recursive upload of a file while removing an empty remote folder with the same name with --delete."""
    utils.create_test_file(self.local_data_path, 1024)
    utils.get_ssh_command_output('mkdir -p %s' % self.remote_data_path)

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          '--dry-run', '-r', '--delete')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1, extraneous_dir=1))
    self.assertFalse(utils.does_file_exist_remotely(self.remote_data_path))
    self.assertTrue(utils.does_directory_exist_remotely(self.remote_data_path))
    self.assertIn('0/0 file(s) and 1/1 folder(s) deleted', str(res.stdout))

  def test_dry_run_sync_file_when_remote_folder_empty(self):
    """Dry-runs a non-recursive upload of a file while there is an empty remote folder with the same name."""
    utils.create_test_file(self.local_data_path, 1024)
    utils.get_ssh_command_output('mkdir -p %s' % self.remote_data_path)

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir,
                          '--dry-run')
    self._assert_rsync_success(res)
    self.assertTrue(utils.files_count_is(res, missing=1, extraneous_dir=1))
    self.assertFalse(utils.does_file_exist_remotely(self.remote_data_path))
    self.assertTrue(utils.does_directory_exist_remotely(self.remote_data_path))
    self.assertNotIn('0/0 file(s) and 1/1 folder(s) deleted', str(res.stdout))


if __name__ == '__main__':
  test_base.test_base.main()
