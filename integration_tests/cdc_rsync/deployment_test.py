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
"""cdc_rsync deployment test."""

from integration_tests.framework import utils
from integration_tests.cdc_rsync import test_base

REMOTE_FOLDER = '~/.cache/cdc-file-transfer/bin/'


class DeploymentTest(test_base.CdcRsyncTest):
  """cdc_rsync deployment test class."""

  def _assert_deployment(self, initial_ts, file, msg):
    """Checks rsync and library are uploaded and the given file's timestamp matches initial_ts."""
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertIn(msg, str(res.stdout))
    changed_ts = utils.get_ssh_command_output('stat --format=%%y %s' %
                                              REMOTE_FOLDER + file)
    self.assertEqual(initial_ts, changed_ts)

  def _change_file_preserve_timestamp(self, file):
    """Changes a file preserving it timestamp."""
    utils.get_ssh_command_output(
        'touch -r %s %s' %
        (REMOTE_FOLDER + file, REMOTE_FOLDER + file + '.tmp'))
    utils.get_ssh_command_output('truncate -s +100 %s' % REMOTE_FOLDER + file)
    utils.get_ssh_command_output(
        'touch -r %s %s' %
        (REMOTE_FOLDER + file + '.tmp', REMOTE_FOLDER + file))
    utils.get_ssh_command_output('rm  %s' % (REMOTE_FOLDER + file + '.tmp'))

  def test_no_server(self):
    """Checks that cdc_rsync_server is uploaded if not present remotely.

      1) Wipes |REMOTE_FOLDER|.
      2) Uploads a file.
      3) Verifies that cdc_rsync_server exists in that folder.
    """
    utils.get_ssh_command_output('rm -rf %s*' % REMOTE_FOLDER)
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertIn('Server not deployed. Deploying...', str(res.stdout))
    self._assert_remote_dir_contains(['cdc_rsync_server'],
                                     remote_dir=REMOTE_FOLDER,
                                     pattern='"*"')

  def test_modified_server(self):
    """Checks that cdc_rsync_server is re-uploaded.

      1) Touches cdc_rsync_server in ‘REMOTE_FOLDER’.
      2) Uploads a file.
      3) Verifies that cdc_rsync_server is re-uploaded.
      4) Appends a few bytes to cdc_rsync_server while keeping its timestamp.
      6) Uploads a file.
      7) Verifies that cdc_rsync_server is re-uploaded.
    """
    # To be sure that cdc_rsync_server exist on the remote system
    # do an "empty" copy.
    utils.run_rsync(self.local_base_dir, self.remote_base_dir)

    remote_server_path = REMOTE_FOLDER + 'cdc_rsync_server'
    initial_ts = utils.get_ssh_command_output('stat --format=%%y %s' %
                                              remote_server_path)
    utils.get_ssh_command_output('touch -d \'1 November 2020 00:00\' %s' %
                                 remote_server_path)
    changed_ts = utils.get_ssh_command_output('stat --format=%%y %s' %
                                              remote_server_path)
    self.assertNotEqual(initial_ts, changed_ts)
    utils.create_test_file(self.local_data_path, 1024)
    self._assert_deployment(initial_ts, 'cdc_rsync_server',
                            'Server outdated. Redeploying...')

    self._change_file_preserve_timestamp('cdc_rsync_server')
    self._assert_deployment(initial_ts, 'cdc_rsync_server',
                            'Server outdated. Redeploying...')

  def test_read_only_server(self):
    """Checks that cdc_rsync_server is overwritten if it is read-only."""
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)

    # Modify cdc_rsync_server and wipe permissions.
    remote_server_path = REMOTE_FOLDER + 'cdc_rsync_server'
    utils.get_ssh_command_output('echo "xxx" > %s && chmod 0 %s' %
                                 (remote_server_path, remote_server_path))

    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertIn('Server failed to start. Redeploying...', str(res.stdout))


if __name__ == '__main__':
  test_base.test_base.main()
