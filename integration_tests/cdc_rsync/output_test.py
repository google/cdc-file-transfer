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
"""cdc_rsync output test."""

import json

from integration_tests.framework import utils
from integration_tests.cdc_rsync import test_base


class OutputTest(test_base.CdcRsyncTest):
  """cdc_rsync output test class."""

  def test_plain(self):
    """Runs rsync and verifies the total progress.

      1) Uploads a file, verifies that the total progress is shown.
      2) Uploads an empty folder with -r --delete options.
      Verifies that the total delete messages are shown.
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir)
    self._assert_rsync_success(res)
    self.assertIn('100% TOT', str(res.stdout))

    utils.remove_test_file(self.local_data_path)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-r',
                          '--delete')
    self._assert_rsync_success(res)
    self.assertIn('1/1 file(s) and 0/0 folder(s) deleted', str(res.stdout))

  def test_verbose_1(self):
    """Runs rsync with -v option for multiple files.

      1) Uploads 3 files with ‘-v’.
      Verifies that each file is listed in the output as ‘C100%’.
      2) Modifies 3 files, uploads them again with ‘--v’.
      Verifies that each file is listed in the output as ‘D100%’.
      3) Uploads an empty folder with -r --delete options.
      Verifies that the delete messages are shown.
    """
    files = ['file1. txt', 'file2.txt', 'file3.txt']
    for file in files:
      utils.create_test_file(self.local_base_dir + file, 1024)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-v', '-r')
    self._assert_rsync_success(res)
    self.assertEqual(3, str(res.stdout).count('C100%'))

    for file in files:
      utils.create_test_file(self.local_base_dir + file, 2048)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-v', '-r')
    self._assert_rsync_success(res)
    self.assertEqual(3, str(res.stdout).count('D100%'))

    for file in files:
      utils.remove_test_file(self.local_base_dir + file)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-r',
                          '--delete')
    self._assert_rsync_success(res)
    self.assertIn('will be deleted due to --delete', str(res.stdout))
    self.assertIn('3/3 file(s) and 0/0 folder(s) deleted', str(res.stdout))

  def test_verbose_2(self):
    """Runs rsync with -vv option.

      1) Uploads a file with ‘-vv’.
      2) Verifies that additional logs show up.
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-vv')
    self._assert_rsync_success(res)
    output = str(res.stdout)

    # client-side output
    self._assert_regex('Starting process', output)
    self._assert_not_regex(
        r'process\.cc\([0-9]+\): Start\(\): Starting process', output)

    # server-side output
    self._assert_regex(
        'INFO    Finding all files in destination folder '
        f"'{self.remote_base_dir}'", output)
    self.assertNotIn('DEBUG', output)

  def test_verbose_3(self):
    """Runs rsync with -vvv option.

      1) Uploads a file with ‘-vvv’.
      Verifies that additional logs show up (LOG_DEBUG logs).
      2) Uploads a file to  ‘/invalid’ with ‘-vvv’.
      Verifies that error messages including filenames are shown.
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-vvv')
    self._assert_rsync_success(res)
    output = str(res.stdout)

    # client-side output
    self._assert_regex(
        r'cdc_rsync_client\.cc\([0-9]+\): SendOptions\(\): Sending options',
        output)

    # server-side output
    self._assert_regex(
        r'DEBUG   server_socket\.cc\([0-9]+\): Receive\(\): EOF\(\) detected',
        output)

    # TODO: Add a check here, as currently the output is misleading
    # res = utils.run_rsync(self.local_data_path, '/invalid', '-vvv')

  def test_verbose_4(self):
    """Runs rsync with -vvv option.

      1) Uploads a file with ‘-vvvv’.
      2) Verifies that additional logs show up (LOG_VERBOSE logs).
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-vvvv')
    self._assert_rsync_success(res)
    output = str(res.stdout)

    # client-side output
    self._assert_regex(
        r'message_pump\.cc\([0-9]+\): ThreadDoSendPacket\(\): Sent packet of size',
        output)

    # server-side output
    self._assert_regex(
        r'VERBOSE message_pump\.cc\([0-9]+\): ThreadDoReceivePacket\(\): Received packet of size',
        output)

  def test_quiet(self):
    """Runs rsync with -q option.

      1) Uploads a file with ‘-q’.
      2) Verifies that no output is shown.
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-q')
    self._assert_rsync_success(res)
    self.assertEqual('\r\n', res.stdout)

  def test_quiet_error(self):
    """Runs rsync with -q option still showing errors.

      1) Uploads a file with ‘-q’ and bad options.
      2) Verifies that an error message is shown.
    """
    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path, self.remote_base_dir, '-q',
                          '-t')
    self.assertEqual(res.returncode, 1)
    self.assertEqual('\r\n', str(res.stdout))
    self.assertIn('Unknown option: \'t\'', str(res.stderr))
    # TODO: Add a test case for the non-existing destination.

  def test_existing_verbose_1(self):
    """Runs rsync with -v --existing."""

    files = ['file1.txt', 'file2.txt']
    for file in files:
      utils.create_test_file(self.local_base_dir + file, 1024)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-r')
    self._assert_rsync_success(res)

    files.append('file3.txt')
    for file in files:
      utils.create_test_file(self.local_base_dir + file, 2048)
    res = utils.run_rsync(self.local_base_dir, self.remote_base_dir, '-v', '-r',
                          '--existing')
    self._assert_rsync_success(res)
    output = str(res.stdout)
    self.assertEqual(2, output.count('D100%'))
    self.assertNotIn('file3.txt', output)

  def test_json_per_file(self):
    """Runs rsync with -v --json."""

    local_path = self.local_base_dir + 'test.txt'
    utils.create_test_file(local_path, 1024)
    res = utils.run_rsync(local_path, self.remote_base_dir, '-v', '--json')
    self._assert_rsync_success(res)
    output = str(res.stdout)

    for val in self.parse_json(output):
      self.assertEqual(val['file'], 'test.txt')
      self.assertEqual(val['operation'], 'Copy')
      self.assertEqual(val['size'], 1024)

      # Those are actually all floats, but sometimes they get rounded to ints.
      self.assertTrue(self.is_float_or_int(val['bytes_per_second']))
      self.assertTrue(self.is_float_or_int(val['duration']))
      self.assertTrue(self.is_float_or_int(val['eta']))
      self.assertTrue(self.is_float_or_int(val['total_duration']))
      self.assertTrue(self.is_float_or_int(val['total_eta']))
      self.assertTrue(self.is_float_or_int(val['total_progress']))

  def test_json_total(self):
    """Runs rsync with --json."""

    local_path = self.local_base_dir + 'test.txt'
    utils.create_test_file(local_path, 1024)
    res = utils.run_rsync(local_path, self.remote_base_dir, '--json')
    self._assert_rsync_success(res)
    output = str(res.stdout)

    for val in self.parse_json(output):
      self.assertNotIn('file', val)

      # Those are actually all floats, but sometimes they get rounded to ints.
      self.assertTrue(self.is_float_or_int(val['total_duration']))
      self.assertTrue(self.is_float_or_int(val['total_eta']))
      self.assertTrue(self.is_float_or_int(val['total_progress']))

  def parse_json(self, output):
    """Parses the JSON lines of output."""
    lines = output.split('\r\n')
    json_values = []
    for line in lines:
      if str.startswith(line, '{'):
        json_values.append(json.loads(line.strip()))
    return json_values

  def is_float_or_int(self, val):
    """Returns true if val is a float or an int."""
    return isinstance(val, float) or isinstance(val, int)


if __name__ == '__main__':
  test_base.test_base.main()
