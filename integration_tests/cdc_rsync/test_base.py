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
"""cdc_rsync base test class."""

import datetime
import logging
import tempfile
import re
import unittest

from integration_tests.framework import utils
from integration_tests.framework import test_base


class CdcRsyncTest(unittest.TestCase):
  """cdc_rsync base test class."""

  tmp_dir = None
  local_base_dir = None
  remote_base_dir = None
  local_data_path = None
  remote_data_path = None

  def setUp(self):
    """Cleans up the remote test data folder, logs a marker, and initializes random."""
    super(CdcRsyncTest, self).setUp()
    logging.debug('CdcRsyncTest -> setUp')

    utils.initialize(test_base.Flags.binary_path, None,
                     test_base.Flags.user_host)

    now_str = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    self.tmp_dir = tempfile.TemporaryDirectory(
        prefix=f'_cdc_rsync_test_{now_str}')
    self.local_base_dir = self.tmp_dir.name + '\\'
    self.remote_base_dir = f'/tmp/_cdc_rsync_test_{now_str}/'
    self.local_data_path = self.local_base_dir + 'testdata.dat'
    self.remote_data_path = self.remote_base_dir + 'testdata.dat'

    logging.info('Local base dir: "%s"', self.local_base_dir)
    logging.info('Remote base dir: "%s"', self.remote_base_dir)
    utils.initialize_random()

  def tearDown(self):
    """Cleans up the local and remote temp directories."""
    super(CdcRsyncTest, self).tearDown()
    logging.debug('CdcRsyncTest -> tearDown')
    self.tmp_dir.cleanup()
    utils.get_ssh_command_output(f'rm -rf {self.remote_base_dir}')

  def _assert_rsync_success(self, res):
    """Asserts if the return code is 0 and outputs return message with args."""
    self.assertEqual(res.returncode, 0, 'Return value is ' + str(res))

  def _assert_regex(self, regex, value):
    """Asserts that the regex string matches the given value."""
    self.assertIsNotNone(
        re.search(regex, value), f'"Regex {regex}" does not match "{value}"')

  def _assert_not_regex(self, regex, value):
    """Asserts that the regex string does not match the given value."""
    self.assertIsNone(
        re.search(regex, value),
        f'"Regex {regex}" unexpectedly matches "{value}"')

  def _assert_remote_dir_contains(self,
                                  file_list,
                                  remote_dir=None,
                                  pattern='"*.[t|d]*"'):
    """Asserts that the remote base dir contains exactly the list of files.

    Args:
        file_list (list of strings): List of relative file paths to check
        remote_dir (string, optional): Remote directory. Defaults to
            remote_base_dir
        pattern (string, optional): Pattern for matching file names.
    """
    find_res = utils.get_ssh_command_output(
        'cd %s && find -name %s -print' %
        (remote_dir or self.remote_base_dir, pattern))

    # Note that assertCountEqual compares items independently of order
    # (not just the size of the list).
    found = sorted(
        filter(lambda item: item and item != '.', find_res.split('\r\n')))
    expected = sorted(['./' + f for f in file_list])
    self.assertListEqual(found, expected)

  def _assert_remote_dir_does_not_contain(self, file_list):
    """Asserts that the remote base dir contains none of the listed files.

    Args:
        file_list (list of strings): List of relative file paths to check
    """
    find_res = utils.get_ssh_command_output(
        'cd %s && find -name "*.[t|d]*" -print' % self.remote_base_dir)

    found = set(file_name for file_name in filter(None, find_res.split('\n')))

    for file in file_list:
      self.assertNotIn('./' + file, found)
