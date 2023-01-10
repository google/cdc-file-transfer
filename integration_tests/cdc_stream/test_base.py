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
"""cdc_stream test."""

import datetime
import logging
import os
import posixpath
import tempfile
import time
import subprocess
import unittest

from integration_tests.framework import utils
from integration_tests.framework import test_base


class CdcStreamTest(unittest.TestCase):
  """cdc_stream test class."""

  # Grpc status codes.
  NOT_FOUND = 5
  SERVICE_UNAVAILABLE = 14

  tmp_dir = None
  local_base_dir = None
  remote_base_dir = '/tmp/_cdc_stream_test/'
  cache_dir = '~/.cache/cdc-file-transfer/chunks/'
  service_port_arg = None
  service_running = False

  # Returns a list of files and directories with mtimes in the remote directory.
  # For example, 2021-10-13 07:49:25.512766391 -0700 /tmp/_cdc_stream_test/2.txt
  ls_cmd = ('find %s -exec stat --format \"%%y %%n\" \"{}\" \\;') % (
      remote_base_dir)

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()
    logging.debug('CdcStreamTest -> setUpClass')

    utils.initialize(None, test_base.Flags.binary_path,
                     test_base.Flags.user_host)
    cls.service_port_arg = f'--service-port={test_base.Flags.service_port}'
    cls._stop_service()
    with tempfile.NamedTemporaryFile() as tf:
      cls.config_path = tf.name

  @classmethod
  def tearDownClass(cls):
    logging.debug('CdcStreamTest -> tearDownClass')
    cls._stop_service()
    if os.path.exists(cls.config_path):
      os.remove(cls.config_path)

  def setUp(self):
    """Stops the service, cleans up cache and streamed directory and initializes random."""
    super(CdcStreamTest, self).setUp()
    logging.debug('CdcStreamTest -> setUp')

    now_str = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    self.tmp_dir = tempfile.TemporaryDirectory(
        prefix=f'_cdc_stream_test_{now_str}')
    self.local_base_dir = self.tmp_dir.name + '\\base\\'
    utils.create_test_directory(self.local_base_dir)

    logging.info('Local base dir: "%s"', self.local_base_dir)
    logging.info('Remote base dir: "%s"', self.remote_base_dir)
    utils.initialize_random()
    self._stop(ignore_not_found=True)
    self._clean_cache()

  def tearDown(self):
    super(CdcStreamTest, self).tearDown()
    logging.debug('CdcStreamTest -> tearDown')
    self.tmp_dir.cleanup()

  @classmethod
  def _start_service(cls, config_json=None):
    """Starts the asset streaming service.

    Args:
        config_json (string, optional): Config JSON string. Defaults to None.
    """
    config_arg = None
    if config_json:
      with open(cls.config_path, 'wt') as file:
        file.write(config_json)
      config_arg = f'--config-file={cls.config_path}'

    # Note: Service must be spawned in a background process.
    args = ['start-service', config_arg, cls.service_port_arg]
    command = [utils.CDC_STREAM_PATH, *filter(None, args)]

    # Workaround issue with unicode logging.
    logging.debug(
        'Executing %s ',
        ' '.join(command).encode('utf-8').decode('ascii', 'backslashreplace'))
    subprocess.Popen(command)
    cls.service_running = True

  @classmethod
  def _stop_service(cls):
    res = utils.run_stream('stop-service', cls.service_port_arg)
    if res.returncode != 0:
      logging.warn(f'Stopping service failed: {res}')
    cls.service_running = False

  def _start(self, local_dir=None):
    """Starts streaming the given directory

    Args:
        local_dir (string): Directory to stream. Defaults to local_base_dir.
    """
    res = utils.run_stream('start', local_dir or self.local_base_dir,
                           utils.target(self.remote_base_dir),
                           self.service_port_arg)
    self._assert_stream_success(res)

  def _stop(self, ignore_not_found=False):
    """Stops streaming to the target

    Args:
        local_dir (string): Directory to stream. Defaults to local_base_dir.
    """
    if not self.service_running:
      return
    res = utils.run_stream('stop', utils.target(self.remote_base_dir),
                           self.service_port_arg)
    if ignore_not_found and res.returncode == self.NOT_FOUND:
      return
    self._assert_stream_success(res)

  def _assert_stream_success(self, res):
    """Asserts if the return code is 0 and outputs return message with args."""
    self.assertEqual(res.returncode, 0, 'Return value is ' + str(res))

  def _assert_remote_dir_matches(self, file_list):
    """Asserts that the remote directory matches the list of files and directories.

    Args:
        file_list (list of strings): List of relative paths to check.
    """
    found = utils.get_sorted_files(self.remote_base_dir, '"*"')
    expected = sorted(['./' + f for f in file_list])
    self.assertListEqual(found, expected)

  def _get_cache_size_in_bytes(self):
    """Returns the asset streaming cache size in bytes.

    Returns:
        bool: Cache size in bytes.
    """
    result = utils.get_ssh_command_output('du -sb %s | awk \'{print $1}\'' %
                                          self.cache_dir)
    logging.info(f'Cache capacity is {int(result)}')
    return int(result)

  def _assert_cache(self):
    """Asserts that the asset streaming cache contains some data."""
    cache_size = self._get_cache_size_in_bytes()
    # On Linux, an empty directory occupies 4KB.
    self.assertTrue(int(cache_size) >= 4096)
    self.assertGreater(
        int(utils.get_ssh_command_output('ls %s | wc -l' % self.cache_dir)), 0)

  def _assert_cdc_fuse_mounted(self, success=True):
    """Asserts that CDC FUSE is appropriately mounted."""
    logging.info(f'Asserting that FUSE is {"" if success else "not "}mounted')
    result = utils.get_ssh_command_output('cat /etc/mtab | grep fuse')
    if success:
      self.assertIn(f'{self.remote_base_dir[:-1]} fuse.', result)
    else:
      self.assertNotIn(f'{self.remote_base_dir[:-1]} fuse.', result)

  def _clean_cache(self):
    """Removes all data from the asset streaming caches."""
    logging.info(f'Clearing cache')
    utils.get_ssh_command_output('rm -rf %s' %
                                 posixpath.join(self.cache_dir, '*'))
    cache_dir = os.path.join(os.environ['APPDATA'], 'cdc-file-transfer',
                             'chunks')
    utils.remove_test_directory(cache_dir)

  def _create_test_data(self, files, dirs):
    """Create test data locally.

    Args:
        files (list of strings): List of relative file paths to create.
        dirs (list of strings): List of relative dir paths to create.
    """
    logging.info(
        f'Creating test data with {len(files)} files and {len(dirs)} dirs')
    for directory in dirs:
      utils.create_test_directory(os.path.join(self.local_base_dir, directory))
    for file in files:
      utils.create_test_file(os.path.join(self.local_base_dir, file), 1024)

  def _wait_until_remote_dir_changed(self, original, counter=20):
    """Wait until the directory content has changed.

    Args:
        original (string): The original file list of the remote directory.
        counter (int): The number of retries.
    Returns:
        bool: Whether the content of the remote directory has changed.
    """
    logging.info(f'Waiting until remote dir changes')
    for _ in range(counter):
      if utils.get_ssh_command_output(self.ls_cmd) != original:
        return True
      time.sleep(0.1)
      logging.info(f'Still waiting...')
    return False

  def _test_dir_content(self, files, dirs, is_exe=False):
    """Check the streamed directory's content on remote instance.

    Args:
        files (list of strings): List of relative file paths to check.
        dirs (list of strings): List of relative dir paths to check.
        is_exe (bool): Flag which identifies whether files are executables.
    """
    logging.info(
        f'Testing dir content with {len(files)} files and {len(dirs)} dirs')
    dirs = [directory.replace('\\', '/').rstrip('/') for directory in dirs]
    files = [file.replace('\\', '/') for file in files]

    # Read the content of the directory once to load some data.
    utils.get_ssh_command_output('ls -al %s' % self.remote_base_dir)
    self._assert_remote_dir_matches(files + dirs)
    if not dirs and not files:
      return
    file_list = list()
    mapping = dict()
    for file in files:
      full_name = posixpath.join(self.remote_base_dir, file)
      self.assertTrue(
          utils.sha1_matches(
              os.path.join(self.local_base_dir, file), full_name))
      file_list.append(full_name)
      if is_exe:
        mapping[full_name] = '-rwxr-xr-x'
      else:
        mapping[full_name] = '-rw-r--r--'
    for directory in dirs:
      full_name = posixpath.join(self.remote_base_dir, directory)
      file_list.append(full_name)
      mapping[full_name] = 'drwxr-xr-x'

    ls_res = utils.get_ssh_command_output('ls -ld %s' % ' '.join(file_list))
    for line in ls_res.splitlines():
      self.assertIn(mapping[list(filter(None, line.split(' ')))[8]], line)
