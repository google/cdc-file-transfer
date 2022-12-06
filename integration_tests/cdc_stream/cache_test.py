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
"""cdc_stream cache test."""

import logging
import os
import posixpath
import time

from integration_tests.framework import utils
from integration_tests.cdc_stream import test_base


class CacheTest(test_base.CdcStreamTest):
  """cdc_stream test class for cache."""

  cache_capacity = 10 * 1024 * 1024  # 10MB
  cleanup_timeout_sec = 2
  access_idle_timeout_sec = 2
  cleanup_time = 5  # estimated cleanup time
  # Returns a list of files and directories with mtimes in the cache.
  # 2021-10-19 01:09:30.070055513 -0700 /var/cache/asset_streaming
  cache_cmd = ('find %s -exec stat --format \"%%y %%n\" '
               '\"{}\" \\;') % (
                   test_base.CdcStreamTest.cache_dir)

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    logging.debug('CacheTest -> setUpClass')

    config_json = ('{\"cache-capacity\":\"%s\",\"cleanup-timeout\":%i,'
                   '\"access-idle-timeout\":%i}') % (
                       cls.cache_capacity, cls.cleanup_timeout_sec,
                       cls.access_idle_timeout_sec)
    cls._start_service(config_json)

  def test_cache_reused(self):
    """Cache survives remount and is reused."""
    filename = '1.txt'
    utils.create_test_file(
        os.path.join(self.local_base_dir, filename), 7 * 1024 * 1024)
    self._start()
    self._test_dir_content(files=[filename], dirs=[])
    # Read the file => fill the cache.file_transfer
    utils.get_ssh_command_output('cat %s > /dev/null' %
                                 posixpath.join(self.remote_base_dir, filename))
    cache_size = self._get_cache_size_in_bytes()
    cache_files = utils.get_ssh_command_output(self.cache_cmd)

    self._stop()
    self._assert_cdc_fuse_mounted(success=False)
    self._assert_cache()

    self._start()
    self._assert_cdc_fuse_mounted()
    self._test_dir_content(files=[filename], dirs=[])
    utils.get_ssh_command_output('cat %s > /dev/null' %
                                 posixpath.join(self.remote_base_dir, filename))
    # The same manifest should be re-used. No change in the cache is expected.
    self.assertEqual(self._get_cache_size_in_bytes(), cache_size)
    # The mtimes of the files should have changed after each Get() operation.
    self.assertNotEqual(
        utils.get_ssh_command_output(self.cache_cmd), cache_files)

  def test_set_cache_capacity_old_chunks_removed(self):
    # Command to return the oldest mtime in the cache directory.
    ts_cmd = ('find %s -type f -printf \"%%T@\\n\" '
              '| sort -n | head -n 1') % (
                  self.cache_dir)
    # Stream a file.
    filename = '1.txt'
    utils.create_test_file(
        os.path.join(self.local_base_dir, filename), 11 * 1024 * 1024)
    self._start()
    self._test_dir_content(files=[filename], dirs=[])
    utils.get_ssh_command_output('cat %s > /dev/null' %
                                 posixpath.join(self.remote_base_dir, filename))
    # Extract the oldest file.
    oldest_ts = utils.get_ssh_command_output(ts_cmd)
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Add and read one more file.
    filename2 = '2.txt'
    utils.create_test_file(
        os.path.join(self.local_base_dir, filename2), 11 * 1024 * 1024)
    self.assertTrue(self._wait_until_remote_dir_changed(original))
    utils.get_ssh_command_output(
        'cat %s > /dev/null' % posixpath.join(self.remote_base_dir, filename2))

    # Wait some time till the cache is cleaned up.
    wait_sec = self.cleanup_timeout_sec + self.access_idle_timeout_sec + self.cleanup_time
    logging.info(f'Waiting {wait_sec} seconds until the cache is cleaned up')
    time.sleep(wait_sec)
    self.assertLessEqual(self._get_cache_size_in_bytes(), self.cache_capacity)
    new_oldest_ts = utils.get_ssh_command_output(ts_cmd)

    self.assertGreater(new_oldest_ts, oldest_ts)
    self._test_dir_content(files=[filename, filename2], dirs=[])


if __name__ == '__main__':
  test_base.test_base.main()
