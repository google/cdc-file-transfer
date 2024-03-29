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
"""cdc_rsync connection test."""

from concurrent import futures
import socket
import time

from integration_tests.framework import utils
from integration_tests.cdc_rsync import test_base

RETURN_CODE_SUCCESS = 0
RETURN_CODE_GENERIC_ERROR = 1
RETURN_CODE_CONNECTION_TIMEOUT = 2
RETURN_CODE_ADDRESS_IN_USE = 4


class ConnectionTest(test_base.CdcRsyncTest):
  """cdc_rsync connection test class."""

  def test_invalid_instance(self):
    """Runs rsync with an invalid host"""
    bad_host = 'bad_host'

    utils.create_test_file(self.local_data_path, 1024)
    res = utils.run_rsync(self.local_data_path,
                          bad_host + ":" + self.remote_base_dir)
    self.assertEqual(res.returncode, RETURN_CODE_GENERIC_ERROR)
    self.assertIn('Failed to detect remote architecture', str(res.stderr))

  def test_contimeout(self):
    """Runs rsync with --contimeout option for an invalid ip.

       1) Uploads a file with bad IP address.
       2) Checks the error message and that it timed out after ~5 seconds.
       3) Uploads a file with bad IP address and --contimeout 1.
       4) Checks the error message and that it timed out after ~1 second.

    """
    utils.create_test_file(self.local_data_path, 1024)
    bad_host = '192.0.2.1'
    start = time.time()
    res = utils.run_rsync(self.local_data_path,
                          bad_host + ":" + self.remote_base_dir)
    elapsed_time = time.time() - start
    self.assertGreater(elapsed_time, 4.5)
    self.assertEqual(res.returncode, RETURN_CODE_CONNECTION_TIMEOUT)
    self.assertIn('Error: Server connection timed out', str(res.stderr))

    start = time.time()
    res = utils.run_rsync(self.local_data_path,
                          bad_host + ":" + self.remote_base_dir,
                          '--contimeout=1')
    elapsed_time = time.time() - start
    self.assertLess(elapsed_time, 3)
    self.assertEqual(res.returncode, RETURN_CODE_CONNECTION_TIMEOUT)
    self.assertIn('Error: Server connection timed out', str(res.stderr))

  def test_multiple_instances(self):
    """Runs multiple instances of rsync at the same time."""
    num_instances = 10

    local_data_paths = []
    for n in range(num_instances):
      path = self.local_base_dir + ('testdata_%i.dat' % n)
      utils.create_test_file(path, 1024)
      local_data_paths.append(path)

    with futures.ThreadPoolExecutor(max_workers=num_instances) as executor:
      res = []
      for n in range(num_instances):
        res.append(
            executor.submit(utils.run_rsync, local_data_paths[n],
                            self.remote_base_dir))
      for r in res:
        self._assert_rsync_success(r.result())


if __name__ == '__main__':
  test_base.test_base.main()
