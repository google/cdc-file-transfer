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
"""Test main and flags."""

import argparse
import contextlib
import logging
import sys
import unittest

from integration_tests.framework import xml_test_runner

APPDATA = 'APPDATA'


class Flags(object):
  binary_path = None
  user_host = None
  ssh_port = 22


class TestCase(unittest.TestCase):

  @contextlib.contextmanager
  def subTest(self, name='Missing Name', must_pass=True, **kwargs):
    """Create a block of code that is reported as a separate subtest.

    Args:
      name: str, identifier for the subtest
      must_pass: bool, if True, testcase will fail immediately if subtest fails
      **kwargs: keyword arguments passed to the subTest

    Yields:
       Yields a context manager which executes the enclosed code block as a
       subtest.
    """
    succeeded = False
    with super(TestCase, self).subTest(name=name, **kwargs):
      yield
      succeeded = True
    self.assertTrue(succeeded or not must_pass,
                    'Cannot continue after failure in subtest "%s"' % name)


def main():
  parser = argparse.ArgumentParser(description='End-to-end integration test.')
  parser.add_argument('--binary_path', help='Target [user@]host', required=True)
  parser.add_argument('--user_host', help='Target [user@]host', required=True)
  parser.add_argument(
      '--ssh_port',
      type=int,
      help='SSH port for connecting to the host',
      default=22)
  parser.add_argument('--xml_result_file', help='XML file with test results')
  parser.add_argument('--log_file', help='Log file path')

  # Capture all remaining arguments to pass to unittest.main().
  args, unittest_args = parser.parse_known_args()
  Flags.binary_path = args.binary_path
  Flags.user_host = args.user_host
  Flags.ssh_port = args.ssh_port

  # Log to STDERR
  log_format = ('%(levelname)-8s%(asctime)s  %(process)-8d'
                '%(filename)s:%(lineno)-3d] %(message)s')
  log_stream = sys.stderr

  if args.log_file:
    log_stream = open(args.log_file, 'w')

  with log_stream:
    logging.basicConfig(
        format=log_format, level=logging.DEBUG, stream=log_stream)

    unittest.main(
        argv=sys.argv[:1] + unittest_args,
        testRunner=xml_test_runner.XmlTestRunner(
            flags=Flags, filename=args.xml_result_file))
