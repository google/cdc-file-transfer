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

from integration_tests.framework import test_runner


class Flags(object):
  binary_path = None
  user_host = None
  service_port = 0


def main():
  parser = argparse.ArgumentParser(description='End-to-end integration test.')
  parser.add_argument('--binary_path', help='Target [user@]host', required=True)
  parser.add_argument('--user_host', help='Target [user@]host', required=True)
  parser.add_argument(
      '--service_port',
      type=int,
      help='Asset streaming service port',
      default=44432)
  parser.add_argument('--log_file', help='Log file path')

  # Capture all remaining arguments to pass to unittest.main().
  args, unittest_args = parser.parse_known_args()
  Flags.binary_path = args.binary_path
  Flags.user_host = args.user_host
  Flags.service_port = args.service_port

  # Log to STDERR
  log_format = ('%(levelname)-8s%(asctime)s '
                '%(filename)s:%(lineno)-3d %(message)s')
  log_stream = sys.stderr

  if args.log_file:
    log_stream = open(args.log_file, 'w')

  with log_stream:
    logging.basicConfig(
        format=log_format, level=logging.DEBUG, stream=log_stream)

    unittest.main(
        argv=sys.argv[:1] + unittest_args, testRunner=test_runner.TestRunner())
