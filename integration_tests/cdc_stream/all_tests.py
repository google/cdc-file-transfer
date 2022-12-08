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
import unittest

from integration_tests.cdc_stream import cache_test
from integration_tests.cdc_stream import consistency_test
from integration_tests.cdc_stream import directory_test
from integration_tests.cdc_stream import general_test
from integration_tests.framework import test_base


# pylint: disable=g-doc-args,g-doc-return-or-yield
def load_tests(loader, unused_tests, unused_pattern):
  """Customizes the list of test cases to run.

  See the Python documentation for details:
  https://docs.python.org/3/library/unittest.html#load-tests-protocol
  """
  suite = unittest.TestSuite()
  suite.addTests(loader.loadTestsFromModule(cache_test))
  suite.addTests(loader.loadTestsFromModule(consistency_test))
  suite.addTests(loader.loadTestsFromModule(directory_test))
  suite.addTests(loader.loadTestsFromModule(general_test))

  return suite


if __name__ == '__main__':
  test_base.main()
