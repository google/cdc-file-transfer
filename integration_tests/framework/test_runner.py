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
"""Test runner, adds some sugar around logs to make them easier to read."""

import logging
import traceback
import unittest


class SubTestFailure(RuntimeError):
  """Raised when a must-pass subtest fails."""


class TestRunner(object):
  """Runner producing test xml output."""

  def run(self, test):  # pylint: disable=invalid-name
    result = TestResult()
    logging.info('Running tests...')
    test(result)
    logging.info('\n\n******************* TESTS FINISHED *******************\n')
    logging.info('Ran %d tests with %d errors and %d failures', result.testsRun,
                 len(result.errors), len(result.failures))
    for test_and_stack in result.failures:
      logging.info('\n\n[  TEST FAILED  ] %s\n', test_and_stack[0])
      logging.info(
          '%s', test_and_stack[1].replace('\\\\r',
                                          '\r').replace('\\\\n', '\n').replace(
                                              '\\r', '\r').replace('\\n', '\n'))

    return result


class TestResult(unittest.TestResult):

  def startTest(self, test):
    """Called when the given test is about to be run."""
    logging.info('\n\n===== BEGIN TEST CASE: %s =====\n', test)
    unittest.TestResult.startTest(self, test)

  def stopTest(self, test):
    """Called when the given test has been run."""
    unittest.TestResult.stopTest(self, test)
    logging.info('\n\n===== END TEST CASE: %s =====\n', test)

  def addSuccess(self, test):
    unittest.TestResult.addSuccess(self, test)

  def addError(self, test, err):
    unittest.TestResult.addError(self, test, err)
    self._LogFailureInfo(err)

  def addFailure(self, test, err):
    unittest.TestResult.addFailure(self, test, err)
    self._LogFailureInfo(err)

  def addSkip(self, test, reason):
    unittest.TestResult.addSkip(self, test, reason)

  def addSubTest(self, test, subtest, outcome):  # pylint: disable=invalid-name
    unittest.TestResult.addSubTest(self, test, subtest, outcome)
    self._HandleSubTest(outcome, **subtest.params)

  def _HandleSubTest(self, outcome, name='Missing Name', **kwargs):
    if outcome:
      self._LogFailureInfo(outcome)
    logging.info('===== END SUBTEST: %s %s =====', name, repr(kwargs))

  def _LogFailureInfo(self, err):
    exctype, exc, tb = err
    if type(exc) is SubTestFailure:
      # Subtest errors have already been logged, don't log them again.
      return
    detail = ''.join(traceback.format_exception(exctype, exc, tb))
    logging.error('FAILURE: %s', detail)
