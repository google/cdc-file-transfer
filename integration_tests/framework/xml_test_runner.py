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

import logging
import os
import sys
import time
import traceback
import unittest

from xml.etree import ElementTree

XML_OUTPUT_FILE = 'XML_OUTPUT_FILE'
DEFAULT_SUBTEST = 'Test Case'


def _GetMillis():
  return int(time.time() * 1000)


def _Typename(cls):
  return '%s.%s' % (cls.__module__, cls.__name__)


def _AddProperties(element, props):
  if props:
    sub = ElementTree.SubElement(element, 'properties')
    for k, v in props.items():
      ElementTree.SubElement(sub, 'property', name=k, value=v)


class XmlTestRunner(object):
  """Runner producing test xml output."""

  def __init__(self, flags, filename=None):
    if not filename:
      if XML_OUTPUT_FILE in os.environ:
        filename = os.environ[XML_OUTPUT_FILE]
    self.filename = filename
    self.flags = flags

  def run(self, test):  # pylint: disable=invalid-name
    result = XmlTestResult(self.flags)
    logging.info('Running tests...')
    test(result)
    logging.info('\n\n******************* TESTS FINISHED *******************\n')
    logging.info('Ran %d tests with %d errors and %d failures', result.testsRun,
                 len(result.errors), len(result.failures))
    result.test_suite_element.set('tests', str(result.testsRun))
    result.test_suite_element.set(
        'failures', str(len(result.errors) + len(result.failures)))
    result.test_suite_element.set('time', str(_GetMillis() - result.start_time))
    if self.filename:
      with open(self.filename, 'wb') as f:
        ElementTree.ElementTree(element=result.root_element).write(f)
    else:
      for test_and_stack in result.failures:
        logging.info('\n\n[  TEST FAILED  ] %s\n', test_and_stack[0])
        logging.info(
            '%s', test_and_stack[1].replace('\\\\r', '\r').replace(
                '\\\\n', '\n').replace('\\r', '\r').replace('\\n', '\n'))

    return result


class XmlTestResult(unittest.TestResult):

  def __init__(self, flags):
    unittest.TestResult.__init__(self)
    self.start_time = _GetMillis()
    self.root_element = ElementTree.Element('testsuites')
    self.test_suite_element = ElementTree.SubElement(
        self.root_element, 'testsuite', name=os.path.basename(sys.argv[0]))
    self.test_case_element = None
    self.sub_test_case_element = None
    self.test_case_start_time = None
    self.flags = flags
    self.succeeded = False

  def startTest(self, test):
    """Called when the given test is about to be run."""
    logging.info('\n\n===== BEGIN TEST CASE: %s =====\n', test)
    self.succeeded = True
    self.flags.full_test_name = str(test)
    self.flags.test_suite = test.__class__.__qualname__
    self.flags.test_name = test._testMethodName
    self.test_case_start_time = _GetMillis()
    self.test_case_element = ElementTree.SubElement(
        self.test_suite_element,
        'testsuite',
        name=str(test),
        classname=_Typename(test.__class__))
    self.sub_test_case_element = ElementTree.SubElement(
        self.test_case_element,
        'testcase',
        name=str(test) + ' - ' + DEFAULT_SUBTEST,
        classname=_Typename(test.__class__),
        result='interrupted')
    unittest.TestResult.startTest(self, test)

  def stopTest(self, test):
    """Called when the given test has been run."""
    unittest.TestResult.stopTest(self, test)
    print(repr(self.skipped))
    if self.skipped:
      self.sub_test_case_element.set('result', 'skipped')
      self.sub_test_case_element.set('status', 'notrun')
    else:
      self.sub_test_case_element.set('result', 'completed')
      self.sub_test_case_element.set('status', 'run')
    self.sub_test_case_element.set(
        'time', str(_GetMillis() - self.test_case_start_time))
    logging.info('\n\n===== END TEST CASE: %s =====\n', test)

  def addSuccess(self, test):
    unittest.TestResult.addSuccess(self, test)

  def addError(self, test, err):
    unittest.TestResult.addError(self, test, err)
    self._SetFailureInfo(err)

  def addFailure(self, test, err):
    unittest.TestResult.addFailure(self, test, err)
    self._SetFailureInfo(err)

  def addSkip(self, test, reason):
    unittest.TestResult.addSkip(self, test, reason)
    self._SetSkipReason(reason)

  def addSubTest(self, test, subtest, outcome):  # pylint: disable=invalid-name
    unittest.TestResult.addSubTest(self, test, subtest, outcome)
    self._HandleSubTest(
        str(test), _Typename(test.__class__), outcome, **subtest.params)

  def _HandleSubTest(self,
                     full_test_name,
                     class_name,
                     outcome,
                     name='Missing Name',
                     tt_test_case_uuid=None,
                     **kwargs):
    sub_test_element = ElementTree.SubElement(
        self.test_case_element,
        'testcase',
        classname=class_name,
        name=full_test_name + ' - ' + name,
        status='run',
        result='completed')
    if outcome:
      self._SetFailureInfo(outcome, sub_test_element=sub_test_element)
    if tt_test_case_uuid:
      _AddProperties(sub_test_element, {'tt_test_case_uuid': tt_test_case_uuid})
    logging.info('===== END SUBTEST: %s %s =====', name, repr(kwargs))

  def _SetFailureInfo(self, err, sub_test_element=None):
    self.succeeded = False
    if sub_test_element is None:
      sub_test_element = self.sub_test_case_element
    exctype, exc, tb = err
    detail = ''.join(traceback.format_exception(exctype, exc, tb))
    logging.error('FAILURE: %s', detail)
    ElementTree.SubElement(
        sub_test_element, 'failure', message=str(exc),
        type=_Typename(exctype)).text = detail

  def _SetSkipReason(self, reason):
    ElementTree.SubElement(
        self.sub_test_case_element, 'skipped', message=reason)
