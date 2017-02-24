# Copyright 2016, Google Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name,protected-access,too-many-public-methods
"""Unit tests for gax package globals."""

from __future__ import absolute_import

import logging
import multiprocessing as mp

import mock
import unittest2

from google.gax import (
    _CallSettings, _LOG, _OperationFuture, BundleOptions, CallOptions,
    INITIAL_PAGE, OPTION_INHERIT, RetryOptions)
from google.gax.errors import GaxError, RetryError
from google.longrunning import operations_pb2
from google.rpc import code_pb2, status_pb2

from tests.fixtures.fixture_pb2 import Simple


class TestBundleOptions(unittest2.TestCase):

    def test_cannot_construct_with_noarg_options(self):
        self.assertRaises(AssertionError,
                          BundleOptions)

    def test_cannot_construct_with_bad_options(self):
        not_an_int = 'i am a string'
        self.assertRaises(AssertionError,
                          BundleOptions,
                          element_count_threshold=not_an_int)
        self.assertRaises(AssertionError,
                          BundleOptions,
                          request_byte_threshold=not_an_int)
        self.assertRaises(AssertionError,
                          BundleOptions,
                          delay_threshold=not_an_int)


class TestCallSettings(unittest2.TestCase):

    def test_call_options_simple(self):
        options = CallOptions(timeout=23)
        self.assertEqual(options.timeout, 23)
        self.assertEqual(options.retry, OPTION_INHERIT)
        self.assertEqual(options.page_token, OPTION_INHERIT)

    def test_cannot_construct_bad_options(self):
        self.assertRaises(
            ValueError, CallOptions, timeout=47, retry=RetryOptions(None, None))

    def test_settings_merge_options1(self):
        options = CallOptions(timeout=46)
        settings = _CallSettings(timeout=9, page_descriptor=None, retry=None)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertIsNone(final.retry)
        self.assertIsNone(final.page_descriptor)

    def test_settings_merge_options2(self):
        retry = RetryOptions(None, None)
        options = CallOptions(retry=retry)
        settings = _CallSettings(
            timeout=9, page_descriptor=None, retry=RetryOptions(None, None))
        final = settings.merge(options)
        self.assertEqual(final.timeout, 9)
        self.assertIsNone(final.page_descriptor)
        self.assertEqual(final.retry, retry)

    def test_settings_merge_options_page_streaming(self):
        retry = RetryOptions(None, None)
        page_descriptor = object()
        options = CallOptions(timeout=46, page_token=INITIAL_PAGE)
        settings = _CallSettings(timeout=9, retry=retry,
                                 page_descriptor=page_descriptor)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertEqual(final.page_descriptor, page_descriptor)
        self.assertEqual(final.page_token, INITIAL_PAGE)
        self.assertFalse(final.flatten_pages)
        self.assertEqual(final.retry, retry)

    def test_settings_merge_none(self):
        settings = _CallSettings(
            timeout=23, page_descriptor=object(), bundler=object(),
            retry=object())
        final = settings.merge(None)
        self.assertEqual(final.timeout, settings.timeout)
        self.assertEqual(final.retry, settings.retry)
        self.assertEqual(final.page_descriptor, settings.page_descriptor)
        self.assertEqual(final.bundler, settings.bundler)
        self.assertEqual(final.bundle_descriptor, settings.bundle_descriptor)


def _task1(operation_future):
    operation_future.test_queue.put(operation_future.result().field1)


def _task2(operation_future):
    operation_future.test_queue.put(operation_future.result().field2)


class _FakeOperationsClient(object):
    def __init__(self, operations):
        self.operations = list(reversed(operations))

    def get_operation(self, *_):
        return self.operations.pop()

    def cancel_operation(self, *_):
        pass


class _FakeLoggingHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.queue = mp.Queue()
        super(_FakeLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        self.acquire()
        try:
            self.queue.put(record.getMessage())
        finally:
            self.release()

    def reset(self):
        self.acquire()
        try:
            self.queue = mp.Queue()
        finally:
            self.release()


class TestOperationFuture(unittest2.TestCase):

    OPERATION_NAME = 'operations/projects/foo/instances/bar/operations/123'

    @classmethod
    def setUpClass(cls):
        cls._log_handler = _FakeLoggingHandler(level='DEBUG')
        _LOG.addHandler(cls._log_handler)

    def setUp(self):
        self._log_handler.reset()

    def _make_operation(self, metadata=None, response=None, error=None,
                        **kwargs):
        operation = operations_pb2.Operation(name=self.OPERATION_NAME, **kwargs)

        if metadata is not None:
            operation.metadata.Pack(metadata)

        if response is not None:
            operation.response.Pack(response)

        if error is not None:
            operation.error.CopyFrom(error)

        return operation

    def _make_operation_future(self, *operations):
        if not operations:
            operations = [self._make_operation()]

        fake_client = _FakeOperationsClient(operations)
        return _OperationFuture(operations[0], fake_client, Simple, Simple)

    def test_cancel_issues_call_when_not_done(self):
        operation = self._make_operation()

        fake_client = _FakeOperationsClient([operation])
        fake_client.cancel_operation = mock.Mock()

        operation_future = _OperationFuture(
            operation, fake_client, Simple, Simple)

        self.assertTrue(operation_future.cancel())
        fake_client.cancel_operation.assert_called_with(self.OPERATION_NAME)

    def test_cancel_does_nothing_when_already_done(self):
        operation = self._make_operation(done=True)

        fake_client = _FakeOperationsClient([operation])
        fake_client.cancel_operation = mock.Mock()

        operation_future = _OperationFuture(
            operation, fake_client, Simple, Simple)

        self.assertFalse(operation_future.cancel())
        fake_client.cancel_operation.assert_not_called()

    def test_cancelled_true(self):
        error = status_pb2.Status(code=code_pb2.CANCELLED)
        operation = self._make_operation(error=error)
        operation_future = self._make_operation_future(operation)

        self.assertTrue(operation_future.cancelled())

    def test_cancelled_false(self):
        operation = self._make_operation(error=status_pb2.Status())
        operation_future = self._make_operation_future(operation)
        self.assertFalse(operation_future.cancelled())

    def test_done_true(self):
        operation = self._make_operation(done=True)
        operation_future = self._make_operation_future(operation)
        self.assertTrue(operation_future.done())

    def test_done_false(self):
        operation_future = self._make_operation_future()
        self.assertFalse(operation_future.done())

    def test_operation_name(self):
        operation_future = self._make_operation_future()
        self.assertEqual(self.OPERATION_NAME, operation_future.operation_name())

    def test_metadata(self):
        metadata = Simple()
        operation = self._make_operation(metadata=metadata)
        operation_future = self._make_operation_future(operation)

        self.assertEqual(metadata, operation_future.metadata())

    def test_metadata_none(self):
        operation_future = self._make_operation_future()
        self.assertIsNone(operation_future.metadata())

    def test_last_operation_data(self):
        operation = self._make_operation()
        operation_future = self._make_operation_future(operation)
        self.assertEqual(operation, operation_future.last_operation_data())

    def test_result_response(self):
        response = Simple()
        operation = self._make_operation(done=True, response=response)
        operation_future = self._make_operation_future(operation)

        self.assertEqual(response, operation_future.result())

    def test_result_error(self):
        operation = self._make_operation(done=True, error=status_pb2.Status())
        operation_future = self._make_operation_future(operation)
        self.assertRaises(GaxError, operation_future.result)

    def test_result_timeout(self):
        operation_future = self._make_operation_future()
        self.assertRaises(RetryError, operation_future.result, 0)

    def test_exception_error(self):
        error = status_pb2.Status()
        operation = self._make_operation(done=True, error=error)
        operation_future = self._make_operation_future(operation)

        self.assertEqual(error, operation_future.exception())

    def test_exception_response(self):
        operation = self._make_operation(done=True, response=Simple())
        operation_future = self._make_operation_future(operation)
        self.assertIsNone(operation_future.exception())

    def test_exception_timeout(self):
        operation_future = self._make_operation_future()
        self.assertRaises(RetryError, operation_future.exception, 0)

    def test_add_done_callback(self):
        response = Simple(field1='foo', field2='bar')
        operation_future = self._make_operation_future(
            self._make_operation(),
            self._make_operation(done=True, response=response))
        operation_future.test_queue = mp.Queue()

        operation_future.add_done_callback(_task1)
        operation_future.add_done_callback(_task2)

        self.assertEqual('foo', operation_future.test_queue.get())
        self.assertEqual('bar', operation_future.test_queue.get())

    def test_add_done_callback_when_already_done(self):
        response = Simple(field1='foo', field2='bar')
        operation_future = self._make_operation_future(
            self._make_operation(done=True, response=response))
        operation_future.test_queue = mp.Queue()

        operation_future.add_done_callback(_task1)

        self.assertEqual('foo', operation_future.test_queue.get())

    def test_add_done_callback_when_exception(self):
        def _raising_task(_):
            raise Exception('Test message')

        operation_future = self._make_operation_future(
            self._make_operation(),
            self._make_operation(done=True, response=Simple()))
        operation_future.add_done_callback(_raising_task)
        self.assertEqual('Test message', self._log_handler.queue.get())
