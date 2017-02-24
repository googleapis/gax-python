# Copyright 2017, Google Inc.
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

# pylint: disable=missing-docstring,invalid-name
"""Unit tests for retry"""

from __future__ import absolute_import, division

import mock
import unittest2

from google.gax import BackoffSettings, errors, retry, RetryOptions

_MILLIS_PER_SEC = 1000


_FAKE_STATUS_CODE_1 = object()


_FAKE_STATUS_CODE_2 = object()


class CustomException(Exception):
    def __init__(self, msg, code):
        super(CustomException, self).__init__(msg)
        self.code = code


class TestRetry(unittest2.TestCase):

    @mock.patch('google.gax.config.exc_to_code')
    @mock.patch('time.time')
    def test_retryable_without_timeout(self, mock_time, mock_exc_to_code):
        mock_time.return_value = 0
        mock_exc_to_code.side_effect = lambda e: e.code

        to_attempt = 3
        mock_call = mock.Mock()
        mock_call.side_effect = ([CustomException('', _FAKE_STATUS_CODE_1)] *
                                 (to_attempt - 1) + [mock.DEFAULT])
        mock_call.return_value = 1729

        retry_options = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, None, None, None, None))

        my_callable = retry.retryable(mock_call, retry_options)

        self.assertEqual(my_callable(None), 1729)
        self.assertEqual(to_attempt, mock_call.call_count)

    @mock.patch('google.gax.config.exc_to_code')
    @mock.patch('time.time')
    def test_retryable_with_timeout(self, mock_time, mock_exc_to_code):
        mock_time.return_value = 1
        mock_exc_to_code.side_effect = lambda e: e.code

        mock_call = mock.Mock()
        mock_call.side_effect = [CustomException('', _FAKE_STATUS_CODE_1),
                                 mock.DEFAULT]
        mock_call.return_value = 1729

        retry_options = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 0))

        my_callable = retry.retryable(mock_call, retry_options)

        self.assertRaises(errors.RetryError, my_callable)
        self.assertEqual(0, mock_call.call_count)

    @mock.patch('google.gax.config.exc_to_code')
    @mock.patch('time.time')
    def test_retryable_when_no_codes(self, mock_time, mock_exc_to_code):
        mock_time.return_value = 0
        mock_exc_to_code.side_effect = lambda e: e.code

        mock_call = mock.Mock()
        mock_call.side_effect = [CustomException('', _FAKE_STATUS_CODE_1),
                                 mock.DEFAULT]
        mock_call.return_value = 1729

        retry_options = RetryOptions(
            [],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))

        my_callable = retry.retryable(mock_call, retry_options)

        try:
            my_callable(None)
            self.fail('Should not have been reached')
        except errors.RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

        self.assertEqual(1, mock_call.call_count)

    @mock.patch('google.gax.config.exc_to_code')
    @mock.patch('time.time')
    def test_retryable_aborts_on_unexpected_exception(
            self, mock_time, mock_exc_to_code):
        mock_time.return_value = 0
        mock_exc_to_code.side_effect = lambda e: e.code

        mock_call = mock.Mock()
        mock_call.side_effect = [CustomException('', _FAKE_STATUS_CODE_2),
                                 mock.DEFAULT]
        mock_call.return_value = 1729

        retry_options = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))

        my_callable = retry.retryable(mock_call, retry_options)

        try:
            my_callable(None)
            self.fail('Should not have been reached')
        except errors.RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

        self.assertEqual(1, mock_call.call_count)

    @mock.patch('google.gax.config.exc_to_code')
    @mock.patch('time.sleep')
    @mock.patch('time.time')
    def test_retryable_exponential_backoff(
            self, mock_time, mock_sleep, mock_exc_to_code):
        def incr_time(secs):
            mock_time.return_value += secs

        def api_call(timeout):
            incr_time(timeout)
            raise CustomException(str(timeout), _FAKE_STATUS_CODE_1)

        mock_time.return_value = 0
        mock_sleep.side_effect = incr_time
        mock_exc_to_code.side_effect = lambda e: e.code

        mock_call = mock.Mock()
        mock_call.side_effect = api_call

        params = BackoffSettings(3, 2, 24, 5, 2, 80, 2500)
        retry_options = RetryOptions([_FAKE_STATUS_CODE_1], params)

        my_callable = retry.retryable(mock_call, retry_options)

        try:
            my_callable()
            self.fail('Should not have been reached')
        except errors.RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

        self.assertGreaterEqual(mock_time(),
                                params.total_timeout_millis / _MILLIS_PER_SEC)

        # Very rough bounds
        calls_lower_bound = params.total_timeout_millis / (
            params.max_retry_delay_millis + params.max_rpc_timeout_millis)
        self.assertGreater(mock_call.call_count, calls_lower_bound)

        calls_upper_bound = (params.total_timeout_millis /
                             params.initial_retry_delay_millis)
        self.assertLess(mock_call.call_count, calls_upper_bound)
