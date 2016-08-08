# Copyright 2015, Google Inc.
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

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name,protected-access
"""Unit tests for api_callable"""

from __future__ import absolute_import, division
import mock
import unittest2

from google.gax import (
    api_callable, bundling, BackoffSettings, BundleDescriptor, BundleOptions,
    CallSettings, CallOptions, INITIAL_PAGE, PageDescriptor, RetryOptions)
from google.gax.errors import GaxError, RetryError


_SERVICE_NAME = 'test.interface.v1.api'


_A_CONFIG = {
    'interfaces': {
        _SERVICE_NAME: {
            'retry_codes': {
                'foo_retry': ['code_a', 'code_b'],
                'bar_retry': ['code_c']
            },
            'retry_params': {
                'default': {
                    'initial_retry_delay_millis': 100,
                    'retry_delay_multiplier': 1.2,
                    'max_retry_delay_millis': 1000,
                    'initial_rpc_timeout_millis': 300,
                    'rpc_timeout_multiplier': 1.3,
                    'max_rpc_timeout_millis': 3000,
                    'total_timeout_millis': 30000
                }
            },
            'methods': {
                # Note that GAX should normalize this to snake case
                'BundlingMethod': {
                    'retry_codes_name': 'foo_retry',
                    'retry_params_name': 'default',
                    'timeout_millis': 25000,
                    'bundling': {
                        'element_count_threshold': 6,
                        'element_count_limit': 10
                    }
                },
                'PageStreamingMethod': {
                    'retry_codes_name': 'bar_retry',
                    'retry_params_name': 'default',
                    'timeout_millis': 12000
                }
            }
        }
    }
}


_PAGE_DESCRIPTORS = {
    'page_streaming_method': PageDescriptor(
        'page_token', 'next_page_token', 'page_streams')
}


_BUNDLE_DESCRIPTORS = {'bundling_method': BundleDescriptor('bundled_field', [])}


_RETRY_DICT = {'code_a': Exception,
               'code_b': Exception,
               'code_c': Exception}


_FAKE_STATUS_CODE_1 = object()


_FAKE_STATUS_CODE_2 = object()


class CustomException(Exception):
    def __init__(self, msg, code):
        super(CustomException, self).__init__(msg)
        self.code = code


class AnotherException(Exception):
    pass


class TestCreateApiCallable(unittest2.TestCase):

    def test_call_api_call(self):
        settings = CallSettings()
        my_callable = api_callable.create_api_call(
            lambda _req, _timeout: 42, settings)
        self.assertEqual(my_callable(None), 42)

    def test_call_override(self):
        settings = CallSettings(timeout=10)
        my_callable = api_callable.create_api_call(
            lambda _req, timeout: timeout, settings)
        self.assertEqual(my_callable(None, CallOptions(timeout=20)), 20)

    def test_call_kwargs(self):
        settings = CallSettings(kwargs={'key': 'value'})
        my_callable = api_callable.create_api_call(
            lambda _req, _timeout, **kwargs: kwargs['key'], settings)
        self.assertEqual(my_callable(None), 'value')
        self.assertEqual(my_callable(None, CallOptions(key='updated')),
                         'updated')

    @mock.patch('time.time')
    @mock.patch('google.gax.config.exc_to_code')
    def test_retry(self, mock_exc_to_code, mock_time):
        mock_exc_to_code.side_effect = lambda e: e.code
        to_attempt = 3
        retry = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))

        # Succeeds on the to_attempt'th call, and never again afterward
        mock_call = mock.Mock()
        mock_call.side_effect = ([CustomException('', _FAKE_STATUS_CODE_1)] *
                                 (to_attempt - 1) + [mock.DEFAULT])
        mock_call.return_value = 1729
        mock_time.return_value = 0
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(mock_call, settings)
        self.assertEqual(my_callable(None), 1729)
        self.assertEqual(mock_call.call_count, to_attempt)

    @mock.patch('time.time')
    def test_no_retry_if_no_codes(self, mock_time):
        retry = RetryOptions([], BackoffSettings(1, 2, 3, 4, 5, 6, 7))

        mock_call = mock.Mock()
        mock_call.side_effect = CustomException('', _FAKE_STATUS_CODE_1)
        mock_time.return_value = 0

        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(mock_call, settings)
        self.assertRaises(CustomException, my_callable, None)
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch('time.time')
    @mock.patch('google.gax.config.exc_to_code')
    def test_retry_aborts_simple(self, mock_exc_to_code, mock_time):
        def fake_call(dummy_request, dummy_timeout):
            raise CustomException('', _FAKE_STATUS_CODE_1)

        retry = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))
        mock_time.side_effect = [0, 2]
        mock_exc_to_code.side_effect = lambda e: e.code
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(fake_call, settings)

        try:
            my_callable(None)
        except RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

    @mock.patch('time.time')
    @mock.patch('google.gax.config.exc_to_code')
    def test_retry_times_out_simple(self, mock_exc_to_code, mock_time):
        mock_exc_to_code.side_effect = lambda e: e.code
        to_attempt = 3
        retry = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))
        mock_call = mock.Mock()
        mock_call.side_effect = CustomException('', _FAKE_STATUS_CODE_1)
        mock_time.side_effect = ([0] * to_attempt + [2])
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(mock_call, settings)

        try:
            my_callable(None)
        except RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

        self.assertEqual(mock_call.call_count, to_attempt)

    @mock.patch('time.time')
    @mock.patch('google.gax.config.exc_to_code')
    def test_retry_aborts_on_unexpected_exception(
            self, mock_exc_to_code, mock_time):
        mock_exc_to_code.side_effect = lambda e: e.code
        retry = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 1))
        mock_call = mock.Mock()
        mock_call.side_effect = CustomException('', _FAKE_STATUS_CODE_2)
        mock_time.return_value = 0
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(mock_call, settings)
        self.assertRaises(Exception, my_callable, None)
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch('time.time')
    def test_retry_times_out_no_response(self, mock_time):
        mock_time.return_value = 1
        retry = RetryOptions(
            [_FAKE_STATUS_CODE_1],
            BackoffSettings(0, 0, 0, 0, 0, 0, 0))
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(lambda: None, settings)

        self.assertRaises(RetryError, my_callable, None)

    @mock.patch('time.sleep')
    @mock.patch('time.time')
    @mock.patch('google.gax.config.exc_to_code')
    def test_retry_exponential_backoff(self, mock_exc_to_code, mock_time,
                                       mock_sleep):
        # pylint: disable=too-many-locals
        mock_exc_to_code.side_effect = lambda e: e.code
        MILLIS_PER_SEC = 1000
        mock_time.return_value = 0

        def incr_time(secs):
            mock_time.return_value += secs

        def api_call(dummy_request, timeout, **dummy_kwargs):
            incr_time(timeout)
            raise CustomException(str(timeout), _FAKE_STATUS_CODE_1)

        mock_call = mock.Mock()
        mock_sleep.side_effect = incr_time
        mock_call.side_effect = api_call

        params = BackoffSettings(3, 2, 24, 5, 2, 80, 2500)
        retry = RetryOptions([_FAKE_STATUS_CODE_1], params)
        settings = CallSettings(timeout=0, retry=retry)
        my_callable = api_callable.create_api_call(mock_call, settings)

        try:
            my_callable(None)
        except RetryError as exc:
            self.assertIsInstance(exc.cause, CustomException)

        self.assertGreaterEqual(mock_time(),
                                params.total_timeout_millis / MILLIS_PER_SEC)

        # Very rough bounds
        calls_lower_bound = params.total_timeout_millis / (
            params.max_retry_delay_millis + params.max_rpc_timeout_millis)
        self.assertGreater(mock_call.call_count, calls_lower_bound)

        calls_upper_bound = (params.total_timeout_millis /
                             params.initial_retry_delay_millis)
        self.assertLess(mock_call.call_count, calls_upper_bound)

    def test_page_streaming(self):
        # A mock grpc function that page streams a list of consecutive
        # integers, returning `page_size` integers with each call and using
        # the next integer to return as the page token, until `pages_to_stream`
        # pages have been returned.
        # pylint:disable=too-many-locals
        page_size = 3
        pages_to_stream = 5

        # pylint: disable=abstract-method, too-few-public-methods
        class PageStreamingRequest(object):
            def __init__(self, page_token=0):
                self.page_token = page_token

        class PageStreamingResponse(object):
            def __init__(self, nums=(), next_page_token=0):
                self.nums = nums
                self.next_page_token = next_page_token

        fake_grpc_func_descriptor = PageDescriptor(
            'page_token', 'next_page_token', 'nums')

        def grpc_return_value(request, *dummy_args, **dummy_kwargs):
            start = int(request.page_token)
            if start > 0 and start < page_size * pages_to_stream:
                return PageStreamingResponse(
                    nums=list(range(start,
                                    start + page_size)),
                    next_page_token=start + page_size)
            elif start >= page_size * pages_to_stream:
                return PageStreamingResponse()
            else:
                return PageStreamingResponse(nums=list(range(page_size)),
                                             next_page_token=page_size)

        with mock.patch('grpc.UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = grpc_return_value
            settings = CallSettings(
                page_descriptor=fake_grpc_func_descriptor, timeout=0)
            my_callable = api_callable.create_api_call(
                mock_grpc, settings=settings)
            self.assertEqual(list(my_callable(PageStreamingRequest())),
                             list(range(page_size * pages_to_stream)))

            unflattened_option = CallOptions(page_token=INITIAL_PAGE)
            # Expect a list of pages_to_stream pages, each of size page_size,
            # plus one empty page
            expected = [list(range(page_size * n, page_size * (n + 1)))
                        for n in range(pages_to_stream)] + [()]
            self.assertEqual(list(my_callable(PageStreamingRequest(),
                                              unflattened_option)),
                             expected)

            pages_already_read = 2
            explicit_page_token_option = CallOptions(
                page_token=str(page_size * pages_already_read))
            # Expect a list of pages_to_stream pages, each of size page_size,
            # plus one empty page, minus the pages_already_read
            expected = [list(range(page_size * n, page_size * (n + 1)))
                        for n in range(pages_already_read, pages_to_stream)]
            expected += [()]
            self.assertEqual(list(my_callable(PageStreamingRequest(),
                                              explicit_page_token_option)),
                             expected)

    def test_bundling_page_streaming_error(self):
        settings = CallSettings(
            page_descriptor=object(), bundle_descriptor=object(),
            bundler=object())
        with self.assertRaises(ValueError):
            api_callable.create_api_call(lambda _req, _timeout: 42, settings)

    def test_bundling(self):
        # pylint: disable=abstract-method, too-few-public-methods
        class BundlingRequest(object):
            def __init__(self, elements=None):
                self.elements = elements

        fake_grpc_func_descriptor = BundleDescriptor('elements', [])
        bundler = bundling.Executor(BundleOptions(element_count_threshold=8))

        def my_func(request, dummy_timeout):
            return len(request.elements)

        settings = CallSettings(
            bundler=bundler, bundle_descriptor=fake_grpc_func_descriptor,
            timeout=0)
        my_callable = api_callable.create_api_call(my_func, settings)
        first = my_callable(BundlingRequest([0] * 3))
        self.assertIsInstance(first, bundling.Event)
        self.assertIsNone(first.result)  # pylint: disable=no-member
        second = my_callable(BundlingRequest([0] * 5))
        self.assertEqual(second.result, 8)  # pylint: disable=no-member

    def test_construct_settings(self):
        defaults = api_callable.construct_settings(
            _SERVICE_NAME, _A_CONFIG, dict(), _RETRY_DICT,
            bundle_descriptors=_BUNDLE_DESCRIPTORS,
            page_descriptors=_PAGE_DESCRIPTORS,
            kwargs={'key1': 'value1'})
        settings = defaults['bundling_method']
        self.assertAlmostEqual(settings.timeout, 25.0)
        self.assertIsInstance(settings.bundler, bundling.Executor)
        self.assertIsInstance(settings.bundle_descriptor, BundleDescriptor)
        self.assertIsNone(settings.page_descriptor)
        self.assertIsInstance(settings.retry, RetryOptions)
        self.assertEqual(settings.kwargs, {'key1': 'value1'})
        settings = defaults['page_streaming_method']
        self.assertAlmostEqual(settings.timeout, 12.0)
        self.assertIsNone(settings.bundler)
        self.assertIsNone(settings.bundle_descriptor)
        self.assertIsInstance(settings.page_descriptor, PageDescriptor)
        self.assertIsInstance(settings.retry, RetryOptions)
        self.assertEqual(settings.kwargs, {'key1': 'value1'})

    def test_construct_settings_override(self):
        _override = {
            'interfaces': {
                _SERVICE_NAME: {
                    'methods': {
                        'PageStreamingMethod': None,
                        'BundlingMethod': {
                            'timeout_millis': 8000,
                            'bundling': None
                        }
                    }
                }
            }
        }
        defaults = api_callable.construct_settings(
            _SERVICE_NAME, _A_CONFIG, _override, _RETRY_DICT,
            bundle_descriptors=_BUNDLE_DESCRIPTORS,
            page_descriptors=_PAGE_DESCRIPTORS)
        settings = defaults['bundling_method']
        self.assertAlmostEqual(settings.timeout, 8.0)
        self.assertIsNone(settings.bundler)
        self.assertIsNone(settings.page_descriptor)
        settings = defaults['page_streaming_method']
        self.assertAlmostEqual(settings.timeout, 12.0)
        self.assertIsInstance(settings.page_descriptor, PageDescriptor)
        self.assertIsNone(settings.retry)

    def test_construct_settings_override2(self):
        _override = {
            'interfaces': {
                _SERVICE_NAME: {
                    'retry_codes': {
                        'bar_retry': [],
                        'baz_retry': ['code_a']
                    },
                    'retry_params': {
                        'default': {
                            'initial_retry_delay_millis': 1000,
                            'retry_delay_multiplier': 1.2,
                            'max_retry_delay_millis': 10000,
                            'initial_rpc_timeout_millis': 3000,
                            'rpc_timeout_multiplier': 1.3,
                            'max_rpc_timeout_millis': 30000,
                            'total_timeout_millis': 300000
                        },
                    },
                    'methods': {
                        'BundlingMethod': {
                            'retry_params_name': 'default',
                            'retry_codes_name': 'baz_retry',
                        },
                    },
                }
            }
        }
        defaults = api_callable.construct_settings(
            _SERVICE_NAME, _A_CONFIG, _override, _RETRY_DICT,
            bundle_descriptors=_BUNDLE_DESCRIPTORS,
            page_descriptors=_PAGE_DESCRIPTORS)
        settings = defaults['bundling_method']
        backoff = settings.retry.backoff_settings
        self.assertEqual(backoff.initial_retry_delay_millis, 1000)
        self.assertEqual(settings.retry.retry_codes, [_RETRY_DICT['code_a']])
        self.assertIsInstance(settings.bundler, bundling.Executor)
        self.assertIsInstance(settings.bundle_descriptor, BundleDescriptor)

        # page_streaming_method is unaffected because it's not specified in
        # overrides. 'bar_retry' or 'default' definitions in overrides should
        # not affect the methods which are not in the overrides.
        settings = defaults['page_streaming_method']
        backoff = settings.retry.backoff_settings
        self.assertEqual(backoff.initial_retry_delay_millis, 100)
        self.assertEqual(backoff.retry_delay_multiplier, 1.2)
        self.assertEqual(backoff.max_retry_delay_millis, 1000)
        self.assertEqual(settings.retry.retry_codes, [_RETRY_DICT['code_c']])

    @mock.patch('google.gax.config.API_ERRORS', (CustomException, ))
    def test_catch_error(self):
        def abortion_error_func(*dummy_args, **dummy_kwargs):
            raise CustomException(None, None)

        def other_error_func(*dummy_args, **dummy_kwargs):
            raise AnotherException

        gax_error_callable = api_callable.create_api_call(
            abortion_error_func, CallSettings())
        self.assertRaises(GaxError, gax_error_callable, None)

        other_error_callable = api_callable.create_api_call(
            other_error_func, CallSettings())
        self.assertRaises(AnotherException, other_error_callable, None)
