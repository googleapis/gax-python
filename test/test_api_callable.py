# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name
"""Unit tests for api_callable"""

from __future__ import absolute_import
import mock
import unittest2

from google.gax import api_callable, page_descriptor
from google.protobuf import message
from grpc.framework.interfaces.face import face


class TestApiCallable(unittest2.TestCase):

    def test_call_api_callable(self):
        mock_grpc_func = lambda request, timeout: 42
        my_callable = api_callable.ApiCallable(mock_grpc_func, timeout=0)
        self.assertEqual(my_callable(None), 42)

    def test_retry(self):
        to_attempt = 3
        # Succeeds on the to_attempt'th call, and never again afterward
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = (
                [face.AbortionError(None, None, None, None)] * (to_attempt - 1)
                + [mock.DEFAULT])
            mock_grpc.return_value = 1729
            my_callable = api_callable.ApiCallable(
                mock_grpc, timeout=0, is_retrying=True,
                max_attempts=to_attempt)
            self.assertEqual(my_callable(None), 1729)
            self.assertEqual(mock_grpc.call_count, to_attempt)

    def test_retry_aborts(self):
        to_attempt = 3
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = face.AbortionError(None, None, None, None)
            my_callable = api_callable.ApiCallable(
                mock_grpc, timeout=0, is_retrying=True,
                max_attempts=to_attempt)
            self.assertRaises(face.AbortionError, my_callable, None)
            self.assertEqual(mock_grpc.call_count, to_attempt)

    def test_page_streaming(self):
        # A mock grpc function that page streams a list of consecutive
        # integers, returning `page_size` integers with each call and using
        # the next integer to return as the page token, until `pages_to_stream`
        # pages have been returned.
        page_size = 3
        pages_to_stream = 5

        # pylint: disable=abstract-method
        class PageStreamingRequest(message.Message):
            def __init__(self, page_token=0):
                self.page_token = page_token

        class PageStreamingResponse(message.Message):
            def __init__(self, nums=(), next_page_token=0):
                self.nums = nums
                self.next_page_token = next_page_token

        mock_grpc_func_descriptor = page_descriptor.PageDescriptor(
            'page_token', 'next_page_token', 'nums')

        def grpc_return_value(request, *dummy_args, **dummy_kwargs):
            if (request.page_token > 0 and
                    request.page_token < page_size * pages_to_stream):
                return PageStreamingResponse(
                    nums=iter(range(request.page_token,
                                    request.page_token + page_size)),
                    next_page_token=request.page_token + page_size)
            elif request.page_token >= page_size * pages_to_stream:
                return PageStreamingResponse()
            else:
                return PageStreamingResponse(nums=iter(range(page_size)),
                                             next_page_token=page_size)

        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = grpc_return_value
            my_callable = api_callable.ApiCallable(
                mock_grpc, timeout=0, page_streaming=mock_grpc_func_descriptor)
            self.assertEqual(list(my_callable(PageStreamingRequest())),
                             list(range(page_size * pages_to_stream)))

    def test_defaults_override_apicallable_defaults(self):
        defaults = api_callable.ApiCallableDefaults(timeout=10, max_attempts=6)
        callable1 = api_callable.ApiCallable(None, defaults=defaults)
        self.assertEqual(callable1.timeout, 10)
        self.assertEqual(callable1.max_attempts, 6)

    def test_constructor_values_override_defaults(self):
        defaults = api_callable.ApiCallableDefaults(timeout=10, max_attempts=6)
        callable2 = api_callable.ApiCallable(
            None, timeout=100, max_attempts=60, defaults=defaults)
        self.assertEqual(callable2.timeout, 100)
        self.assertEqual(callable2.max_attempts, 60)

    def test_idempotent_default_retry(self):
        defaults = api_callable.ApiCallableDefaults(
            is_idempotent_retrying=True)
        my_callable = api_callable.idempotent_callable(None, defaults=defaults)
        self.assertTrue(my_callable.is_retrying)

    def test_idempotent_default_override(self):
        defaults = api_callable.ApiCallableDefaults(
            is_idempotent_retrying=False)
        my_callable = api_callable.idempotent_callable(
            None, is_retrying=True, defaults=defaults)
        self.assertTrue(my_callable.is_retrying)
