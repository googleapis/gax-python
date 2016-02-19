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

from __future__ import absolute_import
import mock
import unittest2

from google.gax import api_callable, PageDescriptor
from grpc.framework.interfaces.face import face

_DUMMY_ERROR = face.AbortionError(None, None, None, None)


class TestApiCallable(unittest2.TestCase):

    def test_call_api_callable(self):
        my_callable = api_callable.ApiCallable(lambda _req, _timeout: 42)
        self.assertEqual(my_callable(None), 42)

    def test_retry(self):
        to_attempt = 3
        # Succeeds on the to_attempt'th call, and never again afterward
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = ([_DUMMY_ERROR] * (to_attempt - 1) +
                                     [mock.DEFAULT])
            mock_grpc.return_value = 1729
            options = api_callable.CallOptions(
                timeout=0, is_retrying=True, max_attempts=to_attempt)
            my_callable = api_callable.ApiCallable(mock_grpc, options=options)
            self.assertEqual(my_callable(None), 1729)
            self.assertEqual(mock_grpc.call_count, to_attempt)

    def test_retry_aborts(self):
        to_attempt = 3
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = _DUMMY_ERROR
            options = api_callable.CallOptions(
                timeout=0, is_retrying=True, max_attempts=to_attempt)
            my_callable = api_callable.ApiCallable(mock_grpc, options=options)
            self.assertRaises(face.AbortionError, my_callable, None)
            self.assertEqual(mock_grpc.call_count, to_attempt)

    def test_page_streaming(self):
        # A mock grpc function that page streams a list of consecutive
        # integers, returning `page_size` integers with each call and using
        # the next integer to return as the page token, until `pages_to_stream`
        # pages have been returned.
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

        mock_grpc_func_descriptor = PageDescriptor(
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
            options = api_callable.CallOptions(
                page_streaming=mock_grpc_func_descriptor, timeout=0)
            my_callable = api_callable.ApiCallable(mock_grpc, options=options)
            self.assertEqual(list(my_callable(PageStreamingRequest())),
                             list(range(page_size * pages_to_stream)))

    def test_defaults_override_apicallable_defaults(self):
        defaults = api_callable.ApiCallDefaults(timeout=10, max_attempts=6)
        my_callable = api_callable.ApiCallable(None, defaults=defaults)
        _, max_attempts, _, timeout = my_callable._call_settings()
        self.assertEqual(timeout, 10)
        self.assertEqual(max_attempts, 6)

    def test_constructor_values_override_defaults(self):
        defaults = api_callable.ApiCallDefaults(timeout=10, max_attempts=6)
        options = api_callable.CallOptions(timeout=100, max_attempts=60)
        my_callable = api_callable.ApiCallable(
            None, options=options, defaults=defaults)
        _, max_attempts, _, timeout = my_callable._call_settings()
        self.assertEqual(timeout, 100)
        self.assertEqual(max_attempts, 60)

    def test_idempotent_default_retry(self):
        defaults = api_callable.ApiCallDefaults(
            is_idempotent_retrying=True)
        my_callable = api_callable.ApiCallable(
            None, defaults=defaults, is_idempotent=True)
        is_retrying, _, _, _ = my_callable._call_settings()
        self.assertTrue(is_retrying)

    def test_idempotent_default_override(self):
        defaults = api_callable.ApiCallDefaults(
            is_idempotent_retrying=False)
        options = api_callable.CallOptions(is_retrying=True)
        my_callable = api_callable.ApiCallable(
            None, options=options, defaults=defaults, is_idempotent=True)
        is_retrying, _, _, _ = my_callable._call_settings()
        self.assertTrue(is_retrying)

    def test_call_options_simple(self):
        options = api_callable.CallOptions(timeout=23, is_retrying=True)
        self.assertEqual(options.timeout, 23)
        self.assertTrue(options.is_retrying)
        self.assertEqual(options.page_streaming, api_callable.OPTION_INHERIT)
        self.assertEqual(options.max_attempts, api_callable.OPTION_INHERIT)

    def test_call_options_update(self):
        first = api_callable.CallOptions(timeout=46, is_retrying=True)
        second = api_callable.CallOptions(
            timeout=9, page_streaming=False, max_attempts=16)
        first.update(second)
        self.assertEqual(first.timeout, 9)
        self.assertTrue(first.is_retrying)
        self.assertFalse(first.page_streaming)
        self.assertEqual(first.max_attempts, 16)

    def test_call_options_update_none(self):
        options = api_callable.CallOptions(timeout=23, page_streaming=False)
        options.update(None)
        self.assertEqual(options.timeout, 23)
        self.assertEqual(options.is_retrying, api_callable.OPTION_INHERIT)
        self.assertFalse(options.page_streaming)
        self.assertEqual(options.max_attempts, api_callable.OPTION_INHERIT)

    def test_call_options_normalize(self):
        options = api_callable.CallOptions(timeout=23, is_retrying=True)
        options.normalize()
        self.assertEqual(options.timeout, 23)
        self.assertTrue(options.is_retrying)
        self.assertIsNone(options.page_streaming)
        self.assertIsNone(options.max_attempts)

    def test_call_options_normalize_default(self):
        options = api_callable.CallOptions()
        options.normalize()
        self.assertIsNone(options.timeout)
        self.assertIsNone(options.is_retrying)
        self.assertIsNone(options.page_streaming)
        self.assertIsNone(options.max_attempts)
