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

from google.gax import (api_callable, bundling, PageDescriptor,
                        BundleDescriptor, BundleOptions)
from grpc.framework.interfaces.face import face

_DUMMY_ERROR = face.AbortionError(None, None, None, None)


class TestApiCallable(unittest2.TestCase):

    def test_call_api_callable(self):
        settings = api_callable.CallSettings()
        my_callable = api_callable.ApiCallable(
            lambda _req, _timeout: 42, settings)
        self.assertEqual(my_callable(None), 42)

    def test_retry(self):
        to_attempt = 3
        # Succeeds on the to_attempt'th call, and never again afterward
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = ([_DUMMY_ERROR] * (to_attempt - 1) +
                                     [mock.DEFAULT])
            mock_grpc.return_value = 1729
            settings = api_callable.CallSettings(
                timeout=0, is_retrying=True, max_attempts=to_attempt)
            my_callable = api_callable.ApiCallable(mock_grpc, settings)
            self.assertEqual(my_callable(None), 1729)
            self.assertEqual(mock_grpc.call_count, to_attempt)

    def test_retry_aborts(self):
        to_attempt = 3
        with mock.patch('grpc.framework.crust.implementations.'
                        '_UnaryUnaryMultiCallable') as mock_grpc:
            mock_grpc.side_effect = _DUMMY_ERROR
            settings = api_callable.CallSettings(
                timeout=0, is_retrying=True, max_attempts=to_attempt)
            my_callable = api_callable.ApiCallable(mock_grpc, settings)
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

        fake_grpc_func_descriptor = PageDescriptor(
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
            settings = api_callable.CallSettings(
                page_descriptor=fake_grpc_func_descriptor, timeout=0)
            my_callable = api_callable.ApiCallable(mock_grpc, settings=settings)
            self.assertEqual(list(my_callable(PageStreamingRequest())),
                             list(range(page_size * pages_to_stream)))

    def test_bundling_page_streaming_error(self):
        settings = api_callable.CallSettings(
            page_descriptor=object(), bundle_descriptor=object(),
            bundler=object())
        my_callable = api_callable.ApiCallable(
            lambda _req, _timeout: 42, settings)
        with self.assertRaises(ValueError):
            my_callable(None)

    def test_bundling(self):
        # pylint: disable=abstract-method, too-few-public-methods
        class BundlingRequest(object):
            def __init__(self, messages=None):
                self.messages = messages

        fake_grpc_func_descriptor = BundleDescriptor('messages', [])
        bundler = bundling.Executor(BundleOptions(message_count_threshold=8))

        def my_func(request, dummy_timeout):
            return len(request.messages)

        settings = api_callable.CallSettings(
            bundler=bundler, bundle_descriptor=fake_grpc_func_descriptor,
            timeout=0)
        my_callable = api_callable.ApiCallable(my_func, settings)
        first = my_callable(BundlingRequest([0] * 3))
        self.assertIsInstance(first, bundling.Event)
        self.assertIsNone(first.result)  # pylint: disable=no-member
        second = my_callable(BundlingRequest([0] * 5))
        self.assertEquals(second.result, 8)  # pylint: disable=no-member

    def test_call_options_simple(self):
        options = api_callable.CallOptions(timeout=23, is_retrying=True)
        self.assertEqual(options.timeout, 23)
        self.assertTrue(options.is_retrying)
        self.assertEqual(options.is_page_streaming, api_callable.OPTION_INHERIT)
        self.assertEqual(options.max_attempts, api_callable.OPTION_INHERIT)

    def test_settings_merge_options1(self):
        options = api_callable.CallOptions(timeout=46, is_retrying=True)
        settings = api_callable.CallSettings(
            timeout=9, page_descriptor=None, max_attempts=16)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertTrue(final.is_retrying)
        self.assertIsNone(final.page_descriptor)
        self.assertEqual(final.max_attempts, 16)

    def test_settings_merge_options2(self):
        options = api_callable.CallOptions(max_attempts=1)
        settings = api_callable.CallSettings(
            timeout=9, page_descriptor=None, max_attempts=16)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 9)
        self.assertFalse(final.is_retrying)
        self.assertIsNone(final.page_descriptor)
        self.assertEqual(final.max_attempts, 1)

    def test_settings_merge_options_page_streaming(self):
        options = api_callable.CallOptions(timeout=46, is_page_streaming=False)
        settings = api_callable.CallSettings(timeout=9, max_attempts=16)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertFalse(final.is_retrying)
        self.assertIsNone(final.page_descriptor)
        self.assertEqual(final.max_attempts, 16)

    def test_settings_merge_none(self):
        settings = api_callable.CallSettings(
            timeout=23, page_descriptor=object(), bundler=object())
        final = settings.merge(None)
        self.assertEqual(final.timeout, settings.timeout)
        self.assertEqual(final.is_retrying, settings.is_retrying)
        self.assertEqual(final.max_attempts, settings.max_attempts)
        self.assertEqual(final.page_descriptor, settings.page_descriptor)
        self.assertEqual(final.bundler, settings.bundler)
        self.assertEqual(final.bundle_descriptor, settings.bundle_descriptor)
