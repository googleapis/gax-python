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

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name
"""Unit tests for gax package globals."""

from __future__ import absolute_import

import unittest2

from google.gax import (
    BundleOptions, CallOptions, CallSettings, INITIAL_PAGE, OPTION_INHERIT,
    RetryOptions)


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
        settings = CallSettings(timeout=9, page_descriptor=None, retry=None)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertIsNone(final.retry)
        self.assertIsNone(final.page_descriptor)

    def test_settings_merge_options2(self):
        retry = RetryOptions(None, None)
        options = CallOptions(retry=retry)
        settings = CallSettings(
            timeout=9, page_descriptor=None, retry=RetryOptions(None, None))
        final = settings.merge(options)
        self.assertEqual(final.timeout, 9)
        self.assertIsNone(final.page_descriptor)
        self.assertEqual(final.retry, retry)

    def test_settings_merge_options_page_streaming(self):
        retry = RetryOptions(None, None)
        page_descriptor = object()
        options = CallOptions(timeout=46, page_token=INITIAL_PAGE)
        settings = CallSettings(timeout=9, retry=retry,
                                page_descriptor=page_descriptor)
        final = settings.merge(options)
        self.assertEqual(final.timeout, 46)
        self.assertEqual(final.page_descriptor, page_descriptor)
        self.assertEqual(final.page_token, INITIAL_PAGE)
        self.assertFalse(final.flatten_pages)
        self.assertEqual(final.retry, retry)

    def test_settings_merge_none(self):
        settings = CallSettings(
            timeout=23, page_descriptor=object(), bundler=object(),
            retry=object())
        final = settings.merge(None)
        self.assertEqual(final.timeout, settings.timeout)
        self.assertEqual(final.retry, settings.retry)
        self.assertEqual(final.page_descriptor, settings.page_descriptor)
        self.assertEqual(final.bundler, settings.bundler)
        self.assertEqual(final.bundle_descriptor, settings.bundle_descriptor)
