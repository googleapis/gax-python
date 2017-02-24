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
# pylint: disable=protected-access
"""Unit tests for _grpc_google_auth."""

from __future__ import absolute_import

import sys

import google_auth_httplib2
import mock
from six.moves import reload_module
import unittest2

import google.auth.credentials
import google.auth.transport.requests
from google.gax import _grpc_google_auth


class TestGetDefaultCredentials(unittest2.TestCase):
    @mock.patch('google.auth.default', autospec=True)
    def test(self, default_mock):
        default_mock.return_value = (
            mock.sentinel.credentials, mock.sentinel.project)
        scopes = ['fake', 'scopes']

        got = _grpc_google_auth.get_default_credentials(scopes)

        self.assertEqual(got, mock.sentinel.credentials)
        default_mock.assert_called_once_with(scopes)


class TestSecureAuthorizedChannel(unittest2.TestCase):
    @mock.patch('google.gax._grpc_google_auth._request_factory', autospec=True)
    @mock.patch(
        'google.auth.transport.grpc.secure_authorized_channel', autospec=True)
    def test(self, secure_authorized_channel_mock, request_factory_mock):

        got_channel = _grpc_google_auth.secure_authorized_channel(
            mock.sentinel.credentials,
            mock.sentinel.target)

        request_factory_mock.assert_called_once_with()
        secure_authorized_channel_mock.assert_called_once_with(
            mock.sentinel.credentials,
            request_factory_mock.return_value,
            mock.sentinel.target,
            ssl_credentials=None)

        self.assertEqual(
            got_channel, secure_authorized_channel_mock.return_value)


class TestRequestsRequestFactory(unittest2.TestCase):
    def test(self):
        self.assertEqual(
            _grpc_google_auth._request_factory,
            google.auth.transport.requests.Request)


class TestHttplib2RequestFactory(unittest2.TestCase):
    def setUp(self):
        # Block requests module during this test.
        self._requests_module = sys.modules['google.auth.transport.requests']
        sys.modules['google.auth.transport.requests'] = None
        reload_module(_grpc_google_auth)

    def tearDown(self):
        sys.modules['google.auth.transport.requests'] = self._requests_module
        reload_module(_grpc_google_auth)

    def test(self):
        request = _grpc_google_auth._request_factory()
        self.assertIsInstance(request, google_auth_httplib2.Request)


class TestNoHttpClient(unittest2.TestCase):
    def setUp(self):
        # Block all transport modules during this test.
        self._requests_module = sys.modules['google.auth.transport.requests']
        self._httplib2_module = sys.modules['google_auth_httplib2']
        sys.modules['google.auth.transport.requests'] = None
        sys.modules['google_auth_httplib2'] = None

    def tearDown(self):
        sys.modules['google.auth.transport.requests'] = self._requests_module
        sys.modules['google_auth_httplib2'] = self._httplib2_module
        reload_module(_grpc_google_auth)

    def test(self):
        with self.assertRaises(ImportError):
            reload_module(_grpc_google_auth)
