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
"""Unit tests for grpc."""

from __future__ import absolute_import

import mock
import unittest2

from google.gax import grpc


def _fake_create_stub(channel):
    return channel


class TestCreateStub(unittest2.TestCase):
    FAKE_SERVICE_PATH = 'service_path'
    FAKE_PORT = 10101

    @mock.patch('google.gax._grpc_google_auth.get_default_credentials')
    @mock.patch('google.gax._grpc_google_auth.secure_authorized_channel')
    def test_creates_a_stub_with_default_credentials(
            self, secure_authorized_channel, get_default_credentials):
        fake_scopes = ['one', 'two']
        got_channel = grpc.create_stub(
            _fake_create_stub, service_path=self.FAKE_SERVICE_PATH,
            service_port=self.FAKE_PORT, scopes=fake_scopes)

        get_default_credentials.assert_called_once_with(fake_scopes)
        secure_authorized_channel.assert_called_once_with(
            get_default_credentials.return_value,
            '{}:{}'.format(self.FAKE_SERVICE_PATH, self.FAKE_PORT),
            ssl_credentials=None)

        self.assertEqual(got_channel, secure_authorized_channel.return_value)

    @mock.patch('google.gax._grpc_google_auth.get_default_credentials')
    @mock.patch('google.gax._grpc_google_auth.secure_authorized_channel')
    def test_creates_a_stub_with_explicit_credentials(
            self, secure_authorized_channel, get_default_credentials):
        credentials = mock.Mock()
        got_channel = grpc.create_stub(
            _fake_create_stub, service_path=self.FAKE_SERVICE_PATH,
            service_port=self.FAKE_PORT, credentials=credentials)

        self.assertFalse(get_default_credentials.called)
        secure_authorized_channel.assert_called_once_with(
            credentials,
            '{}:{}'.format(self.FAKE_SERVICE_PATH, self.FAKE_PORT),
            ssl_credentials=None)

        self.assertEqual(got_channel, secure_authorized_channel.return_value)

    @mock.patch('google.gax._grpc_google_auth.get_default_credentials')
    @mock.patch('google.gax._grpc_google_auth.secure_authorized_channel')
    def test_creates_a_stub_with_given_channel(
            self, secure_authorized_channel, get_default_credentials):
        fake_channel = mock.Mock()
        got_channel = grpc.create_stub(
            _fake_create_stub, channel=fake_channel)
        self.assertEqual(got_channel, fake_channel)
        self.assertFalse(secure_authorized_channel.called)
        self.assertFalse(get_default_credentials.called)


class TestErrors(unittest2.TestCase):
    class MyError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNKNOWN

    def test_exc_to_code(self):
        code = grpc.exc_to_code(TestErrors.MyError())
        self.assertEqual(code, grpc.StatusCode.UNKNOWN)
        self.assertEqual(code, grpc.STATUS_CODE_NAMES['UNKNOWN'])
        self.assertIsNone(grpc.exc_to_code(Exception))
        self.assertIsNone(grpc.exc_to_code(grpc.RpcError()))
