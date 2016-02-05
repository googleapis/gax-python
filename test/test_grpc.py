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


def _fake_create_stub(channel, metadata_transformer=None):
    return channel, metadata_transformer


class TestCreateStub(unittest2.TestCase):
    FAKE_SERVICE_PATH = 'service_path'
    FAKE_PORT = 10101

    @mock.patch('grpc.beta.implementations.ssl_client_credentials')
    @mock.patch('grpc.beta.implementations.secure_channel')
    @mock.patch('google.gax.auth.make_auth_func')
    def test_creates_a_stub_ok_with_no_scopes(self, auth, chan, client_creds):
        got_channel, got_func = grpc.create_stub(
            _fake_create_stub, self.FAKE_SERVICE_PATH, self.FAKE_PORT)
        client_creds.assert_called_once_with(None, None, None)
        chan.assert_called_once_with(self.FAKE_SERVICE_PATH, self.FAKE_PORT,
                                     client_creds.return_value)
        auth.assert_called_once_with([])
        self.assertEquals(got_channel, chan.return_value)
        self.assertEquals(got_func, auth.return_value)

    @mock.patch('grpc.beta.implementations.ssl_client_credentials')
    @mock.patch('grpc.beta.implementations.secure_channel')
    @mock.patch('google.gax.auth.make_auth_func')
    def test_creates_a_stub_ok_with_scopes(self, auth, chan, client_creds):
        fake_scopes = ['dummy', 'scopes']
        grpc.create_stub(
            _fake_create_stub, self.FAKE_SERVICE_PATH, self.FAKE_PORT,
            scopes=fake_scopes)
        client_creds.assert_called_once_with(None, None, None)
        chan.assert_called_once_with(self.FAKE_SERVICE_PATH, self.FAKE_PORT,
                                     client_creds.return_value)
        auth.assert_called_once_with(fake_scopes)

    @mock.patch('grpc.beta.implementations.ssl_client_credentials')
    @mock.patch('grpc.beta.implementations.secure_channel')
    @mock.patch('google.gax.auth.make_auth_func')
    def test_creates_a_stub_with_given_channel(self, auth, chan, client_creds):
        fake_channel = object()
        got_channel, _ = grpc.create_stub(
            _fake_create_stub, self.FAKE_SERVICE_PATH, self.FAKE_PORT,
            channel=fake_channel)
        auth.assert_called_once_with([])
        self.assertEquals(got_channel, fake_channel)
        self.assertFalse(client_creds.called)
        self.assertFalse(chan.called)

    @mock.patch('grpc.beta.implementations.ssl_client_credentials')
    @mock.patch('grpc.beta.implementations.secure_channel')
    @mock.patch('google.gax.auth.make_auth_func')
    def test_creates_a_stub_ok_with_given_creds(self, dummy_auth, chan,
                                                client_creds):
        fake_creds = object()
        grpc.create_stub(
            _fake_create_stub, self.FAKE_SERVICE_PATH, self.FAKE_PORT,
            ssl_creds=fake_creds)
        chan.assert_called_once_with(self.FAKE_SERVICE_PATH, self.FAKE_PORT,
                                     fake_creds)
        self.assertFalse(client_creds.called)

    @mock.patch('grpc.beta.implementations.ssl_client_credentials')
    @mock.patch('grpc.beta.implementations.secure_channel')
    @mock.patch('google.gax.auth.make_auth_func')
    def test_creates_a_stub_ok_with_given_auth_func(self, auth, dummy_chan,
                                                    dummy_client_creds):
        grpc.create_stub(
            _fake_create_stub, self.FAKE_SERVICE_PATH, self.FAKE_PORT,
            metadata_transformer=lambda x: tuple())
        self.assertFalse(auth.called)
