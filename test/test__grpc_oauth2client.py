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
"""Unit tests for _grpc_oauth2client."""

from __future__ import absolute_import

import mock
import unittest2

from google.gax import _grpc_oauth2client


class TestAuthMetadataPlugin(unittest2.TestCase):
    TEST_TOKEN = 'an_auth_token'

    def test(self):
        credentials = mock.Mock()
        credentials.get_access_token.return_value = mock.Mock(
            access_token=self.TEST_TOKEN)

        metadata_plugin = _grpc_oauth2client.AuthMetadataPlugin(credentials)

        self.assertFalse(credentials.create_scoped.called)

        callback = mock.Mock()
        metadata_plugin(None, callback)

        callback.assert_called_once_with([
            ('authorization', 'Bearer {}'.format(self.TEST_TOKEN))], None)


class TestGetDefaultCredentials(unittest2.TestCase):
    TEST_TOKEN = 'an_auth_token'

    @mock.patch('oauth2client.client.GoogleCredentials.get_application_default')
    def test(self, factory):
        creds = mock.Mock()
        creds.get_access_token.return_value = mock.Mock(
            access_token=self.TEST_TOKEN)
        factory_mock_config = {'create_scoped.return_value': creds}
        factory.return_value = mock.Mock(**factory_mock_config)
        fake_scopes = ['fake', 'scopes']

        got = _grpc_oauth2client.get_default_credentials(fake_scopes)

        factory.return_value.create_scoped.assert_called_once_with(fake_scopes)
        self.assertEqual(got, creds)


class TestSecureAuthorizedChannel(unittest2.TestCase):
    FAKE_TARGET = 'service_path:10101'

    @mock.patch('grpc.composite_channel_credentials')
    @mock.patch('grpc.ssl_channel_credentials')
    @mock.patch('grpc.secure_channel')
    @mock.patch('google.gax._grpc_oauth2client.AuthMetadataPlugin')
    def test(
            self, auth_metadata_plugin, secure_channel, ssl_channel_credentials,
            composite_channel_credentials):
        credentials = mock.Mock()

        got_channel = _grpc_oauth2client.secure_authorized_channel(
            credentials, self.FAKE_TARGET)

        ssl_channel_credentials.assert_called_once_with()
        secure_channel.assert_called_once_with(
            self.FAKE_TARGET, composite_channel_credentials.return_value)
        auth_metadata_plugin.assert_called_once_with(credentials)
        self.assertEqual(got_channel, secure_channel.return_value)
