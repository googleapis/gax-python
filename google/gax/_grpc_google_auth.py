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

"""Provides gRPC authentication support using google.auth."""

from __future__ import absolute_import

import google.auth
import google.auth.credentials
import google.auth.transport.grpc

try:
    import google.auth.transport.requests
    # pylint: disable=invalid-name
    # Pylint recognizes this as a class, but we're aliasing it as a factory
    # function.
    _request_factory = google.auth.transport.requests.Request
    # pylint: enable=invalid-name
except ImportError:
    try:
        import httplib2
        import google_auth_httplib2

        def _request_factory():
            http = httplib2.Http()
            return google_auth_httplib2.Request(http)

    except ImportError:
        raise ImportError(
            'No HTTP transport is available. Please install requests or '
            'httplib2 and google_auth_httplib2.')


def get_default_credentials(scopes):
    """Gets the Application Default Credentials."""
    credentials, _ = google.auth.default(scopes=scopes)
    return credentials


def secure_authorized_channel(
        credentials, target, ssl_credentials=None):
    """Creates a secure authorized gRPC channel."""
    http_request = _request_factory()
    return google.auth.transport.grpc.secure_authorized_channel(
        credentials, http_request, target,
        ssl_credentials=ssl_credentials)
