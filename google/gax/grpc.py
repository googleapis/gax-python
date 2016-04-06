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

"""Adapts the grpc surface."""

from __future__ import absolute_import
from grpc.beta import implementations
from grpc.beta.interfaces import StatusCode
from grpc.framework.interfaces.face import face
from . import auth


API_ERRORS = (face.AbortionError, )
"""gRPC exceptions that indicate that an RPC was aborted."""


STATUS_CODE_NAMES = {
    'ABORTED': StatusCode.ABORTED,
    'CANCELLED': StatusCode.CANCELLED,
    'DATA_LOSS': StatusCode.DATA_LOSS,
    'DEADLINE_EXCEEDED': StatusCode.DEADLINE_EXCEEDED,
    'FAILED_PRECONDITION': StatusCode.FAILED_PRECONDITION,
    'INTERNAL': StatusCode.INTERNAL,
    'INVALID_ARGUMENT': StatusCode.INVALID_ARGUMENT,
    'NOT_FOUND': StatusCode.NOT_FOUND,
    'OUT_OF_RANGE': StatusCode.OUT_OF_RANGE,
    'PERMISSION_DENIED': StatusCode.PERMISSION_DENIED,
    'RESOURCE_EXHAUSTED': StatusCode.RESOURCE_EXHAUSTED,
    'UNAUTHENTICATED': StatusCode.UNAUTHENTICATED,
    'UNAVAILABLE': StatusCode.UNAVAILABLE,
    'UNIMPLEMENTED': StatusCode.UNIMPLEMENTED,
    'UNKNOWN': StatusCode.UNKNOWN}
"""Maps strings used in client config to gRPC status codes."""


def exc_to_code(exc):
    """Retrieves the status code from an exception"""
    if not isinstance(exc, face.AbortionError):
        return None
    elif isinstance(exc, face.ExpirationError):
        return StatusCode.DEADLINE_EXCEEDED
    else:
        return getattr(exc, 'code', None)


def _make_grpc_auth_func(auth_func):
    """Creates the auth func expected by the grpc callback."""

    def grpc_auth(dummy_context, callback):
        """The auth signature required by grpc."""
        callback(auth_func(), None)

    return grpc_auth


def _make_channel_creds(auth_func, ssl_creds):
    """Converts the auth func into the composite creds expected by grpc."""
    grpc_auth_func = _make_grpc_auth_func(auth_func)
    call_creds = implementations.metadata_call_credentials(grpc_auth_func)
    return implementations.composite_channel_credentials(ssl_creds, call_creds)


def create_stub(generated_create_stub, service_path, port, ssl_creds=None,
                channel=None, metadata_transformer=None, scopes=None):
    """Creates a gRPC client stub.

    Args:
        generated_create_stub: The generated gRPC method to create a stub.
        service_path: The domain name of the API remote host.
        port: The port on which to connect to the remote host.
        ssl_creds: A ClientCredentials object for use with an SSL-enabled
            Channel. If none, credentials are pulled from a default location.
        channel: A Channel object through which to make calls. If none, a secure
            channel is constructed.
        metadata_transformer: A function that transforms the metadata for
            requests, e.g., to give OAuth credentials.
        scopes: The OAuth scopes for this service. This parameter is ignored if
            a custom metadata_transformer is supplied.

    Returns:
        A gRPC client stub.
    """
    if channel is None:
        if ssl_creds is None:
            ssl_creds = implementations.ssl_channel_credentials(
                None, None, None)
        if metadata_transformer is None:
            if scopes is None:
                scopes = []
            metadata_transformer = auth.make_auth_func(scopes)

        channel_creds = _make_channel_creds(metadata_transformer, ssl_creds)
        channel = implementations.secure_channel(
            service_path, port, channel_creds)

    return generated_create_stub(channel)
