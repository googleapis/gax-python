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

from grpc import RpcError, StatusCode

from google.gax import _grpc_google_auth


API_ERRORS = (RpcError, )
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


NAME_STATUS_CODES = dict([(v, k) for (k, v) in STATUS_CODE_NAMES.items()])
"""Inverse map for STATUS_CODE_NAMES"""


def exc_to_code(exc):
    """Retrieves the status code from an exception"""
    if not isinstance(exc, RpcError):
        return None
    else:
        try:
            return exc.code()
        except AttributeError:
            return None


def create_stub(generated_create_stub, channel=None, service_path=None,
                service_port=None, credentials=None, scopes=None,
                ssl_credentials=None):
    """Creates a gRPC client stub.

    Args:
        generated_create_stub (Callable): The generated gRPC method to create a
            stub.
        channel (grpc.Channel): A Channel object through which to make calls.
            If None, a secure channel is constructed. If specified, all
            remaining arguments are ignored.
        service_path (str): The domain name of the API remote host.
        service_port (int): The port on which to connect to the remote host.
        credentials (google.auth.credentials.Credentials): The authorization
            credentials to attach to requests. These credentials identify your
            application to the service.
        scopes (Sequence[str]): The OAuth scopes for this service. This
            parameter is ignored if a credentials is specified.
        ssl_credentials (grpc.ChannelCredentials): gRPC channel credentials
            used to create a secure gRPC channel. If not specified, SSL
            credentials will be created using default certificates.

    Returns:
        grpc.Client: A gRPC client stub.
    """
    if channel is None:
        target = '{}:{}'.format(service_path, service_port)

        if credentials is None:
            credentials = _grpc_google_auth.get_default_credentials(scopes)

        channel = _grpc_google_auth.secure_authorized_channel(
            credentials, target, ssl_credentials=ssl_credentials)

    return generated_create_stub(channel)
