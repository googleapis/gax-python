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

"""Runtime configuration shared by gax modules."""

from __future__ import absolute_import

from google.gax import grpc


exc_to_code = grpc.exc_to_code  # pylint: disable=invalid-name
"""A function that takes an exception and returns a status code.

May return None if the exception is not associated with a status code.
"""


STATUS_CODE_NAMES = grpc.STATUS_CODE_NAMES
"""Maps strings used in client config to the status codes they represent.

This is necessary for google.gax.api_callable.construct_settings to translate
the client constants configuration for retrying into the correct gRPC objects.
"""


NAME_STATUS_CODES = grpc.NAME_STATUS_CODES
"""Inverse map for STATUS_CODE_NAMES"""


create_stub = grpc.create_stub  # pylint: disable=invalid-name,
"""The function to use to create stubs."""


API_ERRORS = grpc.API_ERRORS
"""Errors that indicate that an RPC was aborted."""
