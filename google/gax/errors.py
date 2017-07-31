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

"""Provides GAX exceptions."""

from __future__ import absolute_import

from google.gax import config


class GaxError(Exception):
    """Common base class for exceptions raised by GAX.

    Attributes:
      msg (string): describes the error that occurred.
      cause (Exception, optional): the exception raised by a lower
        layer of the RPC stack (for example, gRPC) that caused this
        exception, or None if this exception originated in GAX.
    """
    def __init__(self, msg, cause=None):
        super(GaxError, self).__init__(msg)
        self.cause = cause

    def __str__(self):
        msg = super(GaxError, self).__str__()
        if not self.cause:
            return msg

        return '{}({}, caused by {})'.format(
            self.__class__.__name__, msg, self.cause)


class InvalidArgumentError(ValueError, GaxError):
    """GAX exception class for ``INVALID_ARGUMENT`` errors.

    Attributes:
      msg (string): describes the error that occurred.
      cause (Exception, optional): the exception raised by a lower
        layer of the RPC stack (for example, gRPC) that caused this
        exception, or None if this exception originated in GAX.
    """

    def __init__(self, msg, cause=None):
        GaxError.__init__(self, msg, cause=cause)


def create_error(msg, cause=None):
    """Creates a ``GaxError`` or subclass.

    Attributes:
        msg (string): describes the error that occurred.
        cause (Exception, optional): the exception raised by a lower
            layer of the RPC stack (for example, gRPC) that caused this
            exception, or None if this exception originated in GAX.

    Returns:
        .GaxError: The exception that wraps ``cause``.
    """
    status_code = config.exc_to_code(cause)
    status_name = config.NAME_STATUS_CODES.get(status_code)
    if status_name == 'INVALID_ARGUMENT':
        return InvalidArgumentError(msg, cause=cause)
    else:
        return GaxError(msg, cause=cause)


class RetryError(GaxError):
    """Indicates an error during automatic GAX retrying."""
    pass
