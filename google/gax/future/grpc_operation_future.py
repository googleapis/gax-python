# Copyright 2017, Google Inc.
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

"""a gRPC-based future for long-running operations."""

import concurrent.futures
import threading

import grpc

from google.gax import errors
from google.gax.future import _helpers
from google.gax.future import base
from google.rpc import code_pb2


class _RetryPoll(
        errors.GaxError, grpc.RpcError, concurrent.futures.TimeoutError):
    def __init__(self):
        super(_RetryPoll, self).__init__('Operation not complete')

    def code(self):  # pylint: disable=no-self-use
        """RPC status code, inherited from :class:`grpc.RpcError`."""
        return grpc.StatusCode.DEADLINE_EXCEEDED


class OperationFuture(base.PollingFuture):
    """A Future representing a long-running server-side operation."""

    def __init__(
            self, operation, client, result_type, metadata_type=None,
            call_options=None):
        """
        Args:
            operation (google.longrunning.Operation): the initial long-running
                operation object.
            client
                (google.gapic.longrunning.operations_client.OperationsClient):
                a client for the long-running operation service.
            result_type (type): the class type of the result.
            metadata_type (type): the class type of the metadata.
            call_options (google.gax.CallOptions): the call options
                that are used when check the operation status.
        """
        super(OperationFuture, self).__init__()
        self._operation = operation
        self._client = client
        self._result_type = result_type
        self._metadata_type = metadata_type
        self._call_options = call_options
        self._completion_lock = threading.Lock()

    @property
    def operation(self):
        """google.longrunning.Operation: The current long-running operation
        message."""
        return self._operation

    @property
    def metadata(self):
        """google.protobuf.Message: the current operation metadata."""
        if not self._operation.HasField('metadata'):
            return None

        return _helpers.from_any(self._metadata_type, self._operation.metadata)

    def cancel(self):
        """Attempt to cancel the operation.

        Returns:
            bool: True if the cancel RPC was made, False if the operation is
                already complete.
        """
        if self.done():
            return False

        self._client.cancel_operation(self._operation.name)
        return True

    def cancelled(self):
        """True if the operation was cancelled."""
        operation = self._call_get_operation_rpc()
        return (operation.HasField('error') and
                operation.error.code == code_pb2.CANCELLED)

    def running(self):
        """True if the operation is currently running."""
        return not self.done()

    def _call_get_operation_rpc(self):
        """Call the GetOperation RPC method."""
        # If the currently cached operation is done, no need to make another
        # RPC as it will not change once done.
        if not self._operation.done:
            self._operation = self._client.get_operation(
                self._operation.name, self._call_options)
            self._set_result_from_operation()

        return self._operation

    def _set_result_from_operation(self):
        """Set the result or exception from the current Operation message,
        if it is complete."""
        # This must be done in a lock to prevent the polling thread
        # and main thread from both executing the completion logic
        # at the same time.
        with self._completion_lock:
            # If the operation isn't complete or if the result has already been
            # set, do not call set_result/set_exception again.
            # Note: self._result_set is set to True in set_result and
            # set_exception, in case those methods are invoked directly.
            if not self._operation.done or self._result_set:
                return

            if self._operation.HasField('response'):
                response = _helpers.from_any(
                    self._result_type, self._operation.response)
                self.set_result(response)
            elif self._operation.HasField('error'):
                exception = errors.GaxError(self._operation.error.message)
                self.set_exception(exception)
            else:
                exception = errors.GaxError('Unknown operation error')
                self.set_exception(exception)

    def done(self):
        """Checks to see if the operation is complete.

        This will make a blocking RPC to refresh the status of the operation.

        Returns:
            bool: True if the operation is complete, False otherwise.
        """
        operation = self._call_get_operation_rpc()
        return operation.done

    def _poll_once(self, timeout):
        """Checks the status of the operation once.

        This implements the abstract method :meth:`PollingFuture._poll_once`.

        Uses :meth:`done` to refresh the status of the operation. If it's not
        done, it will raise a :class:`OperationTimeoutError`. This fits
        the interface needed by the :func:`_helpers.blocking_poll` helper. The
        helper will continue executing this method with exponential backoff.

        This method exits cleanly with no return value once the operation is
        complete.

        Raises:
            _RetryPoll: if the operation is not done.
        """
        if not self.done():
            raise _RetryPoll()

    @property
    def _poll_retry_codes(self):
        return [grpc.StatusCode.DEADLINE_EXCEEDED]
