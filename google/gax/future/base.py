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

"""Abstract and helper bases for Future implementations."""

import abc

import six

from google.gax.future import _helpers


@six.add_metaclass(abc.ABCMeta)
class Future(object):
    # pylint: disable=missing-docstring, invalid-name
    # We inherit the interfaces here from concurrent.futures.

    """Future interface.

    This interface is based on :class:`concurrent.futures.Future`.
    """

    @abc.abstractmethod
    def cancel(self):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def cancelled(self):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def running(self):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def done(self):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def result(self, timeout=None):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def exception(self, timeout=None):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def add_done_callback(self, fn):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def set_result(self, result):  # pragma: NO COVER
        raise NotImplementedError()

    @abc.abstractmethod
    def set_exception(self, exception):  # pragma: NO COVER
        raise NotImplementedError()


class PollingFuture(Future):
    """A Future that needs to poll some service to check its status.

    The private :meth:`_poll_once` method should be implemented by subclasses.

    Privacy here is intended to prevent the final class from overexposing, not
    to prevent subclasses from accessing methods.
    """
    def __init__(self):
        super(PollingFuture, self).__init__()
        self._result = None
        self._exception = None
        self._result_set = False
        """bool: Set to True when the result has been set via set_result or
        set_exception."""
        self._polling_thread = None
        self._done_callbacks = []

    @abc.abstractmethod
    def _poll_once(self, timeout):  # pragma: NO COVER
        """Poll for completion once.

        Subclasses must implement this. It should check if the task is complete
        and call :meth:`set_result` or :meth:`set_exception`. If the task
        isn't complete, this should raise a
        :class:`google.gax.errors.GaxError` with a code that can be retried.

        Args:
            timeout (float): unused.

        Raises:
            google.gax.errors.GaxError: if the operation should be retried.

        .. note: Due to the retry implementation, the exception raised here
            to indicate retry must also be a `grpc.RpcError`.
        """
        # pylint: disable=missing-raises-doc
        # pylint doesn't recognize this as abstract.
        raise NotImplementedError()

    @abc.abstractproperty
    def _poll_retry_codes(self):  # pragma: NO COVER
        """Sequence[str]: Which API status codes can be retried."""
        raise NotImplementedError()

    def _blocking_poll(self, timeout=None):
        """Poll and wait for the Future to be resolved.

        Args:
            timeout (int): How long to wait for the operation to complete.
                If None, wait indefinitely.
        """
        if self._result_set:
            return

        _helpers.blocking_poll(
            self._poll_once, self._poll_retry_codes, timeout=timeout)

    def result(self, timeout=None):
        """Get the result of the operation, blocking if necessary.

        Args:
            timeout (int): How long to wait for the operation to complete.
                If None, wait indefinitely.

        Returns:
            google.protobuf.Message: The Operation's result.

        Raises:
            google.gax.GaxError: If the operation errors or if the timeout is
                reached before the operation completes.
        """
        self._blocking_poll()

        if self._exception is not None:
            # pylint: disable=raising-bad-type
            # Pylint doesn't recognize that this is valid in this case.
            raise self._exception

        return self._result

    def exception(self, timeout=None):
        """Get the exception from the operation, blocking if necessary.

        Args:
            timeout (int): How long to wait for the operation to complete.
                If None, wait indefinitely.

        Returns:
            Optional[google.gax.GaxError]: The operation's error.
        """
        self._blocking_poll()
        return self._exception

    def add_done_callback(self, fn):
        """Add a callback to be executed when the operation is complete.

        If the operation is not already complete, this will start a helper
        thread to poll for the status of the operation in the background.

        Args:
            fn (Callable[Future]): The callback to execute when the operation
                is complete.
        """
        if self._result_set:
            _helpers.safe_invoke_callback(fn, self)
            return

        self._done_callbacks.append(fn)

        if self._polling_thread is None:
            # The polling thread will exit on its own as soon as the operation
            # is done.
            self._polling_thread = _helpers.start_daemon_thread(
                target=self._blocking_poll)

    def _invoke_callbacks(self, *args, **kwargs):
        """Invoke all done callbacks."""
        for callback in self._done_callbacks:
            _helpers.safe_invoke_callback(callback, *args, **kwargs)

    def set_result(self, result):
        """Set the Future's result."""
        self._result = result
        self._result_set = True
        self._invoke_callbacks(self)

    def set_exception(self, exception):
        """Set the Future's exception."""
        self._exception = exception
        self._result_set = True
        self._invoke_callbacks(self)
