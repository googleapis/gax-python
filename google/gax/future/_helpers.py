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

"""Private helpers for futures."""

import logging
import threading

from google import gax
from google.gax import retry


_LOGGER = logging.getLogger(__name__)


def from_any(pb_type, any_pb):
    """Converts an Any protobuf to the specified message type

    Args:
        pb_type (type): the type of the message that any_pb stores an instance
            of.
        any_pb (google.protobuf.any_pb2.Any): the object to be converted.

    Returns:
        pb_type: An instance of the pb_type message.

    Raises:
        TypeError: if the message could not be converted.
    """
    msg = pb_type()
    if not any_pb.Unpack(msg):
        raise TypeError(
            'Could not convert {} to {}'.format(
                any_pb.__class__.__name__, pb_type.__name__))

    return msg


def start_daemon_thread(*args, **kwargs):
    """Starts a thread and marks it as a daemon thread."""
    thread = threading.Thread(*args, **kwargs)
    thread.daemon = True
    thread.start()
    return thread


def safe_invoke_callback(callback, *args, **kwargs):
    """Invoke a callback, swallowing and logging any exceptions."""
    # pylint: disable=bare-except
    # We intentionally want to swallow all exceptions.
    try:
        callback(*args, **kwargs)
    except:
        _LOGGER.exception('Error while executing Future callback.')


def blocking_poll(poll_once_func, retry_codes, timeout=None):
    """A pattern for repeatedly polling a function.

    This pattern uses gax's retry and backoff functionality to continuously
    poll a function. The function can raises
    :class:`google.gax.errors.TimeoutError` to indicate that it should be
    polled again. This pattern will continue to call the function until the
    timeout expires or the function returns a value.

    Args:
        poll_once_func (Callable): The function to invoke.
        retry_codes (Sequence[str]): a list of Google API error codes that
            signal a retry should happen.
        timeout (int): The maximum number of seconds to poll.

    Returns:
        Any: The final result of invoking the function.
    """
    # If a timeout is set, then convert it to milliseconds.
    #
    # Also, we need to send 0 instead of None for the rpc arguments,
    # because an internal method (`_has_timeout_settings`) will
    # erroneously return False otherwise.
    rpc_timeout = None
    if timeout is not None:
        timeout *= 1000
        rpc_timeout = 0

    # Set the backoff settings. We have specific backoff settings
    # for "are we there yet" calls that are distinct from those configured
    # in the config.json files.
    backoff_settings = gax.BackoffSettings(
        initial_retry_delay_millis=1000,
        retry_delay_multiplier=2,
        max_retry_delay_millis=30000,
        initial_rpc_timeout_millis=rpc_timeout,
        rpc_timeout_multiplier=rpc_timeout,
        max_rpc_timeout_millis=rpc_timeout,
        total_timeout_millis=timeout,
    )

    # Set the retry to retry if poll_once_func raises the
    # a deadline exceeded error, according to the given backoff settings.
    retry_options = gax.RetryOptions(retry_codes, backoff_settings)
    retryable_poll = retry.retryable(poll_once_func, retry_options)

    # Start polling, and return the final result from the poll_once_func.
    return retryable_poll()
