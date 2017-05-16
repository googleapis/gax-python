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

"""Provides function wrappers that implement retrying."""

from __future__ import absolute_import, division

import random
import time

from google.gax import config, errors

_MILLIS_PER_SECOND = 1000


def _has_timeout_settings(backoff_settings):
    return (backoff_settings.rpc_timeout_multiplier is not None and
            backoff_settings.max_rpc_timeout_millis is not None and
            backoff_settings.total_timeout_millis is not None and
            backoff_settings.initial_rpc_timeout_millis is not None)


def add_timeout_arg(a_func, timeout, **kwargs):
    """Updates a_func so that it gets called with the timeout as its final arg.

    This converts a callable, a_func, into another callable with an additional
    positional arg.

    Args:
      a_func (callable): a callable to be updated
      timeout (int): to be added to the original callable as it final positional
        arg.
      kwargs: Addtional arguments passed through to the callable.

    Returns:
      callable: the original callable updated to the timeout arg
    """

    def inner(*args):
        """Updates args with the timeout."""
        updated_args = args + (timeout,)
        return a_func(*updated_args, **kwargs)

    return inner


def retryable(a_func, retry_options, **kwargs):
    """Creates a function equivalent to a_func, but that retries on certain
    exceptions.

    Args:
      a_func (callable): A callable.
      retry_options (RetryOptions): Configures the exceptions upon which the
        callable should retry, and the parameters to the exponential backoff
        retry algorithm.
      kwargs: Addtional arguments passed through to the callable.

    Returns:
        Callable: A function that will retry on exception.
    """
    delay_mult = retry_options.backoff_settings.retry_delay_multiplier
    max_delay_millis = retry_options.backoff_settings.max_retry_delay_millis
    has_timeout_settings = _has_timeout_settings(retry_options.backoff_settings)

    if has_timeout_settings:
        timeout_mult = retry_options.backoff_settings.rpc_timeout_multiplier
        max_timeout = (retry_options.backoff_settings.max_rpc_timeout_millis /
                       _MILLIS_PER_SECOND)
        total_timeout = (retry_options.backoff_settings.total_timeout_millis /
                         _MILLIS_PER_SECOND)

    def inner(*args):
        """Equivalent to ``a_func``, but retries upon transient failure.

        Retrying is done through an exponential backoff algorithm configured
        by the options in ``retry``.
        """
        delay = retry_options.backoff_settings.initial_retry_delay_millis
        exc = errors.RetryError('Retry total timeout exceeded before any'
                                'response was received')
        if has_timeout_settings:
            timeout = (
                retry_options.backoff_settings.initial_rpc_timeout_millis /
                _MILLIS_PER_SECOND)

            now = time.time()
            deadline = now + total_timeout
        else:
            timeout = None
            deadline = None

        while deadline is None or now < deadline:
            try:
                to_call = add_timeout_arg(a_func, timeout, **kwargs)
                return to_call(*args)
            except Exception as exception:  # pylint: disable=broad-except
                code = config.exc_to_code(exception)
                if code not in retry_options.retry_codes:
                    raise errors.RetryError(
                        'Exception occurred in retry method that was not'
                        ' classified as transient', exception)

                exc = errors.RetryError(
                    'Retry total timeout exceeded with exception', exception)

                # Sleep a random number which will, on average, equal the
                # expected delay.
                to_sleep = random.uniform(0, delay * 2)
                time.sleep(to_sleep / _MILLIS_PER_SECOND)
                delay = min(delay * delay_mult, max_delay_millis)

                if has_timeout_settings:
                    now = time.time()
                    timeout = min(
                        timeout * timeout_mult, max_timeout, deadline - now)

        raise exc

    return inner
