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

"""Provides function wrappers that implement page streaming and retrying."""

from __future__ import absolute_import
from . import config


OPTION_INHERIT = object()
"""Global constant.

If a CallOptions field is set to OPTION_INHERIT, the call
to which that CallOptions belong will attempt to inherit that field from the
its default settings."""


def _add_timeout_arg(a_func, timeout):
    """Updates a_func so that it gets called with the timeout as its final arg.

    This converts a callable, a_func, into another callable with an additional
    positional arg.

    Args:
      a_func (callable): a callable to be updated
      timeout (int): to be added the original callable as it final positional
        args.


    Returns:
      callable: the original callable updated to the timeout arg
    """

    def inner(*args, **kw):
        """Updates args with the timeout."""
        updated_args = args + (timeout,)
        return a_func(*updated_args, **kw)

    return inner


def _retryable(a_func, max_attempts):
    """Creates a function equivalent to a_func, but that retries on certain
    exceptions.

    Args:
        a_func (callable): A callable.
        max_attempts (int): The maximum number of times that the call should
            be attempted; the call will always be attempted at least once.

    Returns:
        A function that will retry on exception.
    """

    def inner(*args, **kwargs):
        "Retries a_func upto max_attempt times"
        attempt_count = 0
        while 1:
            try:
                return a_func(*args, **kwargs)
            except config.RETRY_EXCEPTIONS:
                attempt_count += 1
                if attempt_count < max_attempts:
                    continue
                raise

    return inner


def _page_streamable(a_func,
                     request_page_token_field,
                     response_page_token_field,
                     resource_field,
                     timeout):
    """Creates a function that yields an iterable to performs page-streaming.

    Args:
        a_func: an API call that is page streaming.
        request_page_token_field: The field of the page token in the request.
        response_page_token_field: The field of the next page token in the
            response.
        resource_field: The field to be streamed.
        timeout: the timeout to apply to the API call.

    Returns:
        A function that returns an iterable over the specified field.
    """
    with_timeout = _add_timeout_arg(a_func, timeout)

    def inner(request):
        """A generator that yields all the paged responses."""
        while True:
            response = with_timeout(request)
            for obj in getattr(response, resource_field):
                yield obj
            next_page_token = getattr(response, response_page_token_field)
            if not next_page_token:
                break
            setattr(request, request_page_token_field, next_page_token)

    return inner


class ApiCallDefaults(object):
    """Encapsulates the default settings for all ApiCallables in an API"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=30, is_idempotent_retrying=True,
                 max_attempts=16):
        """Constructor.

        Args:
            timeout: The client-side timeout for API calls.
            is_idempotent_retrying: If set, calls determined by configuration
                to be idempotent will retry upon transient error by default.
            max_attempts: The maximum number of attempts that should be made
                for a retrying call to this service.

        Returns:
            An ApiCallDefaults object.
        """
        self.timeout = timeout
        self.is_idempotent_retrying = is_idempotent_retrying
        self.max_attempts = max_attempts


class CallOptions(object):
    """Encapsulates the default settings for a particular API call"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=OPTION_INHERIT, is_retrying=OPTION_INHERIT,
                 max_attempts=OPTION_INHERIT, page_streaming=OPTION_INHERIT):
        """Constructor.

        Args:
            timeout: The client-side timeout for API calls.
            is_retrying: If set, call will retry upon transient error by
                default.
            max_attempts: The maximum number of attempts that should be made
                for a retrying call to this service.
            page_streaming: page_descriptor.PageDescriptor indicating the
                structure of page streaming to be performed. If set to None
                no page streaming is performed.

        Returns:
            A CallOptions object.
        """
        self.timeout = timeout
        self.is_retrying = is_retrying
        self.max_attempts = max_attempts
        self.page_streaming = page_streaming

    def update(self, options):
        """Merges another CallOptions object into this one.

        Args:
            options: A CallOptions object whose values are override those in
                this object. If None, `update` has no effect.
        """
        if not options:
            return
        if options.timeout != OPTION_INHERIT:
            self.timeout = options.timeout
        if options.is_retrying != OPTION_INHERIT:
            self.is_retrying = options.is_retrying
        if options.max_attempts != OPTION_INHERIT:
            self.max_attempts = options.max_attempts
        if options.page_streaming != OPTION_INHERIT:
            self.page_streaming = options.page_streaming

    def normalize(self):
        """Transforms fields set to OPTION_INHERIT to None."""
        if self.timeout == OPTION_INHERIT:
            self.timeout = None
        if self.is_retrying == OPTION_INHERIT:
            self.is_retrying = None
        if self.max_attempts == OPTION_INHERIT:
            self.max_attempts = None
        if self.page_streaming == OPTION_INHERIT:
            self.page_streaming = None


def idempotent_callable(func, timeout=None, is_retrying=None,
                        page_streaming=None, max_attempts=None, defaults=None):
    """Creates an ApiCallable for an idempotent call.

    Args:
        func: The API call that this ApiCallable wraps.
        timeout: The timeout parameter to the API call. If not supplied, will
            default to the value in the defaults parameter.
        is_retrying: Boolean indicating whether this call should retry upon a
            transient error. If None, retrying will be determined by the
            defaults parameter.
        page_streaming: page_descriptor.PageDescriptor indicating the structure
            of page streaming to be performed. If None, this call will not
            perform page streaming.
        max_attempts: If is_retrying, the maximum number of times this call may
            be attempted. If not specified, will default to the value in the
            defaults parameter.
        defaults: A ApiCallDefaults object, from which default values will
            be drawn if not supplied by the other named parameters. The other
            named parameters always override those in the defaults. If neither
            the is_retrying nor defaults parameter is specified, a runtime
            error will result at callable creation time.

    Returns:
        An ApiCallable object.
    """
    if is_retrying is None:
        to_retry = defaults.is_idempotent_retrying
    else:
        to_retry = is_retrying
    return ApiCallable(
        func, timeout=timeout, page_streaming=page_streaming,
        max_attempts=max_attempts, defaults=defaults, is_retrying=to_retry)


class ApiCallable(object):
    """Represents zero or more API calls, with options to retry or perform
    page streaming.

    Calling an object of ApiCallable type causes these calls to be transmitted.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, func, timeout=None, is_retrying=False,
                 page_streaming=None, max_attempts=None, defaults=None):
        """Constructor.

        Args:
            func: The API call that this ApiCallable wraps.
            timeout: The timeout parameter to the API call. If not supplied,
                will default to the value in the defaults parameter.
            is_retrying: Boolean indicating whether this call should retry upon
                a transient error.
            page_streaming: page_descriptor.PageDescriptor indicating the
                structure of page streaming to be performed. If None, this call
                will not perform page streaming.
            max_attempts: If is_retrying, the maximum number of times this call
                may be attempted. If not specified, will default to the value
                in the defaults parameter.
            defaults: A ApiCallDefaults object, from which default values
                will be drawn if not supplied by the other named parameters.
                The other named parameters always override those in the
                defaults. If neither the defaults nor timeout parameter is
                specified, a runtime error will result at call time. If neither
                the defaults nor the max_attempts parameter is specified for a
                retrying call, a runtime error will result at call time.

        Returns:
            An ApiCallable object.
        """
        self.func = func
        self.is_retrying = is_retrying
        self.page_descriptor = page_streaming
        self.max_attempts = max_attempts
        self.timeout = timeout
        if defaults is not None:
            if max_attempts is None:
                self.max_attempts = defaults.max_attempts
            if timeout is None:
                self.timeout = defaults.timeout

    def __call__(self, *args, **kwargs):
        the_func = self.func

        # Update the_func using each of the applicable function decorators
        # before calling.
        if self.is_retrying:
            the_func = _retryable(the_func, self.max_attempts)
        if self.page_descriptor:
            the_func = _page_streamable(
                the_func,
                self.page_descriptor.request_page_token_field,
                self.page_descriptor.response_page_token_field,
                self.page_descriptor.resource_field,
                self.timeout)
        else:
            the_func = _add_timeout_arg(the_func, self.timeout)
        return the_func(*args, **kwargs)
