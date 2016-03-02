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

from __future__ import absolute_import, division

from . import bundling


def _add_timeout_arg(a_func, timeout):
    """Updates a_func so that it gets called with the timeout as its final arg.

    This converts a callable, a_func, into another callable with an additional
    positional arg.

    Args:
      a_func (callable): a callable to be updated
      timeout (int): to be added to the original callable as it final positional
        arg.


    Returns:
      callable: the original callable updated to the timeout arg
    """

    def inner(*args, **kw):
        """Updates args with the timeout."""
        updated_args = args + (timeout,)
        return a_func(*updated_args, **kw)

    return inner


def _retryable(a_func, retry):
    """Creates a function equivalent to a_func, but that retries on certain
    exceptions.

    Args:
        a_func (callable): A callable.
        retry (RetryOptions): Configures the exceptions upon which the callable
          should retry, and the parameters to the exponential backoff retry
          algorithm.

    Returns:
        A function that will retry on exception.
    """

    max_attempts = int(retry.backoff_settings.total_timeout_millis /
                       retry.backoff_settings.initial_rpc_timeout_millis)

    def inner(*args, **kwargs):
        "Retries a_func upto max_attempt times"
        attempt_count = 0
        while 1:
            try:
                return a_func(*args, **kwargs)
            # pylint: disable=catching-non-exception
            except tuple(retry.retry_codes):
                attempt_count += 1
                if attempt_count < max_attempts:
                    continue
                raise

    return inner


def _bundleable(a_func, desc, bundler):
    """Creates a function that transforms an API call into a bundling call.

    It transform a_func from an API call that receives the requests and returns
    the response into a callable that receives the same request, and
    returns two values (collections.deque, callable[[], boolean]).

    The collections.deque will eventually contain the response to the call to
    bundle; the second value when not None is a cancellation function to stop
    the request from being used in the bundle.

    Args:
        a_func (callable[[req], resp]): an API call that supports bundling.
        desc (gax.BundleDescriptor): describes the bundling that a_func supports
        bundler (gax.bundling.Executor): orchestrates bundling

    Returns:
        callable: it takes the API call's request and returns the two
          values described above

    """
    def inner(request):
        """Schedules execution of a bundling task."""
        the_id = bundling.compute_bundle_id(
            request, desc.request_descriminator_fields)
        return bundler.schedule(a_func, the_id, desc, request)

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


class ApiCallable(object):
    """Represents zero or more API calls, with options to retry or perform
    page streaming.

    Calling an object of ApiCallable type causes these calls to be transmitted.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, func, settings):
        """Constructor.

        Args:
            func: The API call that this ApiCallable wraps.
            settings: A gax.CallSettings object from which the settings for this
                call are drawn.

        Returns:
            An ApiCallable object.
        """
        self.func = func
        self.settings = settings

    def __call__(self, *args, **kwargs):
        the_func = self.func

        # Update the_func using each of the applicable function decorators
        # before calling.
        if self.settings.retry:
            the_func = _retryable(the_func, self.settings.retry)
        if self.settings.page_descriptor:
            if self.settings.bundler and self.settings.bundle_descriptor:
                raise ValueError('ApiCallable has incompatible settings: '
                                 'bundling and page streaming')
            the_func = _page_streamable(
                the_func,
                self.settings.page_descriptor.request_page_token_field,
                self.settings.page_descriptor.response_page_token_field,
                self.settings.page_descriptor.resource_field,
                self.settings.timeout)
        else:
            the_func = _add_timeout_arg(the_func, self.settings.timeout)
            if self.settings.bundler and self.settings.bundle_descriptor:
                the_func = _bundleable(
                    the_func, self.settings.bundle_descriptor,
                    self.settings.bundler)

        return the_func(*args, **kwargs)
