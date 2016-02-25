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

from . import bundling, config


OPTION_INHERIT = object()
"""Global constant.

If a CallOptions field is set to OPTION_INHERIT, the call to which that
CallOptions belongs will attempt to inherit that field from its default
settings."""


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


class CallSettings(object):
    """Encapsulates the call settings for an ApiCallable"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=30, is_retrying=False, max_attempts=16,
                 page_descriptor=None, bundler=None, bundle_descriptor=None):
        """Constructor.

        Args:
            timeout (int): The client-side timeout for API calls.
            is_retrying (bool): If set, calls will retry upon transient error
                by default.
            max_attempts (int): The maximum number of attempts that should be
                made for a retrying call to this service. This parameter is
                ignored if ``is_retrying`` is not set.
            page_descriptor (PageDescriptor): indicates the structure of page
                streaming to be performed. If set to None, page streaming is
                not performed.
            bundler (bundle.Executor): orchestrates bundling. If None, bundling
                is not performed.
            bundle_descriptor (BundleDescriptor): indicates the structure of
                the bundle. If None, bundling is not performed.

        Returns:
            A CallSettings object.
        """
        self.timeout = timeout
        self.is_retrying = is_retrying
        self.max_attempts = max_attempts
        self.page_descriptor = page_descriptor
        self.bundler = bundler
        self.bundle_descriptor = bundle_descriptor

    def merge(self, options):
        """Returns a new CallSettings merged from this and a CallOptions object.

        Args:
            options: A CallOptions object whose values are override those in
                this object. If None, `merge` returns a copy of this object.

        Returns:
            A CallSettings object.
        """
        if not options:
            return CallSettings(
                timeout=self.timeout, is_retrying=self.is_retrying,
                max_attempts=self.max_attempts,
                page_descriptor=self.page_descriptor,
                bundler=self.bundler, bundle_descriptor=self.bundle_descriptor)
        else:
            if options.timeout == OPTION_INHERIT:
                timeout = self.timeout
            else:
                timeout = options.timeout

            if options.is_retrying == OPTION_INHERIT:
                is_retrying = self.is_retrying
            else:
                is_retrying = options.is_retrying

            if options.max_attempts == OPTION_INHERIT:
                max_attempts = self.max_attempts
            else:
                max_attempts = options.max_attempts

            if options.is_page_streaming:
                page_descriptor = self.page_descriptor
            else:
                page_descriptor = None

            return CallSettings(
                timeout=timeout, is_retrying=is_retrying,
                max_attempts=max_attempts, page_descriptor=page_descriptor,
                bundler=self.bundler, bundle_descriptor=self.bundle_descriptor)


class CallOptions(object):
    """Encapsulates the overridable settings for a particular API call"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=OPTION_INHERIT, is_retrying=OPTION_INHERIT,
                 max_attempts=OPTION_INHERIT, is_page_streaming=OPTION_INHERIT):
        """Constructor.

        Args:
            timeout (int): The client-side timeout for API calls.
            is_retrying (bool): If set, call will retry upon transient error by
                default.
            max_attempts (int): The maximum number of attempts that should be
                made for a retrying call to this service.
            is_page_streaming (bool): If set and the call is
                configured for page streaming, page streaming is performed.

        Returns:
            A CallOptions object.
        """
        self.timeout = timeout
        self.is_retrying = is_retrying
        self.max_attempts = max_attempts
        self.is_page_streaming = is_page_streaming


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
            settings: A CallSettings object from which the settings for this
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
        if self.settings.is_retrying:
            the_func = _retryable(the_func, self.settings.max_attempts)
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
