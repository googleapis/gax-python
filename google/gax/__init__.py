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

"""Google API Extensions"""

from __future__ import absolute_import

import collections
import logging

import pkg_resources

# pylint: disable=no-member
__version__ = pkg_resources.get_distribution('google-gax').version
# pylint: enable=no-member


_LOG = logging.getLogger(__name__)
_LOG.addHandler(logging.NullHandler())


INITIAL_PAGE = object()
"""A placeholder for the page token passed into an initial paginated request."""


OPTION_INHERIT = object()
"""Global constant.

If a CallOptions field is set to OPTION_INHERIT, the call to which that
CallOptions belongs will attempt to inherit that field from its default
settings."""


class _CallSettings(object):
    """Encapsulates the call settings for an API call."""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=30, retry=None, page_descriptor=None,
                 page_token=None, bundler=None, bundle_descriptor=None,
                 kwargs=None):
        """Constructor.

        Args:
            timeout (int): The client-side timeout for API calls. This
              parameter is ignored for retrying calls.
            retry (RetryOptions): The configuration for retrying upon
              transient error. If set to None, this call will not retry.
            page_descriptor (PageDescriptor): indicates the structure
              of page streaming to be performed. If set to None, page streaming
              is disabled.
            page_token (str): If there is no ``page_descriptor``, this attribute
              has no meaning. Otherwise, determines the page token used in the
              page streaming request.
            bundler (gax.bundling.Executor): orchestrates bundling. If
              None, bundling is not performed.
            bundle_descriptor (BundleDescriptor): indicates the
              structure of of the bundle. If None, bundling is disabled.
            kwargs (dict): other keyword arguments to be passed to the API
              calls.
        """
        self.timeout = timeout
        self.retry = retry
        self.page_descriptor = page_descriptor
        self.page_token = page_token
        self.bundler = bundler
        self.bundle_descriptor = bundle_descriptor
        self.kwargs = kwargs or {}

    @property
    def flatten_pages(self):
        """
        A boolean property indicating whether a page streamed response should
        make the page structure transparent to the user by flattening the
        repeated field in the returned iterator.

        There is no ``page_descriptor``, this means nothing.
        """
        return self.page_token is None

    def merge(self, options):
        """Returns new _CallSettings merged from this and a CallOptions object.

        Note that passing if the CallOptions instance specifies a page_token,
        the merged _CallSettings will have ``flatten_pages`` disabled. This
        permits toggling per-resource/per-page page streaming.

        Args:
            options (CallOptions): an instance whose values override
              those in this object. If None, ``merge`` returns a copy of this
              object

        Returns:
            CallSettings: The merged settings and options.
        """
        if not options:
            return _CallSettings(
                timeout=self.timeout, retry=self.retry,
                page_descriptor=self.page_descriptor,
                page_token=self.page_token,
                bundler=self.bundler, bundle_descriptor=self.bundle_descriptor,
                kwargs=self.kwargs)
        else:
            if options.timeout == OPTION_INHERIT:
                timeout = self.timeout
            else:
                timeout = options.timeout

            if options.retry == OPTION_INHERIT:
                retry = self.retry
            else:
                retry = options.retry

            if options.page_token == OPTION_INHERIT:
                page_token = self.page_token
            else:
                page_token = options.page_token

            if options.is_bundling:
                bundler = self.bundler
            else:
                bundler = None

            if options.kwargs == OPTION_INHERIT:
                kwargs = self.kwargs
            else:
                kwargs = self.kwargs.copy()
                kwargs.update(options.kwargs)

            return _CallSettings(
                timeout=timeout, retry=retry,
                page_descriptor=self.page_descriptor, page_token=page_token,
                bundler=bundler, bundle_descriptor=self.bundle_descriptor,
                kwargs=kwargs)


class CallOptions(object):
    """Encapsulates the overridable settings for a particular API call.

    ``CallOptions`` is an optional arg for all GAX API calls.  It is used to
    configure the settings of a specific API call.

    When provided, its values override the GAX service defaults for that
    particular call.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=OPTION_INHERIT, retry=OPTION_INHERIT,
                 page_token=OPTION_INHERIT, is_bundling=False, **kwargs):
        """
        Example:
           >>> # change an api call's timeout
           >>> o1 = CallOptions(timeout=30)  # make the timeout 30 seconds
           >>>
           >>> # set page streaming to be per-page on a call where it is
           >>> # normally per-resource
           >>> o2 = CallOptions(page_token=INITIAL_PAGE)
           >>>
           >>> # disable retrying on an api call that normally retries
           >>> o3 = CallOptions(retry=None)
           >>>
           >>> # enable bundling on a call that supports it
           >>> o4 = CallOptions(is_bundling=True)

        Args:
            timeout (int): The client-side timeout for non-retrying API calls.
            retry (RetryOptions): determines whether and how to retry
              on transient errors. When set to None, the call will not retry.
            page_token (str): If set and the call is configured for page
              streaming, page streaming is performed per-page, starting with
              this page_token. Use ``INITIAL_PAGE`` for the first request.
              If unset and the call is configured for page streaming, page
              streaming is performed per-resource.
            is_bundling (bool): If set and the call is configured for bundling,
              bundling is performed. Bundling is always disabled by default.
            kwargs: Additional arguments passed through to the API call.

        Raises:
          ValueError: if incompatible options are specified.
        """
        if not (timeout == OPTION_INHERIT or retry == OPTION_INHERIT):
            raise ValueError('The CallOptions has incompatible settings: '
                             '"timeout" cannot be specified on a retrying call')
        self.timeout = timeout
        self.retry = retry
        self.page_token = page_token
        self.is_bundling = is_bundling
        self.kwargs = kwargs or OPTION_INHERIT


class PageDescriptor(
        collections.namedtuple(
            'PageDescriptor',
            ['request_page_token_field',
             'response_page_token_field',
             'resource_field'])):
    """Describes the structure of a page-streaming call."""
    pass


class RetryOptions(
        collections.namedtuple(
            'RetryOptions',
            ['retry_codes',
             'backoff_settings'])):
    """Per-call configurable settings for retrying upon transient failure.

    Attributes:
      retry_codes (list[string]): a list of Google API canonical error codes
        upon which a retry should be attempted.
      backoff_settings (:class:`BackoffSettings`): configures the retry
        exponential backoff algorithm.
    """
    pass


class BackoffSettings(
        collections.namedtuple(
            'BackoffSettings',
            ['initial_retry_delay_millis',
             'retry_delay_multiplier',
             'max_retry_delay_millis',
             'initial_rpc_timeout_millis',
             'rpc_timeout_multiplier',
             'max_rpc_timeout_millis',
             'total_timeout_millis'])):
    """Parameters to the exponential backoff algorithm for retrying.

    Attributes:
      initial_retry_delay_millis: the initial delay time, in milliseconds,
        between the completion of the first failed request and the initiation of
        the first retrying request.
      retry_delay_multiplier: the multiplier by which to increase the delay time
        between the completion of failed requests, and the initiation of the
        subsequent retrying request.
      max_retry_delay_millis: the maximum delay time, in milliseconds, between
        requests. When this value is reached, ``retry_delay_multiplier`` will no
        longer be used to increase delay time.
      initial_rpc_timeout_millis: the initial timeout parameter to the request.
      rpc_timeout_multiplier: the multiplier by which to increase the timeout
        parameter between failed requests.
      max_rpc_timeout_millis: the maximum timeout parameter, in milliseconds,
        for a request. When this value is reached, ``rpc_timeout_multiplier``
        will no longer be used to increase the timeout.
      total_timeout_millis: the total time, in milliseconds, starting from when
        the initial request is sent, after which an error will be returned,
        regardless of the retrying attempts made meanwhile.
    """
    pass


class BundleDescriptor(
        collections.namedtuple(
            'BundleDescriptor',
            ['bundled_field',
             'request_discriminator_fields',
             'subresponse_field'])):
    """Describes the structure of bundled call.

    request_discriminator_fields may include '.' as a separator, which is used
    to indicate object traversal.  This allows fields in nested objects to be
    used to determine what requests to bundle.

    Attributes:
      bundled_field: the repeated field in the request message that
        will have its elements aggregated by bundling
      request_discriminator_fields: a list of fields in the
        target request message class that are used to determine
        which messages should be bundled together.
      subresponse_field: an optional field, when present it indicates the field
        in the response message that should be used to demultiplex the response
        into multiple response messages.
    """
    def __new__(cls,
                bundled_field,
                request_discriminator_fields,
                subresponse_field=None):
        return super(cls, BundleDescriptor).__new__(
            cls,
            bundled_field,
            request_discriminator_fields,
            subresponse_field)


class BundleOptions(
        collections.namedtuple(
            'BundleOptions',
            ['element_count_threshold',
             'element_count_limit',
             'request_byte_threshold',
             'request_byte_limit',
             'delay_threshold'])):
    """Holds values used to configure bundling.

    The xxx_threshold attributes are used to configure when the bundled request
    should be made.

    Attributes:
        element_count_threshold: the bundled request will be sent once the
          count of outstanding elements in the repeated field reaches this
          value.
        element_count_limit: represents a hard limit on the number of elements
          in the repeated field of the bundle; if adding a request to a bundle
          would exceed this value, the bundle is sent and the new request is
          added to a fresh bundle. It is invalid for a single request to exceed
          this limit.
        request_byte_threshold: the bundled request will be sent once the count
          of bytes in the request reaches this value. Note that this value is
          pessimistically approximated by summing the bytesizes of the elements
          in the repeated field, and therefore may be an under-approximation.
        request_byte_limit: represents a hard limit on the size of the bundled
          request; if adding a request to a bundle would exceed this value, the
          bundle is sent and the new request is added to a fresh bundle. It is
          invalid for a single request to exceed this limit. Note that this
          value is pessimistically approximated by summing the bytesizes of the
          elements in the repeated field, with a buffer applied to correspond to
          the resulting under-approximation.
        delay_threshold: the bundled request will be sent this amount of
          time after the first element in the bundle was added to it.

    """
    # pylint: disable=too-few-public-methods

    def __new__(cls,
                element_count_threshold=0,
                element_count_limit=0,
                request_byte_threshold=0,
                request_byte_limit=0,
                delay_threshold=0):
        """Invokes the base constructor with default values.

        The default values are zero for all attributes and it's necessary to
        specify at least one valid threshold value during construction.

        Args:
            element_count_threshold (int): the bundled request will be sent
                once the count of outstanding elements in the repeated field
                reaches this value.
            element_count_limit (int): represents a hard limit on the number of
                elements in the repeated field of the bundle; if adding a
                request to a bundle would exceed this value, the bundle is sent
                and the new request is added to a fresh bundle. It is invalid
                for a single request to exceed this limit.
            request_byte_threshold (int): the bundled request will be sent once
                the count of bytes in the request reaches this value. Note that
                this value is pessimistically approximated by summing the
                bytesizes of the elements in the repeated field, with a buffer
                applied to compensate for the corresponding
                under-approximation.
            request_byte_limit (int): represents a hard limit on the size of
                the bundled request; if adding a request to a bundle would
                exceed this value, the bundle is sent and the new request is
                added to a fresh bundle. It is invalid for a single request to
                exceed this limit. Note that this value is pessimistically
                approximated by summing the bytesizes of the elements in the
                repeated field, with a buffer applied to correspond to the
                resulting under-approximation.
            delay_threshold (int): the bundled request will be sent this amount
                of time after the first element in the bundle was added to it.

        Returns:
          BundleOptions: the constructed object.
        """
        assert isinstance(element_count_threshold, int), 'should be an int'
        assert isinstance(element_count_limit, int), 'should be an int'
        assert isinstance(request_byte_threshold, int), 'should be an int'
        assert isinstance(request_byte_limit, int), 'should be an int'
        assert isinstance(delay_threshold, int), 'should be an int'
        assert (element_count_threshold > 0 or
                request_byte_threshold > 0 or
                delay_threshold > 0), 'one threshold should be > 0'

        return super(cls, BundleOptions).__new__(
            cls,
            element_count_threshold,
            element_count_limit,
            request_byte_threshold,
            request_byte_limit,
            delay_threshold)


class PageIterator(object):
    """An iterator over the pages of a page streaming API call.

    Provides access to the individual pages of the call, as well as the page
    token.

    Attributes:
      response: The full response message for the call most recently made, or
        None if a call has not yet been made.
      page_token: The page token to be passed in the request for the next call
        to be made.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, api_call, page_descriptor, page_token, request,
                 **kwargs):
        """
        Args:
          api_call (Callable[[req], resp]): an API call that is page
            streaming.
          page_descriptor (PageDescriptor): indicates the structure
            of page streaming to be performed.
          page_token (str): The page token to be passed to API call request.
            If no page token has yet been acquired, this field should be set
            to ``INITIAL_PAGE``.
          request (object): The request to be passed to the API call. The page
            token field of the request is overwritten by the ``page_token``
            passed to the constructor, unless ``page_token`` is
            ``INITIAL_PAGE``.
          kwargs: Arbitrary keyword arguments to be passed to the API call.
        """
        self.response = None
        self.page_token = page_token or INITIAL_PAGE
        self._func = api_call
        self._page_descriptor = page_descriptor
        self._request = request
        self._kwargs = kwargs
        self._done = False

    def __iter__(self):
        return self

    def next(self):
        """For Python 2.7 compatibility; see __next__."""
        return self.__next__()

    def __next__(self):
        """Retrieves the next page."""
        if self._done:
            raise StopIteration
        if self.page_token != INITIAL_PAGE:
            setattr(self._request,
                    self._page_descriptor.request_page_token_field,
                    self.page_token)
        response = self._func(self._request, **self._kwargs)
        self.page_token = getattr(
            response, self._page_descriptor.response_page_token_field)
        if not self.page_token:
            self._done = True
        return getattr(response, self._page_descriptor.resource_field)


class ResourceIterator(object):
    """An iterator over resources of the page iterator."""

    # pylint: disable=too-few-public-methods
    def __init__(self, page_iterator):
        """Constructor.

        Args:
          page_iterator (PageIterator): the base iterator of getting pages.
        """
        self._page_iterator = page_iterator
        self._current = None
        self._index = -1

    def __iter__(self):
        return self

    def next(self):
        """For Python 2.7 compatibility; see __next__."""
        return self.__next__()

    def __next__(self):
        """Retrieves the next resource."""
        # pylint: disable=next-method-called
        while not self._current:
            self._current = next(self._page_iterator)
            self._index = 0
        resource = self._current[self._index]
        self._index += 1
        if self._index >= len(self._current):
            self._current = None
        return resource
