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


__version__ = '0.9.3'


OPTION_INHERIT = object()
"""Global constant.

If a CallOptions field is set to OPTION_INHERIT, the call to which that
CallOptions belongs will attempt to inherit that field from its default
settings."""


class CallSettings(object):
    """Encapsulates the call settings for an ApiCallable"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=30, retry=None, page_descriptor=None,
                 bundler=None, bundle_descriptor=None):
        """Constructor.

        Args:
            timeout (int): The client-side timeout for API calls. This
              parameter is ignored for retrying calls.
            retry (RetryOptions): The configuration for retrying upon transient
              error. If set to None, this call will not retry.
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
        self.retry = retry
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
                timeout=self.timeout, retry=self.retry,
                page_descriptor=self.page_descriptor, bundler=self.bundler,
                bundle_descriptor=self.bundle_descriptor)
        else:
            if options.timeout == OPTION_INHERIT:
                timeout = self.timeout
            else:
                timeout = options.timeout

            if options.retry == OPTION_INHERIT:
                retry = self.retry
            else:
                retry = options.retry

            if options.is_page_streaming:
                page_descriptor = self.page_descriptor
            else:
                page_descriptor = None

            return CallSettings(
                timeout=timeout, retry=retry,
                page_descriptor=page_descriptor, bundler=self.bundler,
                bundle_descriptor=self.bundle_descriptor)


class CallOptions(object):
    """Encapsulates the overridable settings for a particular API call"""
    # pylint: disable=too-few-public-methods
    def __init__(self, timeout=OPTION_INHERIT, retry=OPTION_INHERIT,
                 is_page_streaming=OPTION_INHERIT):
        """Constructor.

        Args:
            timeout (int): The client-side timeout for API calls.
            retry (RetryOptions): The configuration for retrying upon transient error.
              If set to None, this call will not retry.
            is_page_streaming (bool): If set and the call is configured for page
              streaming, page streaming is performed.

        Returns:
            A CallOptions object.
        """
        self.timeout = timeout
        self.retry = retry
        self.is_page_streaming = is_page_streaming


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
      retry_codes: a list of exceptions upon which a retry should be attempted.
      backoff_settings: a BackoffSettings object configuring the retry
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
           element_count_threshold: the bundled request will be sent once the
             count of outstanding elements in the repeated field reaches this
             value.
           element_count_limit: represents a hard limit on the number of
             elements in the repeated field of the bundle; if adding a request
             to a bundle would exceed this value, the bundle is sent and the new
             request is added to a fresh bundle. It is invalid for a single
             request to exceed this limit.
           request_byte_threshold: the bundled request will be sent once the
             count of bytes in the request reaches this value. Note that this
             value is pessimistically approximated by summing the bytesizes of
             the elements in the repeated field, with a buffer applied to
             compensate for the corresponding under-approximation.
           request_byte_limit: represents a hard limit on the size of the
             bundled request; if adding a request to a bundle would exceed this
             value, the bundle is sent and the new request is added to a fresh
             bundle. It is invalid for a single request to exceed this
             limit. Note that this value is pessimistically approximated by
             summing the bytesizes of the elements in the repeated field, with a
             buffer applied to correspond to the resulting under-approximation.
           delay_threshold: the bundled request will be sent this amount of
             time after the first element in the bundle was added to it.

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
