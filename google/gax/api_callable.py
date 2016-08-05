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
import random
import time

from future.utils import raise_with_traceback

from . import (BackoffSettings, BundleOptions, bundling, CallSettings, config,
               PageIterator, ResourceIterator, RetryOptions)
from .errors import GaxError, RetryError

_MILLIS_PER_SECOND = 1000


def _add_timeout_arg(a_func, timeout, **kwargs):
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

    def inner(*args):
        """Updates args with the timeout."""
        updated_args = args + (timeout,)
        return a_func(*updated_args, **kwargs)

    return inner


def _retryable(a_func, retry, **kwargs):
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

    delay_mult = retry.backoff_settings.retry_delay_multiplier
    max_delay = (retry.backoff_settings.max_retry_delay_millis /
                 _MILLIS_PER_SECOND)
    timeout_mult = retry.backoff_settings.rpc_timeout_multiplier
    max_timeout = (retry.backoff_settings.max_rpc_timeout_millis /
                   _MILLIS_PER_SECOND)
    total_timeout = (retry.backoff_settings.total_timeout_millis /
                     _MILLIS_PER_SECOND)

    def inner(*args):
        """Equivalent to ``a_func``, but retries upon transient failure.

        Retrying is done through an exponential backoff algorithm configured
        by the options in ``retry``.
        """
        delay = retry.backoff_settings.initial_retry_delay_millis
        timeout = (retry.backoff_settings.initial_rpc_timeout_millis /
                   _MILLIS_PER_SECOND)
        exc = RetryError('Retry total timeout exceeded before any'
                         'response was received')
        now = time.time()
        deadline = now + total_timeout

        while now < deadline:
            try:
                to_call = _add_timeout_arg(a_func, timeout, **kwargs)
                return to_call(*args)

            # pylint: disable=broad-except
            except Exception as exception:
                if config.exc_to_code(exception) not in retry.retry_codes:
                    raise RetryError(
                        'Exception occurred in retry method that was not'
                        ' classified as transient', exception)

                # pylint: disable=redefined-variable-type
                exc = RetryError('Retry total timeout exceeded with exception',
                                 exception)
                to_sleep = random.uniform(0, delay)
                time.sleep(to_sleep / _MILLIS_PER_SECOND)
                now = time.time()
                delay = min(delay * delay_mult, max_delay)
                timeout = min(
                    timeout * timeout_mult, max_timeout, deadline - now)
                continue

        raise exc

    return inner


def _bundleable(desc):
    """Creates a function that transforms an API call into a bundling call.

    It transform a_func from an API call that receives the requests and returns
    the response into a callable that receives the same request, and
    returns a :class:`bundling.Event`.

    The returned Event object can be used to obtain the eventual result of the
    bundled call.

    Args:
      base_caller (callable): the basic API caller for the fallback when
        bundling is actually disabled for the call.
      desc (gax.BundleDescriptor): describes the bundling that a_func
        supports.

    Returns:
      callable: takes the API call's request and keyword args and returns a
        bundling.Event object.

    """
    def inner(a_func, settings, request, **kwargs):
        """Schedules execution of a bundling task."""
        if not settings.bundler:
            return a_func(request, **kwargs)

        the_id = bundling.compute_bundle_id(
            request, desc.request_discriminator_fields)
        return settings.bundler.schedule(a_func, the_id, desc, request, kwargs)

    return inner


def _page_streamable(page_descriptor):
    """Creates a function that yields an iterable to performs page-streaming.

    Args:
        page_descriptor (:class:`PageDescriptor`): indicates the structure
          of page streaming to be performed.
    Returns:
        A function that returns an iterator.
    """

    def inner(a_func, settings, request, **kwargs):
        """Actual page-streaming based on the settings."""
        page_iterator = PageIterator(
            a_func, page_descriptor, settings.page_token, request, **kwargs)
        if settings.flatten_pages:
            return ResourceIterator(page_iterator)
        else:
            return page_iterator

    return inner


def _construct_bundling(bundle_config, bundle_descriptor):
    """Helper for ``construct_settings()``.

    Args:
      bundle_config: A dictionary specifying a bundle parameters, the value for
        'bundling' field in a method config (See ``construct_settings()`` for
        information on this config.)
      bundle_descriptor: A BundleDescriptor object describing the structure of
        bundling for this method. If not set, this method will not bundle.

    Returns:
      A tuple (bundling.Executor, BundleDescriptor) that configures bundling.
      The bundling.Executor may be None if this method should not bundle.
    """
    if bundle_config and bundle_descriptor:
        bundler = bundling.Executor(BundleOptions(
            element_count_threshold=bundle_config.get(
                'element_count_threshold', 0),
            element_count_limit=bundle_config.get('element_count_limit', 0),
            request_byte_threshold=bundle_config.get(
                'request_byte_threshold', 0),
            request_byte_limit=bundle_config.get('request_byte_limit', 0),
            delay_threshold=bundle_config.get('delay_threshold_millis', 0)))
    else:
        bundler = None

    return bundler


def _construct_retry(method_config, retry_codes, retry_params, retry_names):
    """Helper for ``construct_settings()``.

    Args:
      method_config: A dictionary representing a single ``methods`` entry of the
        standard API client config file. (See ``construct_settings()`` for
        information on this yaml.)
      retry_codes: A dictionary parsed from the ``retry_codes`` entry
        of the standard API client config file. (See ``construct_settings()``
        for information on this yaml.)
      retry_params: A dictionary parsed from the ``retry_params`` entry
        of the standard API client config file. (See ``construct_settings()``
        for information on this yaml.)
      retry_names: A dictionary mapping the string names used in the
        standard API client config file to API response status codes.

    Returns:
      A RetryOptions object, or None.
    """
    if method_config is None:
        return None

    codes = None
    if retry_codes and 'retry_codes_name' in method_config:
        codes_name = method_config['retry_codes_name']
        if codes_name in retry_codes and retry_codes[codes_name]:
            codes = [retry_names[name] for name in retry_codes[codes_name]]
        else:
            codes = []

    backoff_settings = None
    if retry_params and 'retry_params_name' in method_config:
        params_name = method_config['retry_params_name']
        if params_name and params_name in retry_params:
            backoff_settings = BackoffSettings(**retry_params[params_name])

    return RetryOptions(retry_codes=codes, backoff_settings=backoff_settings)


def _merge_retry_options(retry, overrides):
    """Helper for ``construct_settings()``.

    Takes two retry options, and merges them into a single RetryOption instance.

    Args:
      retry: The base RetryOptions.
      overrides: The RetryOptions used for overriding ``retry``. Use the values
        if it is not None. If entire ``overrides`` is None, ignore the base
        retry and return None.

    Returns:
      The merged RetryOptions, or None if it will be canceled.
    """
    if overrides is None:
        return None

    if overrides.retry_codes is None and overrides.backoff_settings is None:
        return retry

    codes = retry.retry_codes
    if overrides.retry_codes is not None:
        codes = overrides.retry_codes
    backoff_settings = retry.backoff_settings
    if overrides.backoff_settings is not None:
        backoff_settings = overrides.backoff_settings

    return RetryOptions(retry_codes=codes, backoff_settings=backoff_settings)


def _upper_camel_to_lower_under(string):
    if not string:
        return ''
    out = ''
    out += string[0].lower()
    for char in string[1:]:
        if char.isupper():
            out += '_' + char.lower()
        else:
            out += char
    return out


def construct_settings(
        service_name, client_config, config_override,
        retry_names, bundle_descriptors=None, page_descriptors=None,
        kwargs=None):
    """Constructs a dictionary mapping method names to CallSettings.

    The ``client_config`` parameter is parsed from a client configuration JSON
    file of the form:

    .. code-block:: json

       {
         "interfaces": {
           "google.fake.v1.ServiceName": {
             "retry_codes": {
               "idempotent": ["UNAVAILABLE", "DEADLINE_EXCEEDED"],
               "non_idempotent": []
             },
             "retry_params": {
               "default": {
                 "initial_retry_delay_millis": 100,
                 "retry_delay_multiplier": 1.2,
                 "max_retry_delay_millis": 1000,
                 "initial_rpc_timeout_millis": 2000,
                 "rpc_timeout_multiplier": 1.5,
                 "max_rpc_timeout_millis": 30000,
                 "total_timeout_millis": 45000
               }
             },
             "methods": {
               "CreateFoo": {
                 "retry_codes_name": "idempotent",
                 "retry_params_name": "default",
                 "timeout_millis": 30000
               },
               "Publish": {
                 "retry_codes_name": "non_idempotent",
                 "retry_params_name": "default",
                 "bundling": {
                   "element_count_threshold": 40,
                   "element_count_limit": 200,
                   "request_byte_threshold": 90000,
                   "request_byte_limit": 100000,
                   "delay_threshold_millis": 100
                 }
               }
             }
           }
         }
       }

    Args:
      service_name: The fully-qualified name of this service, used as a key into
        the client config file (in the example above, this value should be
        ``google.fake.v1.ServiceName``).
      client_config: A dictionary parsed from the standard API client config
        file.
      bundle_descriptors: A dictionary of method names to BundleDescriptor
        objects for methods that are bundling-enabled.
      page_descriptors: A dictionary of method names to PageDescriptor objects
        for methods that are page streaming-enabled.
      config_override: A dictionary in the same structure of client_config to
        override the settings. Usually client_config is supplied from the
        default config and config_override will be specified by users.
      retry_names: A dictionary mapping the strings referring to response status
        codes to the Python objects representing those codes.
      kwargs: The keyword arguments to be passed to the API calls.

    Raises:
      KeyError: If the configuration for the service in question cannot be
        located in the provided ``client_config``.
    """
    # pylint: disable=too-many-locals
    defaults = {}
    bundle_descriptors = bundle_descriptors or {}
    page_descriptors = page_descriptors or {}
    kwargs = kwargs or {}

    try:
        service_config = client_config['interfaces'][service_name]
    except KeyError:
        raise KeyError('Client configuration not found for service: {}'
                       .format(service_name))

    overrides = config_override.get('interfaces', {}).get(service_name, {})

    for method in service_config.get('methods'):
        method_config = service_config['methods'][method]
        overriding_method = overrides.get('methods', {}).get(method, {})
        snake_name = _upper_camel_to_lower_under(method)

        if overriding_method and overriding_method.get('timeout_millis'):
            timeout = overriding_method['timeout_millis']
        else:
            timeout = method_config['timeout_millis']
        timeout /= _MILLIS_PER_SECOND

        bundle_descriptor = bundle_descriptors.get(snake_name)
        bundling_config = method_config.get('bundling', None)
        if overriding_method and 'bundling' in overriding_method:
            bundling_config = overriding_method['bundling']
        bundler = _construct_bundling(bundling_config, bundle_descriptor)

        retry = _merge_retry_options(
            _construct_retry(method_config, service_config['retry_codes'],
                             service_config['retry_params'], retry_names),
            _construct_retry(overriding_method, overrides.get('retry_codes'),
                             overrides.get('retry_params'), retry_names))

        defaults[snake_name] = CallSettings(
            timeout=timeout, retry=retry,
            page_descriptor=page_descriptors.get(snake_name),
            bundler=bundler, bundle_descriptor=bundle_descriptor,
            kwargs=kwargs)
    return defaults


def _catch_errors(a_func, errors):
    """Updates a_func to wrap exceptions with GaxError

    Args:
        a_func (callable): A callable.
        retry (list[Exception]): Configures the exceptions to wrap.

    Returns:
        A function that will wrap certain exceptions with GaxError
    """
    def inner(*args, **kwargs):
        """Wraps specified exceptions"""
        try:
            return a_func(*args, **kwargs)
        # pylint: disable=catching-non-exception
        except tuple(errors) as exception:
            raise_with_traceback(GaxError('RPC failed', cause=exception))

    return inner


def create_api_call(func, settings):
    """Converts an rpc call into an API call governed by the settings.

    In typical usage, ``func`` will be a callable used to make an rpc request.
    This will mostly likely be a bound method from a request stub used to make
    an rpc call.

    The result is created by applying a series of function decorators defined
    in this module to ``func``.  ``settings`` is used to determine which
    function decorators to apply.

    The result is another callable which for most values of ``settings`` has
    has the same signature as the original. Only when ``settings`` configures
    bundling does the signature change.

    Args:
      func (callable[[object], object]): is used to make a bare rpc call
      settings (:class:`CallSettings`): provides the settings for this call

    Returns:
      func (callable[[object], object]): a bound method on a request stub used
        to make an rpc call

    Raises:
       ValueError: if ``settings`` has incompatible values, e.g, if bundling
         and page_streaming are both configured

    """
    def base_caller(api_call, _, *args):
        """Simply call api_call and ignore settings."""
        return api_call(*args)

    def inner(request, options=None):
        """Invoke with the actual settings."""
        this_settings = settings.merge(options)
        if this_settings.retry and this_settings.retry.retry_codes:
            api_call = _retryable(
                func, this_settings.retry, **this_settings.kwargs)
        else:
            api_call = _add_timeout_arg(
                func, this_settings.timeout, **this_settings.kwargs)
        api_call = _catch_errors(api_call, config.API_ERRORS)
        return api_caller(api_call, this_settings, request)

    if settings.page_descriptor:
        if settings.bundler and settings.bundle_descriptor:
            raise ValueError('The API call has incompatible settings: '
                             'bundling and page streaming')
        api_caller = _page_streamable(settings.page_descriptor)
    elif settings.bundler and settings.bundle_descriptor:
        api_caller = _bundleable(settings.bundle_descriptor)
    else:
        api_caller = base_caller

    return inner
