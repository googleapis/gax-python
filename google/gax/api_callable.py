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

from __future__ import absolute_import, division, unicode_literals

from future import utils

from google import gax
from google.gax import bundling
from google.gax.utils import metrics

_MILLIS_PER_SECOND = 1000


def _bundleable(desc):
    """Creates a function that transforms an API call into a bundling call.

    It transform a_func from an API call that receives the requests and returns
    the response into a callable that receives the same request, and
    returns a :class:`bundling.Event`.

    The returned Event object can be used to obtain the eventual result of the
    bundled call.

    Args:
      desc (gax.BundleDescriptor): describes the bundling that a_func
        supports.

    Returns:
      Callable: takes the API call's request and keyword args and returns a
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
        Callable: A function that returns an iterator.
    """

    def inner(a_func, settings, request, **kwargs):
        """Actual page-streaming based on the settings."""
        page_iterator = gax.PageIterator(
            a_func, page_descriptor, settings.page_token, request, **kwargs)
        if settings.flatten_pages:
            return gax.ResourceIterator(page_iterator)
        else:
            return page_iterator

    return inner


def _construct_bundling(bundle_config, bundle_descriptor):
    """Helper for ``construct_settings()``.

    Args:
      bundle_config (dict): A dictionary specifying a bundle parameters, the
        value for 'bundling' field in a method config (See
        ``construct_settings()`` for information on this config.)
      bundle_descriptor (BundleDescriptor): A BundleDescriptor object
        describing the structure of bundling for this method. If not set,
        this method will not bundle.

    Returns:
      Tuple[bundling.Executor, BundleDescriptor]: A tuple that configures
        bundling. The bundling.Executor may be None if this method should not
        bundle.
    """
    if bundle_config and bundle_descriptor:
        bundler = bundling.Executor(gax.BundleOptions(
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
      method_config (dict): A dictionary representing a single ``methods``
        entry of the standard API client config file. (See
        ``construct_settings()`` for information on this yaml.)
      retry_codes (dict): A dictionary parsed from the ``retry_codes`` entry
        of the standard API client config file. (See ``construct_settings()``
        for information on this yaml.)
      retry_params (dict): A dictionary parsed from the ``retry_params`` entry
        of the standard API client config file. (See ``construct_settings()``
        for information on this yaml.)
      retry_names (dict): A dictionary mapping the string names used in the
        standard API client config file to API response status codes.

    Returns:
      Optional[RetryOptions]: The retry options, if applicable.
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
            backoff_settings = gax.BackoffSettings(**retry_params[params_name])

    return gax.RetryOptions(
        backoff_settings=backoff_settings,
        retry_codes=codes,
    )


def _merge_retry_options(retry_options, overrides):
    """Helper for ``construct_settings()``.

    Takes two retry options, and merges them into a single RetryOption instance.

    Args:
      retry_options (RetryOptions): The base RetryOptions.
      overrides (RetryOptions): The RetryOptions used for overriding ``retry``.
        Use the values if it is not None. If entire ``overrides`` is None,
        ignore the base retry and return None.

    Returns:
      RetryOptions: The merged options, or None if it will be canceled.
    """
    if overrides is None:
        return None

    if overrides.retry_codes is None and overrides.backoff_settings is None:
        return retry_options

    codes = retry_options.retry_codes
    if overrides.retry_codes is not None:
        codes = overrides.retry_codes
    backoff_settings = retry_options.backoff_settings
    if overrides.backoff_settings is not None:
        backoff_settings = overrides.backoff_settings

    return gax.RetryOptions(
        backoff_settings=backoff_settings,
        retry_codes=codes,
    )


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
        metrics_headers=(), kwargs=None):
    """Constructs a dictionary mapping method names to _CallSettings.

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
      service_name (str): The fully-qualified name of this service, used as a
        key into the client config file (in the example above, this value
        would be ``google.fake.v1.ServiceName``).
      client_config (dict): A dictionary parsed from the standard API client
        config file.
      bundle_descriptors (Mapping[str, BundleDescriptor]): A dictionary of
        method names to BundleDescriptor objects for methods that are
        bundling-enabled.
      page_descriptors (Mapping[str, PageDescriptor]): A dictionary of method
        names to PageDescriptor objects for methods that are page
        streaming-enabled.
      config_override (str): A dictionary in the same structure of
        client_config to override the settings. Usually client_config is
        supplied from the default config and config_override will be
        specified by users.
      retry_names (Mapping[str, object]): A dictionary mapping the strings
        referring to response status codes to the Python objects representing
        those codes.
      metrics_headers (Mapping[str, str]): Dictionary of headers to be passed
        for analytics. Sent as a dictionary; eventually becomes a
        space-separated string (e.g. 'foo/1.0.0 bar/3.14.1').
      kwargs (dict): The keyword arguments to be passed to the API calls.

    Returns:
      dict: A dictionary mapping method names to _CallSettings.

    Raises:
      KeyError: If the configuration for the service in question cannot be
        located in the provided ``client_config``.
    """
    # pylint: disable=too-many-locals
    # pylint: disable=protected-access
    defaults = {}
    bundle_descriptors = bundle_descriptors or {}
    page_descriptors = page_descriptors or {}
    kwargs = kwargs or {}

    # Sanity check: It is possible that we got this far but some headers
    # were specified with an older library, which sends them as...
    #   kwargs={'metadata': [('x-goog-api-client', 'foo/1.0 bar/3.0')]}
    #
    # Note: This is the final format we will send down to GRPC shortly.
    #
    # Remove any x-goog-api-client header that may have been present
    # in the metadata list.
    if 'metadata' in kwargs:
        kwargs['metadata'] = [value for value in kwargs['metadata']
                              if value[0].lower() != 'x-goog-api-client']

    # Fill out the metrics headers with GAX and GRPC info, and convert
    # to a string in the format that the GRPC layer expects.
    kwargs.setdefault('metadata', [])
    kwargs['metadata'].append(
        ('x-goog-api-client', metrics.stringify(metrics.fill(metrics_headers)))
    )

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

        retry_options = _merge_retry_options(
            _construct_retry(method_config, service_config['retry_codes'],
                             service_config['retry_params'], retry_names),
            _construct_retry(overriding_method, overrides.get('retry_codes'),
                             overrides.get('retry_params'), retry_names))

        defaults[snake_name] = gax._CallSettings(
            timeout=timeout, retry=retry_options,
            page_descriptor=page_descriptors.get(snake_name),
            bundler=bundler, bundle_descriptor=bundle_descriptor,
            kwargs=kwargs)
    return defaults


def _catch_errors(a_func, to_catch):
    """Updates a_func to wrap exceptions with GaxError

    Args:
        a_func (callable): A callable.
        to_catch (list[Exception]): Configures the exceptions to wrap.

    Returns:
        Callable: A function that will wrap certain exceptions with GaxError
    """
    def inner(*args, **kwargs):
        """Wraps specified exceptions"""
        try:
            return a_func(*args, **kwargs)
        # pylint: disable=catching-non-exception
        except tuple(to_catch) as exception:
            utils.raise_with_traceback(
                gax.errors.create_error('RPC failed', cause=exception))

    return inner


def _merge_options_metadata(options, settings):
    """Merge metadata list (add all missing tuples)"""
    if not options:
        return options
    kwargs = options.kwargs
    if kwargs == gax.OPTION_INHERIT or 'metadata' not in kwargs:
        return options

    kwarg_meta_dict = {}
    merged_kwargs = options.kwargs.copy()
    for kwarg_meta in merged_kwargs['metadata']:
        kwarg_meta_dict[kwarg_meta[0].lower()] = kwarg_meta
    for kwarg_meta in settings.kwargs['metadata']:
        if kwarg_meta[0].lower() not in kwarg_meta_dict:
            merged_kwargs['metadata'].append(kwarg_meta)
    return gax.CallOptions(
        timeout=options.timeout, retry=options.retry,
        page_token=options.page_token,
        is_bundling=options.is_bundling,
        **merged_kwargs)


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
      func (Callable[Sequence[object], object]): is used to make a bare rpc
        call.
      settings (_CallSettings): provides the settings for this call

    Returns:
      Callable[Sequence[object], object]: a bound method on a request stub used
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
        this_options = _merge_options_metadata(options, settings)
        this_settings = settings.merge(this_options)

        if this_settings.retry and this_settings.retry.retry_codes:
            api_call = gax.retry.retryable(
                func, this_settings.retry, **this_settings.kwargs)
        else:
            api_call = gax.retry.add_timeout_arg(
                func, this_settings.timeout, **this_settings.kwargs)
        api_call = _catch_errors(api_call, gax.config.API_ERRORS)
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
