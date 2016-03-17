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

from . import (BackoffSettings, BundleDescriptor, BundleOptions, bundling,
               CallSettings, config, OPTION_INHERIT, PageDescriptor,
               RetryOptions, RetryException)


_MILLIS_PER_SECOND = 1000


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

    delay_mult = retry.backoff_settings.retry_delay_multiplier
    max_delay = (retry.backoff_settings.max_retry_delay_millis /
                 _MILLIS_PER_SECOND)
    timeout_mult = retry.backoff_settings.rpc_timeout_multiplier
    max_timeout = (retry.backoff_settings.max_rpc_timeout_millis /
                   _MILLIS_PER_SECOND)
    total_timeout = (retry.backoff_settings.total_timeout_millis /
                     _MILLIS_PER_SECOND)

    def inner(*args, **kwargs):
        """Equivalent to ``a_func``, but retries upon transient failure.

        Retrying is done through an exponential backoff algorithm configured
        by the options in ``retry``.
        """
        delay = retry.backoff_settings.initial_retry_delay_millis
        timeout = (retry.backoff_settings.initial_rpc_timeout_millis /
                   _MILLIS_PER_SECOND)
        exc = RetryException('Retry total timeout exceeded before any'
                             'response was received')
        now = time.time()
        deadline = now + total_timeout

        while now < deadline:
            try:
                to_call = _add_timeout_arg(a_func, timeout)
                return to_call(*args, **kwargs)

            # pylint: disable=broad-except
            except Exception as exception:
                if config.exc_to_code(exception) not in retry.retry_codes:
                    raise

                # pylint: disable=redefined-variable-type
                exc = exception
                to_sleep = random.uniform(0, delay)
                time.sleep(to_sleep / _MILLIS_PER_SECOND)
                now = time.time()
                delay = min(delay * delay_mult, max_delay)
                timeout = min(
                    timeout * timeout_mult, max_timeout, deadline - now)
                continue

        raise exc

    return inner


def _bundleable(a_func, desc, bundler):
    """Creates a function that transforms an API call into a bundling call.

    It transform a_func from an API call that receives the requests and returns
    the response into a callable that receives the same request, and
    returns a :class:`bundling.Event`.

    The returned Event object can be used to obtain the eventual result of the
    bundled call.

    Args:
        a_func (callable[[req], resp]): an API call that supports bundling.
        desc (gax.BundleDescriptor): describes the bundling that a_func
          supports.
        bundler (gax.bundling.Executor): orchestrates bundling.

    Returns:
        callable: takes the API call's request and keyword args and returns a
          bundling.Event object.

    """
    def inner(*args, **kwargs):
        """Schedules execution of a bundling task."""
        request = args[0]
        the_id = bundling.compute_bundle_id(
            request, desc.request_discriminator_fields)
        return bundler.schedule(a_func, the_id, desc, request, kwargs)

    return inner


def _page_streamable(a_func,
                     request_page_token_field,
                     response_page_token_field,
                     resource_field):
    """Creates a function that yields an iterable to performs page-streaming.

    Args:
        a_func: an API call that is page streaming.
        request_page_token_field: The field of the page token in the request.
        response_page_token_field: The field of the next page token in the
          response.
        resource_field: The field to be streamed.

    Returns:
        A function that returns an iterable over the specified field.
    """

    def inner(*args, **kwargs):
        """A generator that yields all the paged responses."""
        request = args[0]
        while True:
            response = a_func(request, **kwargs)
            for obj in getattr(response, resource_field):
                yield obj
            next_page_token = getattr(response, response_page_token_field)
            if not next_page_token:
                break
            setattr(request, request_page_token_field, next_page_token)

    return inner


def _construct_bundling(method_config, method_bundling_override):
    """Helper for ``construct_settings()``.

    Args:
      method_config: A dictionary representing a single ``methods`` entry of the
        standard autogenerated API yaml. (See ``construct_settings()`` for
        inforation on this yaml.)
      method_retry_override: A BundleOptions object, OPTION_INHERIT, or None.
        If set to OPTION_INHERIT, the retry settings are derived from method
        config. Otherwise, this parameter overrides ``method_config``.

    Returns:
      A tuple (bundling.Executor, BundleDescriptor) that configures bundling.
      The bundling.Executor may be None if this method should not bundle.
    """
    if ('bundle_options' in method_config and
            'bundle_descriptor' in method_config):

        if method_bundling_override == OPTION_INHERIT:
            bundler = bundling.Executor(BundleOptions(
                **method_config['bundle_options']))
        elif method_bundling_override:
            bundler = bundling.Executor(method_bundling_override)
        else:
            bundler = None

        bundle_descriptor = BundleDescriptor(
            **method_config['bundle_descriptor'])

    else:
        bundle_descriptor = None
        bundler = None
    return bundler, bundle_descriptor


def _construct_retry(
        method_config, method_retry_override, retry_codes_def, retry_params,
        retry_names):
    """Helper for ``construct_settings()``.

    Args:
      method_config: A dictionary representing a single ``methods`` entry of the
        standard autogenerated API yaml. (See ``construct_settings()`` for
        inforation on this yaml.)
      method_retry_override: A RetryOptions object, OPTION_INHERIT, or None.
        If set to OPTION_INHERIT, the retry settings are derived from method
        config. Otherwise, this parameter overrides ``method_config``.
      retry_codes_def: A dictionary parsed from the ``retry_codes_def`` entry
        of the standard autogenerated API yaml. (See ``construct_settings()``
        for information on this yaml.)
      retry_params: A dictionary parsed from the ``retry_params`` entry
        of the standard autogenerated API yaml. (See ``construct_settings()``
        for information on this yaml.)
      retry_names: A dictionary mapping the string names used in the
        standard autogenerated API yaml to API response status codes.

    Returns:
      A RetryOptions object, or None.
    """
    if method_retry_override != OPTION_INHERIT:
        return method_retry_override

    codes = []
    if retry_codes_def:
        for code_set in retry_codes_def:
            if (code_set.get('name') == method_config['retry_codes_name'] and
                    code_set['retry_codes']):
                codes = [
                    retry_names[name] for name in code_set['retry_codes']]
                break

    params_struct = None
    if method_config.get('retry_params_name'):
        for params in retry_params:
            if params.get('name') == method_config['retry_params_name']:
                params_struct = params.copy()
                break
        params_struct.pop('name')
        backoff_settings = BackoffSettings(**params_struct)
    else:
        backoff_settings = None

    retry = RetryOptions(retry_codes=codes, backoff_settings=backoff_settings)
    return retry


def construct_settings(
        configuration, bundling_override, retry_override, retry_names, timeout):
    """Constructs a dictionary mapping method names to CallSettings.

    The ``configuration`` parameter is parsed from a standard autogenerated API
    yaml of the form:

    .. code-block:: yaml

       retry_codes_def:
           - name: idempotent
             retry_codes:
                 - DEADLINE_EXCEEDED
                 - UNAVAILABLE
           - name: non_idempotent
             retry_codes:
       retry_params:
           - name: default
             initial_retry_delay_millis: 100
             retry_delay_multiplier: 1.2
             max_retry_delay_millis: 1000
             initial_rpc_timeout_millis: 300
             rpc_timeout_multiplier: 1.3
             max_rpc_timeout_millis: 3000
             total_timeout_millis: 30000
       methods:
           - name: publish
             retry_codes_name: non_idempotent
             retry_params_name: default
             bundle_options:
                 element_count_threshold: 6
                 delay_threshold_millis: 500
             bundle_descriptor:
                 bundled_field: books
                 discriminator_fields:
                     - edition
                     - shelf.name
           - name: list_shelf
             retry_codes_name: idempotent
             retry_params_name: default
             page_streaming:
                 request:
                     token_field: page_token
                 response:
                     token_field: next_page_token
                     resources_field: books

    Args:
      configuration: A dictionary parsed from the standard autogenerated API yaml.
        method_bundling_override
      bundling_override: A dictionary of method names to BundleOptions that
        override those specified in ``configuration``.
      retry_override: A dictionary of method names to RetryOptions that
        override those specified in ``configuration``.
      retry_names: A dictionary mapping the strings referring to response status
        codes to the Python objects representing those codes.
      timeout: The timeout parameter for all API calls in this dictionary.
    """
    defaults = dict()

    for method_config in configuration.get('methods'):
        method_bundling_override = bundling_override.get(
            method_config['name'], OPTION_INHERIT)
        bundler, bundle_descriptor = _construct_bundling(
            method_config, method_bundling_override)

        method_retry_override = retry_override.get(
            method_config['name'], OPTION_INHERIT)
        retry = _construct_retry(
            method_config, method_retry_override,
            configuration['retry_codes_def'], configuration['retry_params'],
            retry_names)

        if 'page_streaming' in method_config:
            page_descriptor = PageDescriptor(
                method_config['page_streaming']['request']['token_field'],
                method_config['page_streaming']['response']['token_field'],
                method_config['page_streaming']['response']['resources_field'])
        else:
            page_descriptor = None

        defaults[method_config['name']] = CallSettings(
            timeout=timeout, retry=retry, page_descriptor=page_descriptor,
            bundler=bundler, bundle_descriptor=bundle_descriptor)

    return defaults


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

        # Note that the retrying decorator handles timeouts; otherwise, it
        # explicit partial application of the timeout argument is required.
        if self.settings.retry and self.settings.retry.retry_codes:
            the_func = _retryable(the_func, self.settings.retry)
        else:
            the_func = _add_timeout_arg(the_func, self.settings.timeout)

        if self.settings.page_descriptor:
            if self.settings.bundler and self.settings.bundle_descriptor:
                raise ValueError('ApiCallable has incompatible settings: '
                                 'bundling and page streaming')
            the_func = _page_streamable(
                the_func,
                self.settings.page_descriptor.request_page_token_field,
                self.settings.page_descriptor.response_page_token_field,
                self.settings.page_descriptor.resource_field)
        else:
            if self.settings.bundler and self.settings.bundle_descriptor:
                the_func = _bundleable(
                    the_func, self.settings.bundle_descriptor,
                    self.settings.bundler)

        return the_func(*args, **kwargs)
