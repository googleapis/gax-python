# Copyright 2017, Google Inc. All rights reserved.
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
#
# EDITING INSTRUCTIONS
# This file was generated from the file
# https://github.com/google/googleapis/blob/master/google/longrunning/operations.proto,
# and updates to that file get reflected here through a refresh process.
# For the short term, the refresh process will only be runnable by Google engineers.
#
# The only allowed edits are to method and file documentation. A 3-way
# merge preserves those additions if the generated source changes.
"""Accesses the google.longrunning Operations API."""

import collections
import json
import os
import pkg_resources
import platform

from google.gax import api_callable
from google.gax import config
from google.gax import path_template
import google.gax

from google.longrunning import operations_pb2

_PageDesc = google.gax.PageDescriptor


class OperationsClient(object):
    """
    Manages long-running operations with an API service.

    When an API method normally takes long time to complete, it can be designed
    to return ``Operation`` to the client, and the client can use this
    interface to receive the real response asynchronously by polling the
    operation resource, or pass the operation resource to another API (such as
    Google Cloud Pub/Sub API) to receive the response.  Any API service that
    returns long-running operations should implement the ``Operations`` interface
    so developers can have a consistent client experience.
    """

    SERVICE_ADDRESS = 'longrunning.googleapis.com'
    """The default address of the service."""

    DEFAULT_SERVICE_PORT = 443
    """The default port of the service."""

    _PAGE_DESCRIPTORS = {
        'list_operations': _PageDesc('page_token', 'next_page_token',
                                     'operations')
    }

    # The scopes needed to make gRPC calls to all of the methods defined in
    # this service
    _ALL_SCOPES = ()

    def __init__(self,
                 service_path=SERVICE_ADDRESS,
                 port=DEFAULT_SERVICE_PORT,
                 channel=None,
                 credentials=None,
                 ssl_credentials=None,
                 scopes=None,
                 client_config=None,
                 app_name=None,
                 app_version='',
                 lib_name=None,
                 lib_version='',
                 metrics_headers=()):
        """Constructor.

        Args:
          service_path (string): The domain name of the API remote host.
          port (int): The port on which to connect to the remote host.
          channel (:class:`grpc.Channel`): A ``Channel`` instance through
            which to make calls.
          credentials (object): The authorization credentials to attach to
            requests. These credentials identify this application to the
            service.
          ssl_credentials (:class:`grpc.ChannelCredentials`): A
            ``ChannelCredentials`` instance for use with an SSL-enabled
            channel.
          scopes (list[string]): A list of OAuth2 scopes to attach to requests.
          client_config (dict):
            A dictionary for call options for each method. See
            :func:`google.gax.construct_settings` for the structure of
            this data. Falls back to the default config if not specified
            or the specified config is missing data points.
          app_name (string): The name of the application calling
            the service. Recommended for analytics purposes.
          app_version (string): The version of the application calling
            the service. Recommended for analytics purposes.
          lib_name (string): The API library software used for calling
            the service. (Unless you are writing an API client itself,
            leave this as default.)
          lib_version (string): The API library software version used
            for calling the service. (Unless you are writing an API client
            itself, leave this as default.)
          metrics_headers (dict): A dictionary of values for tracking
            client library metrics. Ultimately serializes to a string
            (e.g. 'foo/1.2.3 bar/3.14.1'). This argument should be
            considered private.

        Returns:
          A OperationsClient object.
        """
        # Unless the calling application specifically requested
        # OAuth scopes, request everything.
        if scopes is None:
            scopes = self._ALL_SCOPES

        # Initialize an empty client config, if none is set.
        if client_config is None:
            client_config = {}

        # Initialize metrics_headers as an ordered dictionary
        # (cuts down on cardinality of the resulting string slightly).
        metrics_headers = collections.OrderedDict(metrics_headers)
        metrics_headers['gl-python'] = platform.python_version()

        # The library may or may not be set, depending on what is
        # calling this client. Newer client libraries set the library name
        # and version.
        if lib_name:
            metrics_headers[lib_name] = lib_version

        # Load the configuration defaults.
        default_client_config = json.loads(
            pkg_resources.resource_string(
                __name__, 'operations_client_config.json').decode())
        defaults = api_callable.construct_settings(
            'google.longrunning.Operations',
            default_client_config,
            client_config,
            config.STATUS_CODE_NAMES,
            metrics_headers=metrics_headers,
            page_descriptors=self._PAGE_DESCRIPTORS, )
        self.operations_stub = config.create_stub(
            operations_pb2.OperationsStub,
            channel=channel,
            service_path=service_path,
            service_port=port,
            credentials=credentials,
            scopes=scopes,
            ssl_credentials=ssl_credentials)

        self._get_operation = api_callable.create_api_call(
            self.operations_stub.GetOperation,
            settings=defaults['get_operation'])
        self._list_operations = api_callable.create_api_call(
            self.operations_stub.ListOperations,
            settings=defaults['list_operations'])
        self._cancel_operation = api_callable.create_api_call(
            self.operations_stub.CancelOperation,
            settings=defaults['cancel_operation'])
        self._delete_operation = api_callable.create_api_call(
            self.operations_stub.DeleteOperation,
            settings=defaults['delete_operation'])

    # Service calls
    def get_operation(self, name, options=None):
        """
        Gets the latest state of a long-running operation.  Clients can use this
        method to poll the operation result at intervals as recommended by the API
        service.

        Example:
          >>> from google.gapic.longrunning import operations_client
          >>> api = operations_client.OperationsClient()
          >>> name = ''
          >>> response = api.get_operation(name)

        Args:
          name (string): The name of the operation resource.
          options (:class:`google.gax.CallOptions`): Overrides the default
            settings for this call, e.g, timeout, retries etc.

        Returns:
          A :class:`google.longrunning.operations_pb2.Operation` instance.

        Raises:
          :exc:`google.gax.errors.GaxError` if the RPC is aborted.
          :exc:`ValueError` if the parameters are invalid.
        """
        # Create the request object.
        request = operations_pb2.GetOperationRequest(name=name)
        return self._get_operation(request, options)

    def list_operations(self, name, filter_, page_size=0, options=None):
        """
        Lists operations that match the specified filter in the request. If the
        server doesn't support this method, it returns ``UNIMPLEMENTED``.
        NOTE: the ``name`` binding below allows API services to override the binding
        to use different resource name schemes, such as ``users/*/operations``.

        Example:
          >>> from google.gapic.longrunning import operations_client
          >>> from google.gax import CallOptions, INITIAL_PAGE
          >>> api = operations_client.OperationsClient()
          >>> name = ''
          >>> filter_ = ''
          >>>
          >>> # Iterate over all results
          >>> for element in api.list_operations(name, filter_):
          >>>   # process element
          >>>   pass
          >>>
          >>> # Or iterate over results one page at a time
          >>> for page in api.list_operations(name, filter_, options=CallOptions(page_token=INITIAL_PAGE)):
          >>>   for element in page:
          >>>     # process element
          >>>     pass

        Args:
          name (string): The name of the operation collection.
          filter_ (string): The standard list filter.
          page_size (int): The maximum number of resources contained in the
            underlying API response. If page streaming is performed per-
            resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number
            of resources in a page.
          options (:class:`google.gax.CallOptions`): Overrides the default
            settings for this call, e.g, timeout, retries etc.

        Returns:
          A :class:`google.gax.PageIterator` instance. By default, this
          is an iterable of :class:`google.longrunning.operations_pb2.Operation` instances.
          This object can also be configured to iterate over the pages
          of the response through the `CallOptions` parameter.

        Raises:
          :exc:`google.gax.errors.GaxError` if the RPC is aborted.
          :exc:`ValueError` if the parameters are invalid.
        """
        # Create the request object.
        request = operations_pb2.ListOperationsRequest(
            name=name, filter=filter_, page_size=page_size)
        return self._list_operations(request, options)

    def cancel_operation(self, name, options=None):
        """
        Starts asynchronous cancellation on a long-running operation.  The server
        makes a best effort to cancel the operation, but success is not
        guaranteed.  If the server doesn't support this method, it returns
        ``google.rpc.Code.UNIMPLEMENTED``.  Clients can use
        ``Operations.GetOperation`` or
        other methods to check whether the cancellation succeeded or whether the
        operation completed despite cancellation. On successful cancellation,
        the operation is not deleted; instead, it becomes an operation with
        an ``Operation.error`` value with a ``google.rpc.Status.code`` of 1,
        corresponding to ``Code.CANCELLED``.

        Example:
          >>> from google.gapic.longrunning import operations_client
          >>> api = operations_client.OperationsClient()
          >>> name = ''
          >>> api.cancel_operation(name)

        Args:
          name (string): The name of the operation resource to be cancelled.
          options (:class:`google.gax.CallOptions`): Overrides the default
            settings for this call, e.g, timeout, retries etc.

        Raises:
          :exc:`google.gax.errors.GaxError` if the RPC is aborted.
          :exc:`ValueError` if the parameters are invalid.
        """
        # Create the request object.
        request = operations_pb2.CancelOperationRequest(name=name)
        self._cancel_operation(request, options)

    def delete_operation(self, name, options=None):
        """
        Deletes a long-running operation. This method indicates that the client is
        no longer interested in the operation result. It does not cancel the
        operation. If the server doesn't support this method, it returns
        ``google.rpc.Code.UNIMPLEMENTED``.

        Example:
          >>> from google.gapic.longrunning import operations_client
          >>> api = operations_client.OperationsClient()
          >>> name = ''
          >>> api.delete_operation(name)

        Args:
          name (string): The name of the operation resource to be deleted.
          options (:class:`google.gax.CallOptions`): Overrides the default
            settings for this call, e.g, timeout, retries etc.

        Raises:
          :exc:`google.gax.errors.GaxError` if the RPC is aborted.
          :exc:`ValueError` if the parameters are invalid.
        """
        # Create the request object.
        request = operations_pb2.DeleteOperationRequest(name=name)
        self._delete_operation(request, options)
