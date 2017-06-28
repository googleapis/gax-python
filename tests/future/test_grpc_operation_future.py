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

import mock

from google.gax.future import grpc_operation_future
from google.longrunning import operations_pb2
from google.rpc import code_pb2
from google.rpc import status_pb2

from tests.fixtures import fixture_pb2

TEST_OPERATION_NAME = 'test/operation'


class OperationsClient(object):
    def __init__(self, operations):
        self.operations = operations
        self.cancelled = []

    def get_operation(self, name, *args):
        return self.operations.pop(0)

    def cancel_operation(self, name, *args):
        self.cancelled.append(name)


def make_operation(
        name=TEST_OPERATION_NAME, metadata=None, response=None,
        error=None, **kwargs):
    operation = operations_pb2.Operation(
        name=name, **kwargs)

    if metadata is not None:
        operation.metadata.Pack(metadata)

    if response is not None:
        operation.response.Pack(response)

    if error is not None:
        operation.error.CopyFrom(error)

    return operation


def make_operations_client(operations):
    return OperationsClient(operations)


def make_operations_client_and_future(client_operations_responses=None):
    if client_operations_responses is None:
        client_operations_responses = [make_operation()]

    operations_client = make_operations_client(client_operations_responses)
    operations_future = grpc_operation_future.OperationFuture(
        client_operations_responses[0], operations_client,
        result_type=fixture_pb2.Simple,
        metadata_type=fixture_pb2.Simple)

    return operations_client, operations_future


def test_constructor():
    client, future = make_operations_client_and_future()

    assert future.operation == client.operations[0]
    assert future.operation.done is False
    assert future.operation.name == TEST_OPERATION_NAME
    assert future.metadata is None
    assert future.running()


def test_metadata():
    expected_metadata = fixture_pb2.Simple()
    _, future = make_operations_client_and_future(
        [make_operation(metadata=expected_metadata)])

    assert future.metadata == expected_metadata


def test_cancellation():
    responses = [
        make_operation(),
        # Second response indicates that the operation was cancelled.
        make_operation(
            done=True,
            error=status_pb2.Status(code=code_pb2.CANCELLED))]
    client, future = make_operations_client_and_future(responses)

    assert future.cancel()
    assert future.cancelled()
    assert future.operation.name in client.cancelled
    assert not future.cancel(), 'cancelling twice should have no effect.'


@mock.patch('time.sleep')
def test_result(unused_sleep):
    expected_result = fixture_pb2.Simple()
    responses = [
        make_operation(),
        # Second operation response includes the result.
        make_operation(done=True, response=expected_result)]
    _, future = make_operations_client_and_future(responses)

    result = future.result()

    assert result == expected_result


@mock.patch('time.sleep')
def test_exception(unused_sleep):
    expected_exception = status_pb2.Status(message='meep')
    responses = [
        make_operation(),
        # Second operation response includes the error.
        make_operation(done=True, error=expected_exception)]
    _, future = make_operations_client_and_future(responses)

    exception = future.exception()

    assert expected_exception.message in '{!r}'.format(exception)


@mock.patch('time.sleep')
def test_unexpected_result(unused_sleep):
    responses = [
        make_operation(),
        # Second operation response is done, but has not error or response.
        make_operation(done=True)]
    _, future = make_operations_client_and_future(responses)

    exception = future.exception()

    assert 'Unknown operation error' in '{!r}'.format(exception)
