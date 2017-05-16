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

import unittest

import pytest

from google.api import http_pb2
from google.gax.utils import protobuf
from google.longrunning import operations_proto_pb2 as ops


class GetTests(unittest.TestCase):
    def test_get_dict_sentinel(self):
        with pytest.raises(KeyError):
            assert protobuf.get({}, 'foo')

    def test_get_dict_present(self):
        assert protobuf.get({'foo': 'bar'}, 'foo') == 'bar'

    def test_get_dict_default(self):
        assert protobuf.get({}, 'foo', default='bar') == 'bar'

    def test_get_dict_nested(self):
        assert protobuf.get({'foo': {'bar': 'baz'}}, 'foo.bar') == 'baz'

    def test_get_dict_nested_default(self):
        assert protobuf.get({}, 'foo.baz', default='bacon') == 'bacon'
        assert protobuf.get({'foo': {}}, 'foo.baz', default='bacon') == 'bacon'

    def test_get_pb2_sentinel(self):
        operation = ops.Operation()
        with pytest.raises(KeyError):
            assert protobuf.get(operation, 'foo')

    def test_get_pb2_present(self):
        operation = ops.Operation(name='foo')
        assert protobuf.get(operation, 'name') == 'foo'

    def test_get_pb2_default(self):
        operation = ops.Operation()
        assert protobuf.get(operation, 'foo', default='bar') == 'bar'

    def test_invalid_object(self):
        obj = object()
        with pytest.raises(TypeError):
            protobuf.get(obj, 'foo', 'bar')


class SetTests(unittest.TestCase):
    def test_set_dict(self):
        mapping = {}
        protobuf.set(mapping, 'foo', 'bar')
        assert mapping == {'foo': 'bar'}

    def test_set_pb2(self):
        operation = ops.Operation()
        protobuf.set(operation, 'name', 'foo')
        assert operation.name == 'foo'

    def test_set_nested(self):
        mapping = {}
        protobuf.set(mapping, 'foo.bar', 'baz')
        assert mapping == {'foo': {'bar': 'baz'}}

    def test_invalid_object(self):
        obj = object()
        with pytest.raises(TypeError):
            protobuf.set(obj, 'foo', 'bar')

    def test_set_list(self):
        list_ops_response = ops.ListOperationsResponse()
        protobuf.set(list_ops_response, 'operations', [
            {'name': 'foo'},
            ops.Operation(name='bar'),
        ])
        assert len(list_ops_response.operations) == 2
        for operation in list_ops_response.operations:
            assert isinstance(operation, ops.Operation)
        assert list_ops_response.operations[0].name == 'foo'
        assert list_ops_response.operations[1].name == 'bar'

    def test_set_list_clear_existing(self):
        list_ops_response = ops.ListOperationsResponse(
            operations=[{'name': 'baz'}],
        )
        protobuf.set(list_ops_response, 'operations', [
            {'name': 'foo'},
            ops.Operation(name='bar'),
        ])
        assert len(list_ops_response.operations) == 2
        for operation in list_ops_response.operations:
            assert isinstance(operation, ops.Operation)
        assert list_ops_response.operations[0].name == 'foo'
        assert list_ops_response.operations[1].name == 'bar'

    def test_set_dict_nested_with_message(self):
        rule = http_pb2.HttpRule()
        pattern = http_pb2.CustomHttpPattern(kind='foo', path='bar')
        protobuf.set(rule, 'custom', pattern)
        assert rule.custom.kind == 'foo'
        assert rule.custom.path == 'bar'

    def test_set_dict_nested_with_dict(self):
        rule = http_pb2.HttpRule()
        pattern = {'kind': 'foo', 'path': 'bar'}
        protobuf.set(rule, 'custom', pattern)
        assert rule.custom.kind == 'foo'
        assert rule.custom.path == 'bar'


class SetDefaultTests(unittest.TestCase):
    def test_dict_unset(self):
        mapping = {}
        protobuf.setdefault(mapping, 'foo', 'bar')
        assert mapping == {'foo': 'bar'}

    def test_dict_falsy(self):
        mapping = {'foo': None}
        protobuf.setdefault(mapping, 'foo', 'bar')
        assert mapping == {'foo': 'bar'}

    def test_dict_truthy(self):
        mapping = {'foo': 'bar'}
        protobuf.setdefault(mapping, 'foo', 'baz')
        assert mapping == {'foo': 'bar'}

    def test_pb2_falsy(self):
        operation = ops.Operation()
        protobuf.setdefault(operation, 'name', 'foo')
        assert operation.name == 'foo'

    def test_pb2_truthy(self):
        operation = ops.Operation(name='bar')
        protobuf.setdefault(operation, 'name', 'foo')
        assert operation.name == 'bar'
