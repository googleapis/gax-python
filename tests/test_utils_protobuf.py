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

    def test_get_pb2_sentinel(self):
        op = ops.Operation()
        with pytest.raises(KeyError):
            assert protobuf.get(op, 'foo')

    def test_get_pb2_present(self):
        op = ops.Operation(name='foo')
        assert protobuf.get(op, 'name') == 'foo'

    def test_get_pb2_default(self):
        op = ops.Operation()
        assert protobuf.get(op, 'foo', default='bar') == 'bar'

    def test_invalid_object(self):
        obj = object()
        with pytest.raises(TypeError):
            protobuf.get(obj, 'foo', 'bar')


class SetTests(unittest.TestCase):
    def test_set_dict(self):
        d = {}
        protobuf.set(d, 'foo', 'bar')
        assert d == {'foo': 'bar'}

    def test_set_pb2(self):
        op = ops.Operation()
        protobuf.set(op, 'name', 'foo')
        assert op.name == 'foo'

    def test_set_nested(self):
        d = {}
        protobuf.set(d, 'foo.bar', 'baz')
        assert d == {'foo': {'bar': 'baz'}}

    def test_invalid_object(self):
        obj = object()
        with pytest.raises(TypeError):
            protobuf.set(obj, 'foo', 'bar')


class SetDefaultTests(unittest.TestCase):
    def test_dict_unset(self):
        d = {}
        protobuf.setdefault(d, 'foo', 'bar')
        assert d == {'foo': 'bar'}

    def test_dict_falsy(self):
        d = {'foo': None}
        protobuf.setdefault(d, 'foo', 'bar')
        assert d == {'foo': 'bar'}

    def test_dict_truthy(self):
        d = {'foo': 'bar'}
        protobuf.setdefault(d, 'foo', 'baz')
        assert d == {'foo': 'bar'}

    def test_pb2_falsy(self):
        op = ops.Operation()
        protobuf.setdefault(op, 'name', 'foo')
        assert op.name == 'foo'

    def test_pb2_truthy(self):
        op = ops.Operation(name='bar')
        protobuf.setdefault(op, 'name', 'foo')
        assert op.name == 'bar'
