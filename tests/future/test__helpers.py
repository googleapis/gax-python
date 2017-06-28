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
import pytest

from google.gax import errors
from google.gax.future import _helpers
from google.protobuf import any_pb2

from tests.fixtures import fixture_pb2


def test_from_any():
    in_message = fixture_pb2.Simple(field1='a')
    in_message_any = any_pb2.Any()
    in_message_any.Pack(in_message)
    out_message = _helpers.from_any(fixture_pb2.Simple, in_message_any)
    assert in_message == out_message


def test_from_any_wrong_type():
    in_message = any_pb2.Any()
    in_message.Pack(fixture_pb2.Simple(field1='a'))
    with pytest.raises(TypeError):
        _helpers.from_any(fixture_pb2.Outer, in_message)


@mock.patch('threading.Thread', autospec=True)
def test_start_deamon_thread(mock_thread):
    thread = _helpers.start_daemon_thread(target=mock.sentinel.target)
    assert thread.daemon is True


@mock.patch('time.sleep')
def test_blocking_poll(unused_sleep):
    error = errors.TimeoutError()
    target = mock.Mock(side_effect=[error, error, 42])

    result = _helpers.blocking_poll(target, timeout=1)

    assert result == 42
    assert target.call_count == 3
