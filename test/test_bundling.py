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

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name
"""Unit tests for bundling."""

from __future__ import absolute_import

import mock
import unittest2

from google.gax import bundling, BundleOptions


# pylint: disable=too-few-public-methods
class _Simple(object):
    def __init__(self, value, other_value=None):
        self.field1 = value
        self.field2 = other_value

    def __eq__(self, other):
        return (self.field1 == other.field1 and
                self.field2 == other.field2)

    def __str__(self):
        return "field1={0}, field2={1}".format(self.field1, self.field2)


class _Outer(object):
    def __init__(self, value):
        self.inner = _Simple(value, other_value=value)
        self.field1 = value


class TestComputeBundleId(unittest2.TestCase):

    def test_computes_bundle_ids_ok(self):
        tests = [
            {
                'message': 'single field value',
                'object': _Simple('dummy_value'),
                'fields': ['field1'],
                'want': ('dummy_value',)
            }, {
                'message': 'composite value with None',
                'object': _Simple('dummy_value'),
                'fields': ['field1', 'field2'],
                'want': ('dummy_value', None)
            }, {
                'message': 'a composite value',
                'object': _Simple('dummy_value', 'other_value'),
                'fields': ['field1', 'field2'],
                'want': ('dummy_value', 'other_value')
            }, {
                'message': 'a simple dotted value',
                'object': _Outer('this is dotty'),
                'fields': ['inner.field1'],
                'want': ('this is dotty',)
            }, {
                'message': 'a complex case',
                'object': _Outer('what!?'),
                'fields': ['inner.field1', 'inner.field2', 'field1'],
                'want': ('what!?', 'what!?', 'what!?')
            }
        ]
        for t in tests:
            got = bundling.compute_bundle_id(t['object'], t['fields'])
            message = 'failed while making an id for {}'.format(t['message'])
            self.assertEquals(got, t['want'], message)

    def test_should_raise_if_fields_are_missing(self):
        tests = [
            {
                'object': _Simple('dummy_value'),
                'fields': ['field3'],
            }, {
                'object': _Simple('dummy_value'),
                'fields': ['field1', 'field3'],
            }, {
                'object': _Simple('dummy_value', 'other_value'),
                'fields': ['field1', 'field3'],
            }, {
                'object': _Outer('this is dotty'),
                'fields': ['inner.field3'],
            }, {
                'object': _Outer('what!?'),
                'fields': ['inner.field4'],
            }
        ]
        for t in tests:
            self.assertRaises(AttributeError,
                              bundling.compute_bundle_id,
                              t['object'],
                              t['fields'])


def _return_request(req):
    """A dummy api call that simply returns the request."""
    return req


def _make_a_test_task(api_call=_return_request):
    return bundling.Task(
        api_call,
        'an_id',
        'field1',
        _Simple('dummy_value'))


def _append_msg_n_times(a_task, msg, n):
    for _ in range(n):
        a_task.append(msg)


def _raise_exc(dummy_req):
    """A dummy api call that raises an exception"""
    raise ValueError('Raised in a test')


class TestTask(unittest2.TestCase):

    def test_append_increases_the_message_count(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'want': 0
            }, {
                'update': (lambda t: t.append(simple_msg)),
                'message': 'a single message added',
                'want': 1
            }, {
                'update': (lambda t: _append_msg_n_times(t, simple_msg, 5)),
                'message': 'a 5 messages added',
                'want': 5
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            t['update'](test_task)
            got = test_task.message_count
            message = 'bad message count when {}'.format(t['message'])
            self.assertEquals(got, t['want'], message)

    def test_append_increases_the_message_bytesize(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'want': 0
            }, {
                'update': (lambda t: t.append(simple_msg)),
                'message': 'a single bundle message',
                'want': len(simple_msg)
            }, {
                'update': (lambda t: _append_msg_n_times(t, simple_msg, 5)),
                'message': '5 bundled messages',
                'want': 5 * len(simple_msg)
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            t['update'](test_task)
            got = test_task.message_bytesize
            message = 'bad message count when {}'.format(t['message'])
            self.assertEquals(got, t['want'], message)

    def test_run_sends_the_bundle_messages(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'count_before_run': 0,
                'want': []
            }, {
                'update': (lambda t: t.append(simple_msg)),
                'message': 'a single bundled message',
                'count_before_run': 1,
                'want': [_Simple([simple_msg])]
            }, {
                'update': (lambda t: _append_msg_n_times(t, simple_msg, 5)),
                'message': '5 bundle messages',
                'count_before_run': 5,
                'want': [_Simple([simple_msg] * 5)]
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            t['update'](test_task)
            self.assertEquals(test_task.message_count, t['count_before_run'])
            test_task.run()
            got = list(test_task.out_deque)
            message = 'bad output when run with {}'.format(t['message'])
            self.assertEquals(got, t['want'], message)
            self.assertEquals(test_task.message_count, 0)
            self.assertEquals(test_task.message_bytesize, 0)

    def test_run_adds_an_error_if_execution_fails(self):
        simple_msg = 'a simple msg'
        test_task = _make_a_test_task(api_call=_raise_exc)
        test_task.append(simple_msg)
        self.assertEquals(test_task.message_count, 1)
        test_task.run()
        self.assertEquals(test_task.message_count, 0)
        self.assertEquals(test_task.message_bytesize, 0)
        self.assertEquals(len(test_task.out_deque), 1)
        self.assertTrue(isinstance(test_task.out_deque[0], ValueError))

    def test_calling_the_canceller_stops_the_message_from_getting_sent(self):
        a_msg = 'a simple msg'
        another_msg = 'another msg'
        test_task = _make_a_test_task()
        test_task.append(a_msg)
        test_task.append(another_msg)
        self.assertEquals(test_task.message_count, 2)
        canceller = test_task.canceller_for(a_msg)
        self.assertTrue(canceller())
        self.assertEquals(test_task.message_count, 1)
        self.assertFalse(canceller())
        self.assertEquals(test_task.message_count, 1)
        test_task.run()
        self.assertEquals(test_task.message_count, 0)
        self.assertEquals([_Simple([another_msg])], list(test_task.out_deque))


class TestExecutor(unittest2.TestCase):

    def test_schedule_executes_immediately_with_noarg_options(self):
        a_msg = 'dummy message'
        an_id = 'bundle_id'
        bundler = bundling.Executor(BundleOptions())
        got_queue, got_canceller = bundler.schedule(
            _return_request,
            an_id,
            'field1',
            _Simple(a_msg)
        )
        self.assertIsNone(got_canceller)
        self.assertEquals([_Simple([a_msg])], list(got_queue))

    def test_api_calls_grouped_by_bundle_id(self):
        a_msg = 'dummy message'
        api_call = _return_request
        bundle_ids = ['id1', 'id2']
        threshold = 5  # arbitrary
        options = BundleOptions(message_count_threshold=threshold)
        bundler = bundling.Executor(options)
        for an_id in bundle_ids:
            current_queue = None
            for i in range(threshold - 1):
                got_queue, got_canceller = bundler.schedule(
                    api_call,
                    an_id,
                    'field1',
                    _Simple(a_msg)
                )
                if current_queue is None:
                    current_queue = got_queue
                else:
                    self.assertEquals(current_queue, got_queue)
                self.assertIsNotNone(
                    got_canceller,
                    'missing canceller after message #{}'.format(i))
                self.assertEquals([], list(got_queue))
        for an_id in bundle_ids:
            got_queue, got_canceller = bundler.schedule(
                api_call,
                an_id,
                'field1',
                _Simple(a_msg)
            )
            self.assertIsNone(got_canceller, 'expected None as canceller')
            self.assertEquals([_Simple([a_msg] * threshold)], list(got_queue))


class TestExecutor_MessageCountTrigger(unittest2.TestCase):

    def test_api_call_not_invoked_until_threshold(self):
        a_msg = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        threshold = 3  # arbitrary
        options = BundleOptions(message_count_threshold=threshold)
        bundler = bundling.Executor(options)
        for i in range(threshold):
            got_queue, got_canceller = bundler.schedule(
                api_call,
                an_id,
                'field1',
                _Simple(a_msg)
            )
            if i + 1 < threshold:
                self.assertIsNotNone(
                    got_canceller,
                    'missing canceller after message #{}'.format(i))
                self.assertEquals([], list(got_queue))
            else:
                self.assertIsNone(
                    got_canceller,
                    'expected None as canceller after message #{}'.format(i)
                )
                self.assertEquals([_Simple([a_msg] * threshold)],
                                  list(got_queue))


class TestExecutor_MessageByteSizeTrigger(unittest2.TestCase):

    def test_api_call_not_invoked_until_threshold(self):
        a_msg = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        msgs_for_threshold = 3
        threshold = msgs_for_threshold * len(a_msg)  # arbitrary
        options = BundleOptions(message_count_threshold=0,
                                message_bytesize_threshold=threshold)
        bundler = bundling.Executor(options)
        for i in range(msgs_for_threshold):
            got_queue, got_canceller = bundler.schedule(
                api_call,
                an_id,
                'field1',
                _Simple(a_msg)
            )
            if i + 1 < msgs_for_threshold:
                self.assertIsNotNone(
                    got_canceller,
                    'missing canceller after message #{}'.format(i))
                self.assertEquals([], list(got_queue))
            else:
                self.assertIsNone(
                    got_canceller,
                    'expected None as canceller after message #{}'.format(i)
                )
                self.assertEquals([_Simple([a_msg] * msgs_for_threshold)],
                                  list(got_queue))


class TestExecutor_DelayThreshold(unittest2.TestCase):

    @mock.patch('google.gax.bundling.TIMER_FACTORY')
    def test_api_call_is_scheduled_on_timer(self, timer_class):
        a_msg = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        delay_threshold = 3
        options = BundleOptions(message_count_threshold=0,
                                delay_threshold=delay_threshold)
        bundler = bundling.Executor(options)
        got_queue, got_canceller = bundler.schedule(
            api_call,
            an_id,
            'field1',
            _Simple(a_msg)
        )
        self.assertIsNotNone(got_canceller, 'missing canceller after first msg')
        self.assertEquals([], list(got_queue))
        self.assertTrue(timer_class.called)
        timer_args, timer_kwargs = timer_class.call_args_list[0]
        self.assertEquals(delay_threshold, timer_args[0])
        self.assertEquals({'args': [an_id]}, timer_kwargs)
        timer_class.return_value.start.assert_called_once_with()
