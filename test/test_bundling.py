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

from google.gax import bundling, BundleOptions, BundleDescriptor

from fixture_pb2 import Simple, Outer, Bundled


def _Simple(value, other_value=None):
    if other_value is None:
        return Simple(field1=value)
    else:
        return Simple(field1=value, field2=other_value)


def _Outer(value):
    return Outer(inner=_Simple(value, value), field1=value)


def _Bundled(value):
    return Bundled(field1=value)


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
            self.assertEqual(got, t['want'], message)

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


def _return_kwargs(dummy_req, **kwargs):
    """A dummy api call that simply returns its keyword arguments."""
    return kwargs


def _make_a_test_task(api_call=_return_request):
    return bundling.Task(
        api_call,
        'an_id',
        'field1',
        _Bundled([]),
        dict())


def _extend_with_n_elts(a_task, elt, n):
    return a_task.extend([elt] * n)


def _raise_exc(dummy_req):
    """A dummy api call that raises an exception"""
    raise ValueError('Raised in a test')


class TestTask(unittest2.TestCase):

    def test_extend_increases_the_element_count(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'want': 0
            }, {
                'update': (lambda t: t.extend([simple_msg])),
                'message': 'a single message added',
                'want': 1
            }, {
                'update': (lambda t: _extend_with_n_elts(t, simple_msg, 5)),
                'message': 'a 5 messages added',
                'want': 5
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            t['update'](test_task)
            got = test_task.element_count
            message = 'bad message count when {}'.format(t['message'])
            self.assertEqual(got, t['want'], message)

    def test_extend_increases_the_request_byte_count(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'want': 0
            }, {
                'update': (lambda t: t.extend([simple_msg])),
                'message': 'a single bundle message',
                'want': len(simple_msg)
            }, {
                'update': (lambda t: _extend_with_n_elts(t, simple_msg, 5)),
                'message': '5 bundled messages',
                'want': 5 * len(simple_msg)
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            t['update'](test_task)
            got = test_task.request_bytesize
            message = 'bad message count when {}'.format(t['message'])
            self.assertEqual(got, t['want'], message)

    def test_run_sends_the_bundle_elements(self):
        simple_msg = 'a simple msg'
        tests = [
            {
                'update': (lambda t: None),
                'message': 'no messages added',
                'has_event': False,
                'count_before_run': 0,
                'want': []
            }, {
                'update': (lambda t: t.extend([simple_msg])),
                'message': 'a single bundled message',
                'has_event': True,
                'count_before_run': 1,
                'want': _Bundled([simple_msg])
            }, {
                'update': (lambda t: _extend_with_n_elts(t, simple_msg, 5)),
                'message': '5 bundle messages',
                'has_event': True,
                'count_before_run': 5,
                'want': _Bundled([simple_msg] * 5)
            }
        ]
        for t in tests:
            test_task = _make_a_test_task()
            event = t['update'](test_task)
            self.assertEqual(test_task.element_count, t['count_before_run'])
            test_task.run()
            self.assertEqual(test_task.element_count, 0)
            self.assertEqual(test_task.request_bytesize, 0)
            if t['has_event']:
                self.assertIsNotNone(
                    event,
                    'expected event for {}'.format(t['message']))
                got = event.result
                message = 'bad output when run with {}'.format(t['message'])
                self.assertEqual(got, t['want'], message)

    def test_run_adds_an_error_if_execution_fails(self):
        simple_msg = 'a simple msg'
        test_task = _make_a_test_task(api_call=_raise_exc)
        event = test_task.extend([simple_msg])
        self.assertEqual(test_task.element_count, 1)
        test_task.run()
        self.assertEqual(test_task.element_count, 0)
        self.assertEqual(test_task.request_bytesize, 0)
        self.assertTrue(isinstance(event.result, ValueError))

    def test_calling_the_canceller_stops_the_element_from_getting_sent(self):
        an_elt = 'a simple msg'
        another_msg = 'another msg'
        test_task = _make_a_test_task()
        an_event = test_task.extend([an_elt])
        another_event = test_task.extend([another_msg])
        self.assertEqual(test_task.element_count, 2)
        self.assertTrue(an_event.cancel())
        self.assertEqual(test_task.element_count, 1)
        self.assertFalse(an_event.cancel())
        self.assertEqual(test_task.element_count, 1)
        test_task.run()
        self.assertEqual(test_task.element_count, 0)
        self.assertEqual(_Bundled([another_msg]), another_event.result)
        self.assertFalse(an_event.is_set())
        self.assertIsNone(an_event.result)


SIMPLE_DESCRIPTOR = BundleDescriptor('field1', [])
DEMUX_DESCRIPTOR = BundleDescriptor('field1', [], subresponse_field='field1')


class TestExecutor(unittest2.TestCase):

    def test_api_calls_are_grouped_by_bundle_id(self):
        an_elt = 'dummy message'
        api_call = _return_request
        bundle_ids = ['id1', 'id2']
        threshold = 5  # arbitrary
        options = BundleOptions(element_count_threshold=threshold)
        bundler = bundling.Executor(options)
        for an_id in bundle_ids:
            for i in range(threshold - 1):
                got_event = bundler.schedule(
                    api_call,
                    an_id,
                    SIMPLE_DESCRIPTOR,
                    _Bundled([an_elt])
                )
                self.assertIsNotNone(
                    got_event.canceller,
                    'missing canceller after element #{}'.format(i))
                self.assertFalse(
                    got_event.is_set(),
                    'event unexpectedly set after element #{}'.format(i))
                self.assertIsNone(got_event.result)
        for an_id in bundle_ids:
            got_event = bundler.schedule(
                api_call,
                an_id,
                SIMPLE_DESCRIPTOR,
                _Bundled([an_elt])
            )
            self.assertIsNotNone(got_event.canceller,
                                 'missing expected canceller')
            self.assertTrue(
                got_event.is_set(),
                'event is not set after triggering element')
            self.assertEqual(_Bundled([an_elt] * threshold),
                             got_event.result)

    def test_each_event_has_exception_when_demuxed_api_call_fails(self):
        an_elt = 'dummy message'
        api_call = _raise_exc
        bundle_id = 'an_id'
        threshold = 5  # arbitrary, greater than 1
        options = BundleOptions(element_count_threshold=threshold)
        bundler = bundling.Executor(options)
        events = []
        for i in range(threshold - 1):
            got_event = bundler.schedule(
                api_call,
                bundle_id,
                DEMUX_DESCRIPTOR,
                _Bundled(['%s%d' % (an_elt, i)])
            )
            self.assertFalse(
                got_event.is_set(),
                'event unexpectedly set after element #{}'.format(i))
            self.assertIsNone(got_event.result)
            events.append(got_event)
        last_event = bundler.schedule(
            api_call,
            bundle_id,
            DEMUX_DESCRIPTOR,
            _Bundled(['%s%d' % (an_elt, threshold - 1)])
        )
        events.append(last_event)

        previous_event = None
        for event in events:
            if previous_event:
                self.assertTrue(previous_event != event)
            self.assertTrue(event.is_set(),
                            'event is not set after triggering element')
            self.assertTrue(isinstance(event.result, ValueError))
            previous_event = event

    def test_each_event_has_its_result_from_a_demuxed_api_call(self):
        an_elt = 'dummy message'
        api_call = _return_request
        bundle_id = 'an_id'
        threshold = 5  # arbitrary, greater than 1
        options = BundleOptions(element_count_threshold=threshold)
        bundler = bundling.Executor(options)
        events = []

        # send 3 groups of elements of different sizes in the bundle
        for i in range(1, 4):
            got_event = bundler.schedule(
                api_call,
                bundle_id,
                DEMUX_DESCRIPTOR,
                _Bundled(['%s%d' % (an_elt, i)] * i)
            )
            events.append(got_event)
        previous_event = None
        for i, event in enumerate(events):
            index = i + 1
            if previous_event:
                self.assertTrue(previous_event != event)
            self.assertTrue(event.is_set(),
                            'event is not set after triggering element')
            self.assertEqual(event.result,
                             _Bundled(['%s%d' % (an_elt, index)] * index))
            previous_event = event

    def test_each_event_has_same_result_from_mismatched_demuxed_api_call(self):
        an_elt = 'dummy message'
        mismatched_result = _Bundled([an_elt, an_elt])
        bundle_id = 'an_id'
        threshold = 5  # arbitrary, greater than 1
        options = BundleOptions(element_count_threshold=threshold)
        bundler = bundling.Executor(options)
        events = []

        # send 3 groups of elements of different sizes in the bundle
        for i in range(1, 4):
            got_event = bundler.schedule(
                lambda x: mismatched_result,
                bundle_id,
                DEMUX_DESCRIPTOR,
                _Bundled(['%s%d' % (an_elt, i)] * i)
            )
            events.append(got_event)
        previous_event = None
        for i, event in enumerate(events):
            if previous_event:
                self.assertTrue(previous_event != event)
            self.assertTrue(event.is_set(),
                            'event is not set after triggering element')
            self.assertEqual(event.result, mismatched_result)
            previous_event = event

    def test_schedule_passes_kwargs(self):
        an_elt = 'dummy_msg'
        options = BundleOptions(element_count_threshold=1)
        bundle_id = 'an_id'
        bundler = bundling.Executor(options)
        event = bundler.schedule(
            _return_kwargs,
            bundle_id,
            SIMPLE_DESCRIPTOR,
            _Bundled([an_elt]),
            {'an_option': 'a_value'}
        )
        self.assertEqual('a_value',
                         event.result['an_option'])


class TestExecutor_ElementCountTrigger(unittest2.TestCase):

    def test_api_call_not_invoked_until_threshold(self):
        an_elt = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        threshold = 3  # arbitrary
        options = BundleOptions(element_count_threshold=threshold)
        bundler = bundling.Executor(options)
        for i in range(threshold):
            got_event = bundler.schedule(
                api_call,
                an_id,
                SIMPLE_DESCRIPTOR,
                _Bundled([an_elt])
            )
            self.assertIsNotNone(
                got_event.canceller,
                'missing canceller after element #{}'.format(i))
            if i + 1 < threshold:
                self.assertFalse(got_event.is_set())
                self.assertIsNone(got_event.result)
            else:
                self.assertTrue(got_event.is_set())
                self.assertEqual(_Bundled([an_elt] * threshold),
                                 got_event.result)


class TestExecutor_RequestByteTrigger(unittest2.TestCase):

    def test_api_call_not_invoked_until_threshold(self):
        an_elt = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        elts_for_threshold = 3
        threshold = elts_for_threshold * len(an_elt)  # arbitrary
        options = BundleOptions(request_byte_threshold=threshold)
        bundler = bundling.Executor(options)
        for i in range(elts_for_threshold):
            got_event = bundler.schedule(
                api_call,
                an_id,
                SIMPLE_DESCRIPTOR,
                _Bundled([an_elt])
            )
            self.assertIsNotNone(
                got_event.canceller,
                'missing canceller after element #{}'.format(i))
            if i + 1 < elts_for_threshold:
                self.assertFalse(got_event.is_set())
                self.assertIsNone(got_event.result)
            else:
                self.assertTrue(got_event.is_set())
                self.assertEqual(_Bundled([an_elt] * elts_for_threshold),
                                 got_event.result)


class TestExecutor_DelayThreshold(unittest2.TestCase):

    @mock.patch('google.gax.bundling.TIMER_FACTORY')
    def test_api_call_is_scheduled_on_timer(self, timer_class):
        an_elt = 'dummy message'
        an_id = 'bundle_id'
        api_call = _return_request
        delay_threshold = 3
        options = BundleOptions(delay_threshold=delay_threshold)
        bundler = bundling.Executor(options)
        got_event = bundler.schedule(
            api_call,
            an_id,
            SIMPLE_DESCRIPTOR,
            _Bundled([an_elt])
        )
        self.assertIsNotNone(got_event, 'missing event after first request')
        self.assertIsNone(got_event.result)
        self.assertTrue(timer_class.called)
        timer_args, timer_kwargs = timer_class.call_args_list[0]
        self.assertEqual(delay_threshold, timer_args[0])
        self.assertEqual({'args': [an_id]}, timer_kwargs)
        timer_class.return_value.start.assert_called_once_with()


class TestEvent(unittest2.TestCase):

    def test_can_be_set(self):
        ev = bundling.Event()
        self.assertFalse(ev.is_set())
        ev.set()
        self.assertTrue(ev.is_set())

    def test_can_be_cleared(self):
        ev = bundling.Event()
        ev.result = object()
        ev.set()
        self.assertTrue(ev.is_set())
        self.assertIsNotNone(ev.result)
        ev.clear()
        self.assertFalse(ev.is_set())
        self.assertIsNone(ev.result)

    def test_cancel_returns_false_without_canceller(self):
        ev = bundling.Event()
        self.assertFalse(ev.cancel())

    def test_cancel_returns_canceller_result(self):
        ev = bundling.Event()
        ev.canceller = lambda: True
        self.assertTrue(ev.cancel())
        ev.canceller = lambda: False
        self.assertFalse(ev.cancel())

    def test_wait_does_not_block_if_event_is_set(self):
        ev = bundling.Event()
        ev.set()
        self.assertTrue(ev.wait())
