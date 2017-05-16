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

"""Provides behavior that supports request bundling.

:func:`compute_bundle_id` is used generate ids linking API requests to the
appropriate bundles.

:class:`Event` is the result of scheduling a bundled api call.  It is a
decorated :class:`threading.Event`; its ``wait`` and ``is_set`` methods
are used wait for the bundle request to complete or determine if it has
been completed respectively.

:class:`Task` manages the sending of all the requests in a specific bundle.

:class:`Executor` has a ``schedule`` method that is used add bundled api calls
to a new or existing :class:`Task`.

"""

from __future__ import absolute_import

import collections
import copy
import logging
import threading

_LOG = logging.getLogger(__name__)


def _str_dotted_getattr(obj, name):
    """Expands extends getattr to allow dots in x to indicate nested objects.

    Args:
       obj (object): an object.
       name (str): a name for a field in the object.

    Returns:
       Any: the value of named attribute.

    Raises:
       AttributeError: if the named attribute does not exist.
    """
    for part in name.split('.'):
        obj = getattr(obj, part)
    return str(obj) if obj else None


def compute_bundle_id(obj, discriminator_fields):
    """Computes a bundle id from the discriminator fields of `obj`.

    discriminator_fields may include '.' as a separator, which is used to
    indicate object traversal.  This is meant to allow fields in the
    computed bundle_id.

    the id is a tuple computed by going through the discriminator fields in
    order and obtaining the str(value) object field (or nested object field)

    if any discriminator field cannot be found, ValueError is raised.

    Args:
      obj (object): an object.
      discriminator_fields (Sequence[str]): a list of discriminator fields in
        the order to be to be used in the id.

    Returns:
      Tuple[str]: computed as described above.

    Raises:
      AttributeError: if any discriminator fields attribute does not exist.
    """
    return tuple(_str_dotted_getattr(obj, x) for x in discriminator_fields)


_WARN_DEMUX_MISMATCH = ('cannot demultiplex the bundled response, got'
                        ' %d subresponses; want %d, each bundled request will'
                        ' receive all responses')


class Task(object):
    """Coordinates the execution of a single bundle."""
    # pylint: disable=too-many-instance-attributes

    def __init__(self, api_call, bundle_id, bundled_field, bundling_request,
                 kwargs, subresponse_field=None):
        """
        Args:
           api_call (Callable[Sequence[object], object]): the func that is this
             tasks's API call.
           bundle_id (Tuple[str]): the id of this bundle.
           bundled_field (str): the field used to create the bundled request.
           bundling_request (object): the request to pass as the arg to
              api_call.
           kwargs (dict): keyword arguments passed to api_call.
           subresponse_field (str): optional field used to demultiplex
              responses.

        """
        self._api_call = api_call
        self._bundling_request = bundling_request
        self._kwargs = kwargs
        self.bundle_id = bundle_id
        self.bundled_field = bundled_field
        self.subresponse_field = subresponse_field
        self.timer = None
        self._in_deque = collections.deque()
        self._event_deque = collections.deque()

    @property
    def element_count(self):
        """The number of bundled elements in the repeated field."""
        return sum(len(elts) for elts in self._in_deque)

    @property
    def request_bytesize(self):
        """The size of in bytes of the bundled field elements."""
        return sum(len(str(e)) for elts in self._in_deque for e in elts)

    def run(self):
        """Call the task's func.

        The task's func will be called with the bundling requests func
        """
        if not self._in_deque:
            return
        req = self._bundling_request
        del getattr(req, self.bundled_field)[:]
        getattr(req, self.bundled_field).extend(
            [e for elts in self._in_deque for e in elts])

        subresponse_field = self.subresponse_field
        if subresponse_field:
            self._run_with_subresponses(req, subresponse_field, self._kwargs)
        else:
            self._run_with_no_subresponse(req, self._kwargs)

    def _run_with_no_subresponse(self, req, kwargs):
        try:
            resp = self._api_call(req, **kwargs)
            for event in self._event_deque:
                event.result = resp
                event.set()
        except Exception as exc:  # pylint: disable=broad-except
            for event in self._event_deque:
                event.result = exc
                event.set()
        finally:
            self._in_deque.clear()
            self._event_deque.clear()

    def _run_with_subresponses(self, req, subresponse_field, kwargs):
        try:
            resp = self._api_call(req, **kwargs)
            in_sizes = [len(elts) for elts in self._in_deque]
            all_subresponses = getattr(resp, subresponse_field)
            if len(all_subresponses) != sum(in_sizes):
                _LOG.warning(_WARN_DEMUX_MISMATCH, len(all_subresponses),
                             sum(in_sizes))
                for event in self._event_deque:
                    event.result = resp
                    event.set()
            else:
                start = 0
                for i, event in zip(in_sizes, self._event_deque):
                    next_copy = copy.copy(resp)
                    subresponses = all_subresponses[start:start + i]
                    next_copy.ClearField(subresponse_field)
                    getattr(next_copy, subresponse_field).extend(subresponses)
                    start += i
                    event.result = next_copy
                    event.set()
        except Exception as exc:  # pylint: disable=broad-except
            for event in self._event_deque:
                event.result = exc
                event.set()
        finally:
            self._in_deque.clear()
            self._event_deque.clear()

    def extend(self, elts):
        """Adds elts to the tasks.

        Args:
           elts (Sequence): a iterable of elements that can be appended to the
            task's bundle_field.

        Returns:
            Event: an event that can be used to wait on the response.
        """
        # Use a copy, not a reference, as it is later necessary to mutate
        # the proto field from which elts are drawn in order to construct
        # the bundled request.
        elts = elts[:]
        self._in_deque.append(elts)
        event = self._event_for(elts)
        self._event_deque.append(event)
        return event

    def _event_for(self, elts):
        """Creates an Event that is set when the bundle with elts is sent."""
        event = Event()
        event.canceller = self._canceller_for(elts, event)
        return event

    def _canceller_for(self, elts, event):
        """Obtains a cancellation function that removes elts.

        The returned cancellation function returns ``True`` if all elements
        was removed successfully from the _in_deque, and false if it was not.
        """
        def canceller():
            """Cancels submission of ``elts`` as part of this bundle.

            Returns:
               bool: ``False`` if any of elements had already been sent,
               otherwise ``True``.
            """
            try:
                self._event_deque.remove(event)
                self._in_deque.remove(elts)
                return True
            except ValueError:
                return False

        return canceller


TIMER_FACTORY = threading.Timer  # pylint: disable=invalid-name
"""A class with an interface similar to threading.Timer.

Defaults to threading.Timer.  This makes it easy to plug-in alternate
timer implementations."""


class Executor(object):
    """Organizes bundling for an api service that requires it."""
    # pylint: disable=too-few-public-methods

    def __init__(self, options):
        """Constructor.

        Args:
           options (gax.BundleOptions): configures strategy this instance
             uses when executing bundled functions.

        """
        self._options = options
        self._tasks = {}
        self._task_lock = threading.RLock()

    def schedule(self, api_call, bundle_id, bundle_desc, bundling_request,
                 kwargs=None):
        """Schedules bundle_desc of bundling_request as part of bundle_id.

        The returned value an :class:`Event` that

        * has a ``result`` attribute that will eventually be set to the result
          the api call
        * will be used to wait for the response
        * holds the canceller function for canceling this part of the bundle

        Args:
          api_call (callable[[object], object]): the scheduled API call.
          bundle_id (str): identifies the bundle on which the API call should be
            made.
          bundle_desc (gax.BundleDescriptor): describes the structure of the
            bundled call.
          bundling_request (object): the request instance to use in the API
            call.
          kwargs (dict): optional, the keyword arguments passed to the API call.

        Returns:
           Event: the scheduled event.
        """
        kwargs = kwargs or dict()
        bundle = self._bundle_for(api_call, bundle_id, bundle_desc,
                                  bundling_request, kwargs)
        elts = getattr(bundling_request, bundle_desc.bundled_field)
        event = bundle.extend(elts)

        # Run the bundle if the count threshold was reached.
        count_threshold = self._options.element_count_threshold
        if count_threshold > 0 and bundle.element_count >= count_threshold:
            self._run_now(bundle.bundle_id)

        # Run the bundle if the size threshold was reached.
        size_threshold = self._options.request_byte_threshold
        if size_threshold > 0 and bundle.request_bytesize >= size_threshold:
            self._run_now(bundle.bundle_id)

        return event

    def _bundle_for(self, api_call, bundle_id, bundle_desc, bundling_request,
                    kwargs):
        with self._task_lock:
            bundle = self._tasks.get(bundle_id)
            if bundle is None:
                bundle = Task(api_call, bundle_id, bundle_desc.bundled_field,
                              bundling_request, kwargs,
                              subresponse_field=bundle_desc.subresponse_field)
                delay_threshold = self._options.delay_threshold
                if delay_threshold > 0:
                    self._run_later(bundle, delay_threshold)
                self._tasks[bundle_id] = bundle
            return bundle

    def _run_later(self, bundle, delay_threshold):
        with self._task_lock:
            if bundle.timer is None:
                the_timer = TIMER_FACTORY(
                    delay_threshold,
                    self._run_now,
                    args=[bundle.bundle_id])
                the_timer.start()
                bundle.timer = the_timer

    def _run_now(self, bundle_id):
        with self._task_lock:
            if bundle_id in self._tasks:
                a_task = self._tasks.pop(bundle_id)
                a_task.run()


class Event(object):
    """Wraps a threading.Event, adding, canceller and result attributes."""

    def __init__(self):
        """Constructor.

        """
        self._event = threading.Event()
        self.result = None
        self.canceller = None

    def is_set(self):
        """Calls ``is_set`` on the decorated :class:`threading.Event`."""
        return self._event.is_set()

    def set(self):
        """Calls ``set`` on the decorated :class:`threading.Event`."""
        return self._event.set()

    def clear(self):
        """Calls ``clear`` on the decorated :class:`threading.Event`.

        Also resets the result if one has been set.
        """
        self.result = None
        return self._event.clear()

    def wait(self, timeout=None):
        """Calls ``wait`` on the decorated :class:`threading.Event`."""
        return self._event.wait(timeout=timeout)

    def cancel(self):
        """Invokes the cancellation function provided on construction."""
        if self.canceller:
            return self.canceller()
        else:
            return False
