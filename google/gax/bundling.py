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

"""Provides behavior that supports request bundling."""

from __future__ import absolute_import

import collections
import threading


def _str_dotted_getattr(obj, name):
    """Expands extends getattr to allow dots in x to indicate nested objects.

    Args:
       obj: an object
       name: a name for a field in the object

    Returns:
       the value of named attribute

    Raises:
       AttributeError if the named attribute does not exist
    """
    if name.find('.') == -1:
        return getattr(obj, name)
    for part in name.split('.'):
        obj = getattr(obj, part)
    return str(obj) if obj is not None else None


def compute_bundle_id(obj, descriminator_fields):
    """Computes a bundle id from the descriminator fields of `obj`.

    descriminator_fields may include '.' as a separator, which is used to
    indicate object traversal.  This is meant to allow fields in the
    computed bundle_id.

    the id is a tuple computed by going through the descriminator fields in
    order and obtaining the str(value) object field (or nested object field)

    if any descriminator field cannot be found, ValueError is raised.

    Args:
      obj: an object
      descriminator_fields: a list of descriminator fields in the order to be
        to be used in the id

    Returns:
      tuple: computed as described above

    Raises:
      AttributeError: if any descriminator fields attribute does not exist
    """
    return tuple(_str_dotted_getattr(obj, x) for x in descriminator_fields)


class Task(object):
    """Coordinates the execution of a single bundle."""

    def __init__(self, api_call, bundle_id, bundled_field, bundling_request):
        """Constructor.

        Args:
           api_call (callable[[object], object]): the func that is this tasks's
             API call
           bundle_id: the id of this bundle
           bundled_field: the field used to create the bundled request
           bundling_request: the request to pass as the arg to api_call

        """
        self._api_call = api_call
        self._bundling_request = bundling_request
        self.bundle_id = bundle_id
        self.bundled_field = bundled_field
        self.in_deque = collections.deque()
        self.out_deque = collections.deque()

    @property
    def message_count(self):
        """The number of bundled messages."""
        return len(self.in_deque)

    @property
    def message_bytesize(self):
        """The size of in bytes of the bundle messages."""
        return sum(len(str(x)) for x in self.in_deque)

    def run(self):
        """Call the task's func.

        The task's func will be called with the bundling requests func
        """
        if len(self.in_deque) == 0:
            return
        req = self._bundling_request
        setattr(req, self.bundled_field, list(x for x in self.in_deque))
        self.in_deque.clear()
        try:
            resp = self._api_call(req)
            self.out_deque.append(resp)
        except Exception as exc:  # pylint: disable=broad-except
            self.out_deque.append(exc)

    def append(self, msg):
        """Adds a msg to the in_queue."""
        self.in_deque.append(msg)

    def canceller_for(self, msg):
        """Obtains a cancellation function for msg.

`        The returned cancellation function returns ``True`` if the message
        was removed successfully from the in_deque, and false if it was not.


        Args:
           msg (object): the message to be cancelled

        Returns:
           (callable[[], boolean]): used to remove the message from the in_deque

        """

        def canceller():
            """Cancels submission of ``msg`` as part of this bundle."""
            try:
                self.in_deque.remove(msg)
                return True
            except ValueError:
                return False

        return canceller


TIMER_FACTORY = threading.Timer
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
        self._timer = None

    def schedule(self, api_call, bundle_id, bundled_field, bundling_request):
        """Schedules ``bundled_msg`` to be sent in ``bundling_request``.

        The returned value consists of two values, a collections.deque that will
        contain the response and an optional callable([[],boolean]) that can be
        used to cancel sending of this particular part of the bundle.

        TODO: determine whether or not the deque to provide seperate responses
        for each part of the bundle.

        Args:
          api_call (callable[[object], object]): the scheduled API call
          bundle_id (str): identifies the bundle on which the API call should be
            made
          bundled_field (str): the name of the field in bundling_request
          bundling_request (object): the request instance to use in the API call

        Returns:
           tuple: (:class:`collections.deque`,
                    callable([[], boolean]) )

        """
        bundle = self._bundle_for(api_call, bundle_id, bundled_field,
                                  bundling_request)
        msg = getattr(bundling_request, bundled_field)
        bundle.append(msg)

        # Run the bundle if the count threshold was reached.
        count_threshold = self._options.message_count_threshold
        if count_threshold > 0 and bundle.message_count >= count_threshold:
            self._run_now(bundle.bundle_id)
            return bundle.out_deque, None

        # Run the bundle if the size threshold was reached.
        size_threshold = self._options.message_bytesize_threshold
        if size_threshold > 0 and bundle.message_bytesize >= size_threshold:
            self._run_now(bundle.bundle_id)
            return bundle.out_deque, None

        return bundle.out_deque, bundle.canceller_for(msg)

    def _bundle_for(self, api_call, bundle_id, bundled_field, bundling_request):
        with self._task_lock:
            bundle = self._tasks.get(bundle_id)
            if bundle is None:
                bundle = Task(api_call, bundle_id, bundled_field,
                              bundling_request)
                delay_threshold = self._options.delay_threshold
                if delay_threshold > 0:
                    self._run_later(bundle, delay_threshold)
                self._tasks[bundle_id] = bundle
            return bundle

    def _run_later(self, bundle, delay_threshold):
        with self._task_lock:
            if self._timer is None:
                the_timer = TIMER_FACTORY(
                    delay_threshold,
                    self._run_now,
                    args=[bundle.bundle_id])
                the_timer.start()
                self._timer = the_timer

    def _run_now(self, bundle_id):
        with self._task_lock:
            if bundle_id in self._tasks:
                a_task = self._tasks.pop(bundle_id)
                a_task.run()
