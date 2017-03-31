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

"""Utility functions for manipulation of metrics headers."""

from __future__ import absolute_import

import collections
import platform

import pkg_resources

from google import gax


def fill(metrics_headers=()):
    """Add the metrics headers known to GAX.

    Return an OrderedDict with all of the metrics headers provided to
    this function, as well as the metrics known to GAX (such as its own
    version, the GRPC version, etc.).
    """
    # Create an ordered dictionary with the Python version, which
    # should go first.
    answer = collections.OrderedDict((
        ('gl-python', platform.python_version()),
    ))

    # Add anything that already appears in the passed metrics headers,
    # in order.
    for key, value in collections.OrderedDict(metrics_headers).items():
        answer[key] = value

    # Add the GAX and GRPC headers to our metrics.
    # These come after what may have been passed in (generally the GAPIC
    # library).
    answer['gax'] = gax.__version__
    # pylint: disable=no-member
    answer['grpc'] = pkg_resources.get_distribution('grpcio').version
    # pylint: enable=no-member

    return answer


def stringify(metrics_headers=()):
    """Convert the provided metrics headers to a string.

    Iterate over the metrics headers (a dictionary, usually ordered) and
    return a properly-formatted space-separated string
    (e.g. foo/1.2.3 bar/3.14.159).
    """
    metrics_headers = collections.OrderedDict(metrics_headers)
    return ' '.join(['%s/%s' % (k, v) for k, v in metrics_headers.items()])
