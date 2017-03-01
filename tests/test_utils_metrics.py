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

from __future__ import absolute_import

import collections
import platform
import unittest

import pkg_resources

from google import gax
from google.gax.utils import metrics


# pylint: disable=no-member
GRPC_VERSION = pkg_resources.get_distribution('grpcio').version
# pylint: enable=no-member


class TestFill(unittest.TestCase):
    def test_no_argument(self):
        headers = metrics.fill()

        # Assert that the headers are set appropriately.
        self.assertEqual(headers['gl-python'], platform.python_version())
        self.assertEqual(headers['gax'], gax.__version__)
        self.assertEqual(headers['grpc'], GRPC_VERSION)

        # Assert that the headers are in the correct order.
        self.assertEqual(
            [k for k in headers.keys()],
            ['gl-python', 'gax', 'grpc'],
        )

    def test_gapic(self):
        headers = metrics.fill(collections.OrderedDict((
            ('gl-python', platform.python_version()),
            ('gapic', '1.0.0'),
        )))

        # Assert that the headers are set appropriately.
        self.assertEqual(headers['gl-python'], platform.python_version())
        self.assertEqual(headers['gapic'], '1.0.0')
        self.assertEqual(headers['gax'], gax.__version__)
        self.assertEqual(headers['grpc'], GRPC_VERSION)

        # Assert that the headers are in the correct order.
        self.assertEqual(
            [k for k in headers.keys()],
            ['gl-python', 'gapic', 'gax', 'grpc'],
        )


class TestStringify(unittest.TestCase):
    def test_no_argument(self):
        self.assertEqual(metrics.stringify(), '')

    def test_with_unordered_argument(self):
        string = metrics.stringify({
            'gl-python': platform.python_version(),
            'gapic': '1.0.0',
            'gax': gax.__version__,
            'grpc': GRPC_VERSION,
        })

        # Assert that each section of the string is present, but
        # ignore ordering.
        self.assertIn('gl-python/%s' % platform.python_version(), string)
        self.assertIn('gapic/1.0.0', string)
        self.assertIn('gax/%s' % gax.__version__, string)
        self.assertIn('grpc/%s' % GRPC_VERSION, string)

    def test_with_ordered_argument(self):
        headers = collections.OrderedDict()
        headers['gl-python'] = platform.python_version()
        headers['gapic'] = '1.0.0'
        headers['gax'] = gax.__version__
        headers['grpc'] = GRPC_VERSION
        string = metrics.stringify(headers)

        # Check for the exact string, order and all.
        expected = 'gl-python/{python} gapic/{gapic} gax/{gax} grpc/{grpc}'
        self.assertEqual(string, expected.format(
            gapic='1.0.0',
            gax=gax.__version__,
            grpc=GRPC_VERSION,
            python=platform.python_version(),
        ))
