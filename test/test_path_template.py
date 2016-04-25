# Copyright 2015, Google Inc.
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
"""Unit tests for the path_template module."""

from __future__ import absolute_import
import unittest2

from google.gax.path_template import PathTemplate, ValidationException


class TestPathTemplate(unittest2.TestCase):
    """Unit tests for PathTemplate."""

    def test_len(self):
        self.assertEqual(len(PathTemplate('a/b/**/*/{a=hello/world}')), 6)

    def test_fail_invalid_token(self):
        self.assertRaises(ValidationException,
                          PathTemplate, 'hello/wor*ld')

    def test_fail_when_impossible_match(self):
        template = PathTemplate('hello/world')
        self.assertRaises(ValidationException,
                          template.match, 'hello')
        template = PathTemplate('hello/world')
        self.assertRaises(ValidationException,
                          template.match, 'hello/world/fail')

    def test_fail_mismatched_literal(self):
        template = PathTemplate('hello/world')
        self.assertRaises(ValidationException,
                          template.match, 'hello/world2')

    def test_fail_when_multiple_path_wildcards(self):
        self.assertRaises(ValidationException,
                          PathTemplate, 'buckets/*/**/**/objects/*')

    def test_fail_if_inner_binding(self):
        self.assertRaises(ValidationException,
                          PathTemplate, 'buckets/{hello={world}}')

    def test_fail_unexpected_eof(self):
        self.assertRaises(ValidationException,
                          PathTemplate, 'a/{hello=world')

    def test_match_atomic_resource_name(self):
        template = PathTemplate('buckets/*/*/objects/*')
        self.assertEqual({'$0': 'f', '$1': 'o', '$2': 'bar'},
                         template.match('buckets/f/o/objects/bar'))
        template = PathTemplate('/buckets/{hello}')
        self.assertEqual({'hello': 'world'},
                         template.match('buckets/world'))
        template = PathTemplate('/buckets/{hello=*}')
        self.assertEqual({'hello': 'world'},
                         template.match('buckets/world'))

    def test_match_escaped_chars(self):
        template = PathTemplate('buckets/*/objects')
        self.assertEqual({'$0': 'hello%2F%2Bworld'},
                         template.match('buckets/hello%2F%2Bworld/objects'))

    def test_match_template_with_unbounded_wildcard(self):
        template = PathTemplate('buckets/*/objects/**')
        self.assertEqual({'$0': 'foo', '$1': 'bar/baz'},
                         template.match('buckets/foo/objects/bar/baz'))

    def test_match_with_unbound_in_middle(self):
        template = PathTemplate('bar/**/foo/*')
        self.assertEqual({'$0': 'foo/foo', '$1': 'bar'},
                         template.match('bar/foo/foo/foo/bar'))

    def test_render_atomic_resource(self):
        template = PathTemplate('buckets/*/*/*/objects/*')
        url = template.render({
            '$0': 'f', '$1': 'o', '$2': 'o', '$3': 'google.com:a-b'})
        self.assertEqual(url, 'buckets/f/o/o/objects/google.com:a-b')

    def test_render_fail_when_too_few_variables(self):
        template = PathTemplate('buckets/*/*/*/objects/*')
        self.assertRaises(ValidationException,
                          template.render,
                          {'$0': 'f', '$1': 'l', '$2': 'o'})

    def test_render_with_unbound_in_middle(self):
        template = PathTemplate('bar/**/foo/*')
        url = template.render({'$0': '1/2', '$1': '3'})
        self.assertEqual(url, 'bar/1/2/foo/3')

    def test_to_string(self):
        template = PathTemplate('bar/**/foo/*')
        self.assertEqual(str(template), 'bar/{$0=**}/foo/{$1=*}')
        template = PathTemplate('buckets/*/objects/*')
        self.assertEqual(str(template), 'buckets/{$0=*}/objects/{$1=*}')
        template = PathTemplate('/buckets/{hello}')
        self.assertEqual(str(template), 'buckets/{hello=*}')
        template = PathTemplate('/buckets/{hello=what}/{world}')
        self.assertEqual(str(template), 'buckets/{hello=what}/{world=*}')
        template = PathTemplate('/buckets/helloazAZ09-.~_what')
        self.assertEqual(str(template), 'buckets/helloazAZ09-.~_what')
