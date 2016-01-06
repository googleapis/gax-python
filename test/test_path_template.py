# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=missing-docstring,no-self-use,no-init,invalid-name
"""Unit tests for the path_template module."""

from __future__ import absolute_import
import unittest2

from google.gax.path_template import PathTemplate, ValidationException


class TestPathTemplate(unittest2.TestCase):
    """Unit tests for PathTemplate."""

    def test_match_atomic_resource_name(self):
        template = PathTemplate.from_string('buckets/*/*/objects/*')
        self.assertEqual({'$0': 'f', '$1': 'o', '$2': 'bar'},
                         template.match('buckets/f/o/objects/bar'))

    def test_match_template_with_unbounded_wildcard(self):
        template = PathTemplate.from_string('buckets/*/objects/**')
        self.assertEqual({'$0': 'foo', '$1': 'bar/baz'},
                         template.match('buckets/foo/objects/bar/baz'))

    def test_match_with_forced_host_name(self):
        template = PathTemplate.from_string('buckets/*/objects/*')
        match = template.match_from_full_name(
            'somewhere.io/buckets/b/objects/o')
        self.assertNotEqual(None, match)
        self.assertEqual('somewhere.io', match[PathTemplate.HOSTNAME_VAR])
        self.assertEqual('b', match['$0'])
        self.assertEqual('o', match['$1'])

    def test_match_with_host_name(self):
        template = PathTemplate.from_string('buckets/*/objects/*')
        match = template.match('//somewhere.io/buckets/b/objects/o')
        self.assertNotEqual(None, match)
        self.assertEqual('//somewhere.io', match[PathTemplate.HOSTNAME_VAR])
        self.assertEqual('b', match['$0'])
        self.assertEqual('o', match['$1'])

    def test_match_with_custom_method(self):
        template = PathTemplate.from_string('buckets/*/objects/*:custom')
        match = template.match('buckets/b/objects/o:custom')
        self.assertNotEqual(None, match)
        self.assertEqual('b', match['$0'])
        self.assertEqual('o', match['$1'])

    def test_match_fail_when_path_mismatch(self):
        template = PathTemplate.from_string('buckets/*/*/objects/*')
        self.assertEqual(None, template.match('buckets/f/o/o/objects/bar'))

    def test_match_fail_when_path_too_short(self):
        template = PathTemplate.from_string('buckets/*/*/objects/*')
        self.assertEqual(None, template.match('buckets/f/o/objects'))

    def test_match_fail_when_path_too_long(self):
        template = PathTemplate.from_string('buckets/*/*/objects/*')
        self.assertEqual(None, template.match('buckets/f/o/objects/too/long'))

    def test_match_with_unbound_in_middle(self):
        template = PathTemplate.from_string('bar/**/foo/*')
        self.assertEqual({'$0': 'foo/foo', '$1': 'bar'},
                         template.match('bar/foo/foo/foo/bar'))

    def test_instantiate_atomic_resource(self):
        template = PathTemplate.from_string('buckets/*/*/*/objects/*')
        url = template.instantiate('$0', 'f', '$1', 'o', '$2', 'o', '$3', 'bar')
        self.assertEqual(url, 'buckets/f/o/o/objects/bar')

    def test_instantiate_escape_unsafe_char(self):
        template = PathTemplate.from_string('buckets/*/objects/*')
        url = template.instantiate('$0', 'f/o/o', '$1', 'b/a/r')
        self.assertEqual(url, 'buckets/f%2Fo%2Fo/objects/b%2Fa%2Fr')

    def test_instantiate_not_escape_for_unbounded_wildcard(self):
        template = PathTemplate.from_string('buckets/*/objects/**')
        url = template.instantiate('$0', 'f/o/o', '$1', 'b/a/r')
        self.assertEqual(url, 'buckets/f%2Fo%2Fo/objects/b/a/r')

    def test_instantiate_fail_when_too_few_variables(self):
        template = PathTemplate.from_string('buckets/*/*/*/objects/*')
        self.assertRaises(ValidationException,
                          template.instantiate, '$0', 'f', 'l', 'o')

    def test_instantiate_with_unbound_in_middle(self):
        template = PathTemplate.from_string('bar/**/foo/*')
        url = template.instantiate('$0', '1/2', '$1', '3')
        self.assertEqual(url, 'bar/1/2/foo/3')

    def test_instantiate_partial(self):
        template = PathTemplate.from_string('bar/*/foo/*')
        url = template.instantiate_partial({'$0': '_1'})
        self.assertEqual(url, 'bar/_1/foo/*')

    def test_instantiate_with_host_name(self):
        template = PathTemplate.from_string('bar/*')
        url = template.instantiate(PathTemplate.HOSTNAME_VAR, '//somewhere.io',
                                   '$0', 'foo')
        self.assertEqual(url, '//somewhere.io/bar/foo')

    def test_to_string(self):
        template = PathTemplate.from_string('bar/**/foo/*')
        self.assertEqual(str(template), 'bar/**/foo/*')
        template = PathTemplate.from_string('buckets/*/objects/*:custom')
        self.assertEqual(str(template), 'buckets/*/objects/*:custom')
