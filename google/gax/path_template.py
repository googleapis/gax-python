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

"""Classes and functions for path template generation."""

from __future__ import absolute_import
import re
import urllib

CUSTOM_VERB_PATTERN = re.compile(':([^/*}{=]+)$')


def _encode_url(text):
    return urllib.quote(text.encode('utf8'), safe='')


def _decode_url(url):
    return urllib.unquote(url)


class ValidationException(Exception):
    """Indicates errors in path template parsing."""
    pass


class SegmentKind(object):
    """Enumerates possible Segment types."""
    # pylint: disable=too-few-public-methods
    LITERAL = 1
    CUSTOM_VERB = 2
    WILDCARD = 3
    PATH_WILDCARD = 4
    BINDING = 5
    END_BINDING = 6


class Segment(object):
    """Defines a single path template segment."""
    # pylint: disable=too-few-public-methods
    kind = None
    value = None
    separator = None

    def __init__(self, kind, value):
        self.kind = kind
        self.value = value
        if kind == SegmentKind.CUSTOM_VERB:
            self.separator = ':'
        elif kind == SegmentKind.END_BINDING:
            self.separator = ''
        else:
            self.separator = '/'

_WILDCARD_SEGMENT = Segment(SegmentKind.WILDCARD, '*')
_PATH_WILDCARD_SEGMENT = Segment(SegmentKind.PATH_WILDCARD, '**')
_END_BINDING_SEGMENT = Segment(SegmentKind.END_BINDING, '')


def _parse_template(template):
    """Splits a template into a list of path segments.

    Args:
        template: (str) A path template string.

    Returns:
        (list of Segment) A list of the parsed path segments.

    Raises:
        ValidationException: If there is a parsing error.
    """
    # pylint: disable=too-many-branches,too-many-statements
    if template.startswith('/'):
        template = template[1:]
    custom_verb = None
    match_obj = CUSTOM_VERB_PATTERN.search(template)
    if match_obj:
        custom_verb = match_obj.group(1)
        template = template[0:match_obj.start(0)]
    segment_list = []
    var_name = None
    free_wildcard_counter = 0
    path_wildcard_bound = 0

    for seg in [x.strip() for x in template.split('/')]:
        binding_starts = seg.startswith('{')
        implicit_wildcard = False
        if binding_starts:
            if var_name:
                raise ValidationException(
                    'parse error: nested binding in \'%s\'' % template)
            seg = seg[1:]
            i = seg.find('=')
            if i <= 0:
                if seg.endswith('}'):
                    implicit_wildcard = True
                    var_name = seg[:-1].strip()
                    seg = seg[-1:].strip()
                else:
                    raise ValidationException(
                        'parse error: invalid binding syntax in '
                        '\'%s\'' % template)
            else:
                var_name = seg[0:i].strip()
                seg = seg[i + 1:].strip()
            segment_list.append(Segment(SegmentKind.BINDING, var_name))

        binding_ends = seg.endswith('}')
        if binding_ends:
            seg = seg[0:-1].strip()

        if seg == '**' or seg == '*':
            if seg == '**':
                path_wildcard_bound += 1

            if len(seg) == 2:
                wildcard = _PATH_WILDCARD_SEGMENT
            else:
                wildcard = _WILDCARD_SEGMENT

            if var_name is None:
                segment_list.append(
                    Segment(SegmentKind.BINDING, '$%d' % free_wildcard_counter))
                free_wildcard_counter += 1
                segment_list.append(wildcard)
                segment_list.append(_END_BINDING_SEGMENT)
            else:
                segment_list.append(wildcard)
        elif not seg:
            if not binding_ends:
                raise ValidationException(
                    'parse error: empty segment not allowed in '
                    '\'%s\'' % template)
        else:
            segment_list.append(Segment(SegmentKind.LITERAL, seg))

        if binding_ends:
            var_name = None
            if implicit_wildcard:
                segment_list.append(_WILDCARD_SEGMENT)
            segment_list.append(_END_BINDING_SEGMENT)
        if path_wildcard_bound > 1:
            raise ValidationException(
                'parse error: pattern must not contain more than one path '
                'wildcard (\'**\') in \'%s\'' % template)
    if custom_verb:
        segment_list.append(Segment(SegmentKind.CUSTOM_VERB, custom_verb))
    return segment_list


class PathTemplate(object):
    """Representation of a path template."""

    HOSTNAME_VAR = '$hostname'

    bindings = None
    segments = None

    def __init__(self, segments):
        # Copy segments list.
        self.segments = list(segments)
        if not segments:
            raise ValidationException('template cannot be empty.')
        bindings = {}
        for seg in self.segments:
            if seg.kind == SegmentKind.BINDING:
                if seg.value in bindings:
                    raise ValidationException('duplicate binding \'%s\'',
                                              seg.value)
                bindings[seg.value] = seg
        self.bindings = bindings

    def __repr__(self):
        return self._to_syntax(True)

    @classmethod
    def from_string(cls, template):
        """Creates a PathTemplate from a template string.

        Args:
            template: (str) A path template string.

        Returns:
            (PathTemplate) A PathTemplate object.

        Raises:
            ValidationException: If there is a parsing error.
        """
        return PathTemplate(_parse_template(template))

    def _to_syntax(self, pretty):
        """Returns a pretty version of the template as a string."""
        result = ''
        continue_last = True
        iterator = iter(range(0, len(self.segments)))
        for i in iterator:
            seg = self.segments[i]
            if not continue_last:
                result += seg.separator
            continue_last = False
            if seg.kind == SegmentKind.BINDING:
                if pretty and seg.value.startswith('$'):
                    seg = self.segments[next(iterator)]
                    result += seg.value
                    next(iterator)
                    continue
                result += '{' + seg.value
                if (pretty and i + 2 < len(self.segments) and
                        self.segments[i + 1].kind == SegmentKind.WILDCARD and
                        self.segments[i + 2].kind == SegmentKind.END_BINDING):
                    next(iterator)
                    next(iterator)
                    result += '}'
                    continue
                result += '='
                continue_last = True
                continue
            elif seg.kind == SegmentKind.END_BINDING:
                result += '}'
                continue
            else:
                result += seg.value
                continue
        return result

    def match(self, path):
        """Returns a dict of variable names to matched values.

        All matched values will be properly unescaped using URL encoding rules.
        If the path does not match the template, None is returned.

        Args:
            path: (string) Path to match.

        Returns:
            (dict): A dictionary of variable names to matched values.
        """
        return self._match(path, False)

    def match_from_full_name(self, path):
        """Returns a dict of variable names to matched values from full name.

        Matches the path, where the first segment is interpreted as the host
        name regardless of whether it starts with '//' or not.

        Args:
            path: (string) Path to match.

        Returns:
            (dict): A dictionary of variable names to matched values.
        """
        return self._match(path, True)

    def _match(self, path, force_host_name):
        """Returns a dict of variable names to matched values."""
        last = self.segments[-1]
        if last.kind == SegmentKind.CUSTOM_VERB:
            match_obj = CUSTOM_VERB_PATTERN.search(path)
            if not match_obj or (
                    _decode_url(match_obj.group(1)) != last.value):
                return None
            path = path[0:match_obj.start(0)]

        # Do full match.
        with_host_name = path.startswith('//')
        if with_host_name:
            path = path[2:]
        inp = [x.strip() for x in path.split('/')]
        in_pos = 0
        values = {}
        if with_host_name or force_host_name:
            if not inp:
                return None
            host_name = inp[in_pos]
            in_pos += 1
            if with_host_name:
                host_name = '//' + host_name
            values[self.HOSTNAME_VAR] = host_name
        if not self._match_segments(inp, in_pos, 0, values):
            return None
        return values

    def _match_segments(self, inp, in_pos, seg_pos, values):
        """Returns whether the segments match the input."""
        # pylint: disable=too-many-branches
        current_var = None
        while seg_pos < len(self.segments):
            seg = self.segments[seg_pos]
            seg_pos += 1
            if seg.kind == SegmentKind.END_BINDING:
                current_var = None
                continue
            elif seg.kind == SegmentKind.BINDING:
                current_var = seg.value
                continue
            elif seg.kind == SegmentKind.CUSTOM_VERB:
                break
            else:
                if in_pos >= len(inp):
                    return False
                next_val = _decode_url(inp[in_pos])
                in_pos += 1
                if seg.kind == SegmentKind.LITERAL:
                    if seg.value != next_val:
                        return False
                if current_var:
                    current = values.get(current_var)
                    if not current:
                        values[current_var] = next_val
                    else:
                        values[current_var] = current + '/' + next_val
                if seg.kind == SegmentKind.PATH_WILDCARD:
                    segs_to_match = 0
                    for i in range(seg_pos, len(self.segments)):
                        kind = self.segments[i].kind
                        if (kind == SegmentKind.BINDING or
                                kind == SegmentKind.END_BINDING):
                            continue
                        else:
                            segs_to_match += 1
                    available = len(inp) - in_pos - segs_to_match
                    while available > 0:
                        values[current_var] += '/'
                        values[current_var] += _decode_url(inp[in_pos])
                        in_pos += 1
                        available -= 1
        return in_pos == len(inp)

    def instantiate(self, *args):
        """Instantiate the template based on the given variable assignment.

        Performs proper URL escaping of variable assignments. Note that free
        wildcards in the template must have bindings of '$n' variables, where
        'n' is the position of the wildcard (starting at 0).

        Args:
            *args: (str) A variable number of key value pairs.

        Returns:
            (str) An instantiated path template string.

        Raises:
            ValidationException: If there is a parsing error.
        """
        values = {}
        for i in range(0, len(args), 2):
            values[args[i]] = args[i + 1]
        return self._instantiate(values, False)

    def instantiate_partial(self, values):
        """Instantiate the template based on the given variable assignment.

        Similar to instantiate(*args), but allows for unbound variables, which
        are substituted with their original syntax.

        Args:
            values: (dict) A dictionary of binding pairs.

        Returns:
            (str) An instantiated path template string. Can be used to create a
                new template.

        Raises:
            ValidationException: If there is a parsing error.
        """
        return self._instantiate(values, True)

    def _instantiate(self, values, allow_partial):
        """Instantiates the template based on the given variable assignment."""
        # pylint: disable=too-many-branches,too-many-locals
        result = ''
        if self.HOSTNAME_VAR in values:
            result += values[self.HOSTNAME_VAR] + '/'
        continue_last = True
        skip = False

        iterator = iter(range(0, len(self.segments)))
        for i in iterator:
            seg = self.segments[i]
            if not skip and not continue_last:
                result += seg.separator
            continue_last = False
            if seg.kind == SegmentKind.BINDING:
                var = seg.value
                if seg.value not in values:
                    if not allow_partial:
                        raise ValidationException(
                            'unbound variable \'%s\'. bindings: %s' % (var,
                                                                       values))
                    if var.startswith('$'):
                        result += self.segments[i + 1].value
                        next(iterator)
                        next(iterator)
                        continue
                    result += '{' + seg.value + '='
                    continue_last = True
                    continue

                value = values[seg.value]

                next_segment = self.segments[i + 1]
                next_next_segment = self.segments[i + 2]
                path_escape = next_segment.kind == SegmentKind.PATH_WILDCARD
                path_escape |= next_next_segment.kind != SegmentKind.END_BINDING
                if not path_escape:
                    result += _encode_url(value)
                else:
                    first = True
                    for sub_seg in [x.strip() for x in value.split('/')]:
                        if not first:
                            result += '/'
                        first = False
                        result += _encode_url(sub_seg)
                skip = True
                continue
            elif seg.kind == SegmentKind.END_BINDING:
                if not skip:
                    result += '}'
                skip = False
                continue
            else:
                if not skip:
                    result += seg.value
        return result
