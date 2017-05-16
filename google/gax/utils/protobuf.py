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

"""Utility functions around reading and writing from protobufs."""

import collections

from google.protobuf.message import Message

__all__ = ('get', 'set', 'setdefault')


_SENTINEL = object()


def get(pb_or_dict, key, default=_SENTINEL):
    """Retrieve the given key off of the object.

    If a default is specified, return it if the key is not found, otherwise
    raise KeyError.

    Args:
        pb_or_dict (Union[~google.protobuf.message.Message, Mapping]): the
            object.
        key (str): The key to retrieve from the object in question.
        default (Any): If the key is not present on the object, and a default
            is set, returns that default instead. A type-appropriate falsy
            default is generally recommended, as protobuf messages almost
            always have default values for unset values and it is not always
            possible to tell the difference between a falsy value and an
            unset one. If no default is set, raises KeyError for not found
            values.

    Returns:
        Any: The return value from the underlying message or dict.

    Raises:
        KeyError: If the key is not found. Note that, for unset values,
            messages and dictionaries may not have consistent behavior.
        TypeError: If pb_or_dict is not a Message or Mapping.
    """
    # We may need to get a nested key. Resolve this.
    key, subkey = _resolve_subkeys(key)

    # Attempt to get the value from the two types of objects we know baout.
    # If we get something else, complain.
    if isinstance(pb_or_dict, Message):
        answer = getattr(pb_or_dict, key, default)
    elif isinstance(pb_or_dict, collections.Mapping):
        answer = pb_or_dict.get(key, default)
    else:
        raise TypeError('Tried to fetch a key %s on an invalid object; '
                        'expected a dict or protobuf message.')

    # If the object we got back is our sentinel, raise KeyError; this is
    # a "not found" case.
    if answer is _SENTINEL:
        raise KeyError(key)

    # If a subkey exists, call this method recursively against the answer.
    if subkey and answer is not default:
        return get(answer, subkey, default=default)

    # Return the value.
    return answer


def set(pb_or_dict, key, value):
    """Set the given key on the object.

    Args:
        pb_or_dict (Union[~google.protobuf.message.Message, Mapping]): the
            object.
        key (str): The key on the object in question.
        value (Any): The value to set.

    Raises:
        TypeError: If pb_or_dict is not a Message or Mapping.
    """
    # pylint: disable=redefined-builtin,too-many-branches
    # redefined-builtin: We want 'set' to be part of the public interface.
    # too-many-branches: This method is inherently complex.

    # Sanity check: Is our target object valid?
    if not isinstance(pb_or_dict, (collections.MutableMapping, Message)):
        raise TypeError('Tried to set a key %s on an invalid object; '
                        'expected a dict or protobuf message.' % key)

    # We may be setting a nested key. Resolve this.
    key, subkey = _resolve_subkeys(key)

    # If a subkey exists, then get that object and call this method
    # recursively against it using the subkey.
    if subkey is not None:
        if isinstance(pb_or_dict, collections.MutableMapping):
            pb_or_dict.setdefault(key, {})
        set(get(pb_or_dict, key), subkey, value)
        return

    # Attempt to set the value on the types of objects we know how to deal
    # with.
    if isinstance(pb_or_dict, collections.MutableMapping):
        pb_or_dict[key] = value
    elif isinstance(value, (collections.MutableSequence, tuple)):
        # Clear the existing repeated protobuf message of any elements
        # currently inside it.
        while getattr(pb_or_dict, key):
            getattr(pb_or_dict, key).pop()

        # Write our new elements to the repeated field.
        for item in value:
            if isinstance(item, collections.Mapping):
                getattr(pb_or_dict, key).add(**item)
            else:
                getattr(pb_or_dict, key).extend([item])
    elif isinstance(value, collections.Mapping):
        # Assign the dictionary values to the protobuf message.
        for item_key, item_value in value.items():
            set(getattr(pb_or_dict, key), item_key, item_value)
    elif isinstance(value, Message):
        # Assign the protobuf message values to the protobuf message.
        for item_key, item_value in value.ListFields():
            set(getattr(pb_or_dict, key), item_key.name, item_value)
    else:
        setattr(pb_or_dict, key, value)


def setdefault(pb_or_dict, key, value):
    """Set the key on the object to the value if the current value is falsy.

    Because protobuf Messages do not distinguish between unset values and
    falsy ones particularly well, this method treats any falsy value
    (e.g. 0, empty list) as a target to be overwritten, on both Messages
    and dictionaries.

    Args:
        pb_or_dict (Union[~google.protobuf.message.Message, Mapping]): the
            object.
        key (str): The key on the object in question.
        value (Any): The value to set.

    Raises:
        TypeError: If pb_or_dict is not a Message or Mapping.
    """
    if not get(pb_or_dict, key, default=None):
        set(pb_or_dict, key, value)


def _resolve_subkeys(key, separator='.'):
    """Given a key which may actually be a nested key, return the top level
    key and any nested subkeys as separate values.

    Args:
        key (str): A string that may or may not contain the separator.
        separator (str): The namespace separator. Defaults to `.`.

    Returns:
        Tuple[str, str]: The key and subkey(s).
    """
    subkey = None
    if separator in key:
        index = key.index(separator)
        subkey = key[index + 1:]
        key = key[:index]
    return key, subkey
