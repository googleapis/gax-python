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

"""Google API Extensions"""

from __future__ import absolute_import
import collections


__version__ = '0.4.0'


class PageDescriptor(
        collections.namedtuple(
            'PageDescriptor',
            ['request_page_token_field',
             'response_page_token_field',
             'resource_field'])):
    """Describes the structure of a page-streaming call."""
    pass


class BundleDescriptor(
        collections.namedtuple(
            'BundleDescriptor',
            ['bundled_field',
             'request_descriminator_fields',
             'subresponse_field'])):
    """Describes the structure of bundled call.

    request_descriminator_fields may include '.' as a separator, which is used
    to indicate object traversal.  This allows fields in nested objects to be
    used to determine what requests to bundle.

    Attributes:
      bundled_field: the repeated field in the request message that
        will have its messages aggregated by bundling
      request_descriminator_fields: a list of fields in the
        target request message class that are used to determine
        which messages should be bundled together.
      subresponse_field: an optional field, when present it indicates the field
        in the response message that should be used to demultiplex the response
        into multiple response messages.
    """
    def __new__(cls,
                bundled_field,
                request_descriminator_fields,
                subresponse_field=None):
        return super(cls, BundleDescriptor).__new__(
            cls,
            bundled_field,
            request_descriminator_fields,
            subresponse_field)


class BundleOptions(
        collections.namedtuple(
            'BundleOptions',
            ['message_count_threshold',
             'message_bytesize_threshold',
             'delay_threshold'])):
    """Holds values used to configure bundling.

    The xxx_threshold attributes are used to configure when the bundled request
    should be made.

    Attributes:
        message_count_threshold: the bundled request will be sent once
            the count of outstanding messages reaches this value
        message_bytesize_threshold: the bundled request will be sent once
            the count of bytes in the outstanding messages reaches this value
        delay_threshold: the bundled request will be sent this amount of
            time after the first message in the bundle was added to it.

    """
    # pylint: disable=too-few-public-methods

    def __new__(cls,
                message_count_threshold=0,
                message_bytesize_threshold=0,
                delay_threshold=0):
        """Invokes the base constructor with default values.

        The default values are zero for all attributes and it's necessary to
        specify at least one valid threshold value during construction.

        Args:
           message_count_threshold: the bundled request will be sent once
             the count of outstanding messages reaches this value
           message_bytesize_threshold: the bundled request will be sent once
             the count of bytes in the outstanding messages reaches this value
           delay_threshold: the bundled request will be sent this amount of
              time after the first message in the bundle was added to it.

        """
        assert isinstance(message_count_threshold, int), 'should be an int'
        assert isinstance(message_bytesize_threshold, int), 'should be an int'
        assert isinstance(delay_threshold, int), 'should be an int'
        assert (message_bytesize_threshold > 0 or
                message_count_threshold > 0 or
                delay_threshold > 0), 'one threshold should be > 0'

        return super(cls, BundleOptions).__new__(
            cls,
            message_count_threshold,
            message_bytesize_threshold,
            delay_threshold)
