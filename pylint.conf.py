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

"""This module is used to config gcp-devrel-py-tools run-pylint."""

import copy

library_additions = {
    'MESSAGES CONTROL': {
        'disable': [
            'I',
            'import-error',
            'no-member',
            'protected-access',
            'redefined-variable-type',
            'similarities',
            'no-else-return',
        ],
    },
}

library_replacements = {
    'MASTER': {
        'ignore': [
            'CVS', '.git', '.cache', '.tox', '.nox', 'gapic', 'fixtures'],
        'load-plugins': 'pylint.extensions.check_docs',
    },
    'REPORTS': {
        'reports': 'no',
    },
    'BASIC': {
        'method-rgx': '[a-z_][a-z0-9_]{2,40}$',
        'function-rgx': '[a-z_][a-z0-9_]{2,40}$',
    },
    'TYPECHECK': {
        'ignored-modules': ['six', 'google.protobuf'],
    },
    'DESIGN': {
        'min-public-methods': '0',
        'max-args': '10',
        'max-attributes': '15',
    },
}

test_additions = copy.deepcopy(library_additions)
test_additions['MESSAGES CONTROL']['disable'].extend([
    'missing-docstring',
    'no-self-use',
    'redefined-outer-name',
    'unused-argument',
    'no-name-in-module',
])
test_replacements = copy.deepcopy(library_replacements)
test_replacements.setdefault('BASIC', {})
test_replacements['BASIC'].update({
    'good-names': ['i', 'j', 'k', 'ex', 'Run', '_', 'fh', 'pytestmark'],
    'method-rgx': '[a-z_][a-z0-9_]{2,80}$',
    'function-rgx': '[a-z_][a-z0-9_]{2,80}$',
})

ignored_files = ()
