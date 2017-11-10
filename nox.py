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

import nox


@nox.session
def lint(session):
    session.interpreter = 'python3.6'
    session.install(
        'flake8', 'flake8-import-order', 'pylint', 'docutils',
        'gcp-devrel-py-tools>=0.0.3')
    session.install('.')
    session.run(
        'python', 'setup.py', 'check', '--metadata',
        '--restructuredtext', '--strict')
    session.run(
        'flake8',
        '--import-order-style=google',
        '--application-import-names=google,tests',
        '--ignore=E501,I202',
        '--exclude=gapic,fixtures',
        'google', 'tests')
    session.run(
        'gcp-devrel-py-tools', 'run-pylint',
        '--config', 'pylint.conf.py',
        '--library-filesets', 'google',
        '--test-filesets', 'tests',
        success_codes=range(0, 31))


@nox.session
def docs(session):
    session.interpreter = 'python3.6'
    session.install('-r', 'docs/requirements-docs.txt')
    session.install('.')
    session.chdir('docs')
    session.run('make', 'html')


@nox.session
def generate_fixtures(session):
    session.interpreter = 'python2.7'
    session.install('-r', 'test-requirements.txt')
    session.install('.')
    session.run(
        'python', '-m', 'grpc.tools.protoc',
        '--proto_path=tests',
        '--python_out=tests',
        'tests/fixtures/fixture.proto')


@nox.session
def cover(session):
    session.interpreter = 'python3.6'
    session.install('-r', 'test-requirements.txt')
    session.install('.')
    session.run(
        'py.test', '--cov=google.gax', '--cov=tests', '--cov-report=', 'tests')
    session.run(
        'coverage', 'report', '--show-missing', '--fail-under=98')


@nox.session
@nox.parametrize(
    'python', ['python2.7', 'python3.4', 'python3.5', 'python3.6'])
def unit_tests(session, python):
    session.interpreter = python
    session.install('-r', 'test-requirements.txt')
    session.install('.')
    session.run(
        'py.test', '--cov=google.gax', '--cov=tests', 'tests',
        *session.posargs)
