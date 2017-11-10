#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import io

from setuptools import find_packages
from setuptools import setup


DEPENDENCIES = [
    'dill >= 0.2.5, < 0.3dev',
    'future >= 0.16.0, < 0.17dev',
    'googleapis-common-protos >= 1.5.2, < 2.0dev',
    'grpcio >=1.0.2, <2.0dev',
    'google-auth >= 1.0.0, <2.0dev',
    'ply == 3.8',
    'protobuf >= 3.0.0, < 4.0dev',
    'requests >= 2.13.0, < 3.0dev'
]

with io.open('README.rst', 'r') as readme:
    long_description = readme.read()

setup(
    name='google-gax',
    version='0.15.16',
    description='Google API Extensions',
    long_description=long_description,
    author='Google API Authors',
    author_email='googleapis-packages@google.com',
    url='https://github.com/googleapis/gax-python',
    packages=find_packages(exclude=('tests*',)),
    namespace_packages=('google',),
    license='BSD-3-Clause',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    install_requires=DEPENDENCIES,
    include_package_data=True,
)
