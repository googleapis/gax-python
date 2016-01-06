#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

import os
import re
import sys

from setuptools import setup, find_packages

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

# Get the version
version_regex = r'__version__ = ["\']([^"\']*)["\']'
with open('google/gax/__init__.py', 'r') as f:
    text = f.read()
    match = re.search(version_regex, text)
    if match:
        version = match.group(1)
    else:
        raise RuntimeError("No version number found!")


install_requires = [
    'grpcio==0.11.0b1',
    'oauth2client>=1.5.2',
    'protobuf>=3.0.0b1.post1'
]

setup(
    name='google-gax',
    version=version,
    description='Google API Extensions',
    long_description=open('README.rst').read(),
    author='Google API Authors',
    author_email='googleapis-packages@google.com',
    url='',
    namespace_packages = ['google'],
    packages=find_packages(),
    package_dir={'google-gax': 'google'},
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
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    tests_require=['pytest'],
    install_requires=install_requires,
)
