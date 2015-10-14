#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from setuptools.command.test import test as TestCommand
from setuptools import find_packages
import uuid
import imp

from pip.req import parse_requirements

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

# Avoiding import so we don't execute __init__.py, which has imports
# that aren't installed until after installation.
ambry_meta = imp.load_source('_meta', 'ambry_sources/__meta__.py')

packages = find_packages()

package_data = {
}

install_requires = parse_requirements('requirements/base.txt', session=uuid.uuid1())
tests_require = parse_requirements('requirements/dev.txt', session=uuid.uuid1())

classifiers = [
    'Development Status :: 4 - Beta',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Topic :: Software Development :: Debuggers',
    'Topic :: Software Development :: Libraries :: Python Modules',
]


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to py.test')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ''

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        if 'capture' not in self.pytest_args:
            # capture arg is not given. Disable capture by default.
            self.pytest_args = self.pytest_args + ' --capture=no'

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name='ambry-sources',
    version=ambry_meta.__version__,
    description='Ambry Partition Message Pack File',
    long_description=readme,
    packages=packages,
    package_data=package_data,
    install_requires=[x for x in reversed([str(x.req) for x in install_requires])],
    tests_require=[x for x in reversed([str(x.req) for x in tests_require])],
    scripts=['scripts/ampr'],
    author=ambry_meta.__author__,
    author_email='eric@civicknowledge.com',
    url='https://github.com/CivicKnowledge/ambry_sources.git',
    license='MIT',
    cmdclass={'test': PyTest},
    classifiers=classifiers,
    extras_require={
        'fdw': ['apsw==3.8.8.2-post1','psycopg2==2.6'],
        'geo': ['Fiona==1.6.1','Shapely==1.5.12']
    }
)
