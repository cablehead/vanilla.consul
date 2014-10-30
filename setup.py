import sys
import re

from setuptools.command.test import test as TestCommand
from setuptools import setup
from setuptools import find_packages


metadata = dict(
    re.findall("__([a-z]+)__ = '([^']+)'", open('vanilla/__init__.py').read()))

requirements = [
    x.strip() for x
    in open('requirements.txt').readlines() if not x.startswith('#')]


description = "Vanilla Python client for Consul.io"


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


setup(
    name='vanilla.consul',
    version=metadata['version'],
    author='Andy Gayton',
    author_email='andy@thecablelounge.com',
    install_requires=requirements,
    packages=find_packages(),
    namespace_packages=['vanilla'],
    url='https://github.com/cablehead/vanilla.consul',
    license='MIT',
    description=description,
    long_description=open('README.rst').read(),
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
)
