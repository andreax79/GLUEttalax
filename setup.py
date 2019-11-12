#!/usr/bin/env python3

import os
import os.path
import codecs
from setuptools import setup
from gluettalax import __version__

d = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(d, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name = 'gluettalax',
    version = __version__,
    description = 'Glue ETL without constipation',
    long_description = long_description,
    long_description_content_type='text/markdown',
    url = 'https://github.com/andreax79/GLUEttalax',
    author = 'Andrea Bonomi',
    author_email = 'andrea.bonomi@gmail.com',
    license = 'MIT',
    classifiers = [
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'License :: Public Domain',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    zip_safe=True,
    include_package_data=True,
    keywords = 'aws glue cli',
    py_modules=[ 'gluettalax' ],
    install_requires = [ 'boto3' ],
    entry_points = {
        'console_scripts': [
            'gluettalax=gluettalax:main',
        ],
    },
    test_suite='nose.collector',
    tests_require=['nose'],
)

