#!/usr/bin/env python3
#
#
# MIT License
#
# Copyright (c) 2019 Andrea Bonomi <andrea.bonomi@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import os
import pytest
from moto import mock_glue
from gluettalax import gluettalax, get_glue


@pytest.fixture(scope='function')
def aws_credentials():
    "Mocked AWS Credentials"
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-1'


def create_test_crawler(glue):
    assert glue.get_crawlers()['Crawlers'] == []
    glue.create_crawler(
        Name='test',
        Role='role',
        DatabaseName='database',
        Targets={
            'S3Targets': [
                {
                    'Path': 'string',
                },
            ]
        },
    )
    assert glue.get_crawlers()['Crawlers'] != []


@mock_glue
def test_help(aws_credentials):
    assert gluettalax() == 2
    assert gluettalax('help') == 0


@mock_glue
def test_list_crawlers(aws_credentials):
    glue = get_glue()
    create_test_crawler(glue)
    assert gluettalax('list_crawlers') == 0
    assert gluettalax('list_crawlers', 'test*') == 0


@mock_glue
def test_run_crawler(aws_credentials):
    glue = get_glue()
    create_test_crawler(glue)
    assert gluettalax('run_crawler', 'test', '--async') == 0
