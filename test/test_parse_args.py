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

import pytest
from gluettalax import parse_args, InvalidOption

help_text_1 = '<crawler_name> [--async] [--timeout=seconds]'
default_args_1 = {'op_async': False, 'timeout': 123}
help_text_2 = '[<job_name>] [--lines=num] [--noheaders]'
default_args_2 = {'lines': None, 'op_noheaders': False}
help_text_3 = '<job_name> [--async] [--param=value...]'
default_args_3 = {'op_async': False}


def test_none():
    args = None
    name, kargs = parse_args(args, help_text_2)


def test_empty_list():
    args = []
    name, kargs = parse_args(args, help_text_2)


def test_parse_ok_1_full():
    args = ['run_crawler', 'NAME', '--async', '--timeout=456']
    name, kargs = parse_args(args, help_text_1, default_args_1)
    assert name == 'NAME'
    assert kargs['timeout'] == '456'
    assert kargs['op_async'] == True


def test_parse_ok_1_space():
    args = ['run_crawler', 'NAME', '--timeout', '456', '--async']
    name, kargs = parse_args(args, help_text_1, default_args_1)
    assert name == 'NAME'
    assert kargs['timeout'] == '456'
    assert kargs['op_async'] == True


def test_parse_ok_1_part():
    args = ['run_crawler', 'NAME', '--timeout=456']
    name, kargs = parse_args(args, help_text_1, default_args_1)
    assert name == 'NAME'
    assert kargs['timeout'] == '456'
    assert kargs['op_async'] == False


def test_parse_defaults():
    args = ['run_crawler', 'NAME']
    name, kargs = parse_args(args, help_text_1, default_args_1)
    assert name == 'NAME'
    assert kargs['timeout'] == 123
    assert kargs['op_async'] == False


def test_parse_missing():
    with pytest.raises(InvalidOption):
        args = ['run_crawler']
        name, kargs = parse_args(args, help_text_1, default_args_1)
        print(name, kargs)


def test_parse_invalid():
    with pytest.raises(InvalidOption):
        args = ['run_crawler', 'A', 'B']
        name, kargs = parse_args(args, help_text_1, default_args_1)
        print(name, kargs)


def test_parse_ok_2():
    args = ['list_runs', 'NAME', '--lines=1']
    name, kargs = parse_args(args, help_text_2, default_args_2)
    assert name == 'NAME'
    assert kargs['lines'] == '1'


def test_parse_ok_2_defaults():
    args = ['list_runs']
    name, kargs = parse_args(args, help_text_2, default_args_2)
    assert name == None
    assert kargs['lines'] == None


def test_parse_ok_2_no_name():
    args = ['list_runs', '--lines=123', '--noheaders']
    name, kargs = parse_args(args, help_text_2, default_args_2)
    assert name == None
    assert kargs['lines'] == '123'
    assert kargs['op_noheaders'] == True


def test_parse_ok_2_no_name_reverse():
    args = ['list_runs', '--noheaders', '--lines=123']
    name, kargs = parse_args(args, help_text_2, default_args_2)
    assert name == None
    assert kargs['lines'] == '123'
    assert kargs['op_noheaders'] == True


def test_parse_ok_3():
    args = ['run_job', 'NAME', '--a=1', '--b=2']
    name, kargs = parse_args(args, help_text_3, default_args_3)
    assert name == 'NAME'
    assert kargs['a'] == '1'
    assert kargs['b'] == '2'


def test_parse_ok_3_defaults():
    args = ['run_job', 'NAME']
    name, kargs = parse_args(args, help_text_3, default_args_3)
    assert name == 'NAME'
