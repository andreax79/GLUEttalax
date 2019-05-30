#!/usr/bin/env python
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

import sys
import boto3
import time

__author__ = 'Andrea Bonomi <andrea.bonomi@gmail.com>'
__version__ = '0.1.0'
__all__ = [
    'Crawler',
    'CrawlerTimeout',
    'Job',
    'JobTimeout',
    'run_crawler',
    'list_crawlers',
    'run_job',
    'list_jobs'
]

def seconds(x): return x
def minutes(x): return x * 60

DEFAULT_CRAWLER_DELAY   = seconds(10)
DEFAULT_CRAWLER_TIMEOUT = minutes(10)
DEFAULT_JOB_DELAY       = seconds(10)


class CrawlerTimeout(Exception):
    """ Raised when Glue Crawler a timeout error occurs """
    pass

class CrawlerNotFound(Exception):
    pass

class JobNotFound(Exception):
    pass

class JobConcurrentRunsExceededException(Exception):
    pass

class Crawler(object):

    def __init__(self, name, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.glue = boto3.client('glue')

    @property
    def status(self):
        try:
            return self.glue.get_crawler(Name=self.name)['Crawler']
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise CrawlerNotFound(ex.message)

    @property
    def is_ready(self):
        return self.status['State'] == 'READY'

    def run(self, rerun=False):
        if rerun:
            start_time = time.time()
            while not self.is_ready:
                if time.time() > start_time + self.timeout:
                    raise CrawlerTimeout()
                time.sleep(self.delay)
        if self.is_ready:
            self.glue.start_crawler(Name=self.name)
        start_time = time.time()
        while not self.is_ready:
            if time.time() > start_time + self.timeout:
                raise CrawlerTimeout()
            time.sleep(self.delay)


class JobTimeout(Exception):
    """ Raised when Glue Job a timeout error occurs """
    pass


class Job(object):

    def __init__(self, name, delay=DEFAULT_JOB_DELAY, timeout=None):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.glue = boto3.client('glue')
        try:
            job = self.glue.get_job(JobName=self.name)['Job']
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise JobNotFound(ex.message)
        if self.timeout is None:
            self.timeout = minutes(job['Timeout'])

    def get_runs(self):
        try:
            return self.glue.get_job_runs(JobName=self.name)['JobRuns']
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise JobNotFound(ex.message)

    def get_run_state(self, job_run_id):
        try:
            return self.glue.get_job_run(JobName=self.name, RunId=job_run_id)['JobRun']['JobRunState']
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise JobNotFound(ex.message)

    def run(self, **kargs):
        arguments = dict([('--%s' % k, v) for k, v in kargs.items()])
        try:
            result = self.glue.start_job_run(JobName=self.name, Timeout=int(self.timeout/60), Arguments=arguments)
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise JobNotFound(ex.message)
        except self.glue.exceptions.ConcurrentRunsExceededException as ex:
            raise JobConcurrentRunsExceededException(ex.message)
        job_run_id = result['JobRunId']
        start_time = time.time()
        run_state = self.get_run_state(job_run_id)
        while run_state not in [ 'SUCCEEDED', 'FAILED' ]:
            if time.time() > start_time + self.timeout:
                raise JobTimeout()
            time.sleep(self.delay)
            run_state = self.get_run_state(job_run_id)
        return run_state == 'SUCCEEDED'

def run_crawler(name, rerun=False, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT):
    return Crawler(name=name, delay=delay, timeout=timeout).run(delay)

def list_crawlers():
    return [x['Name'] for x in boto3.client('glue').get_crawlers()['Crawlers']]

def run_job(name, delay=DEFAULT_JOB_DELAY, timeout=None, **kargs):
    return Job(name=name, delay=delay, timeout=timeout).run(**kargs)

def list_jobs():
    return [x['Name'] for x in boto3.client('glue').get_jobs()['Jobs']]

def main():
    argv = sys.argv
    if len(argv) < 2:
        print('usage: gluettalax <command> [parameters]')
        print(' gluettalax list_crawlers')
        print(' gluettalax list_jobs')
        print(' gluettalax run_creawler <name>')
        print(' gluettalax run_job <name> [--param=value ...]')
        sys.exit(2)
    cmd = argv[1]
    try:
        if cmd == 'list_crawlers':
            print('\n'.join(list_crawlers()))
        elif cmd == 'run_crawler':
            if len(argv) < 3:
                raise ValueError('missing crawler name')
            name = argv[2]
            run_crawler(name)
        if cmd == 'list_jobs':
            print('\n'.join(list_jobs()))
        if cmd == 'run_job':
            if len(argv) < 3:
                raise ValueError('missing job name')
            name = argv[2]
            kargs = {}
            args = argv[3:]
            opt = None
            while args:
                arg = args.pop(0)
                if opt is not None:
                    value = arg
                    kargs[opt] = value
                    opt = None
                elif "=" in arg:
                    (opt, next_arg) = arg.split("=", 1)
                    if not opt.startswith('--'):
                        raise ValueError('invalid option')
                    opt = opt[2:]
                    args.insert(0, next_arg)
                else:
                    if not arg.startswith('--'):
                        raise ValueError('invalid option')
                    opt = arg[2:]
            if opt is not None:
                raise ValueError('missing value for {0}'.format(opt))
            if run_job(name, **kargs):
                sys.exit(0)
            else:
                sys.exit(1)
    except ValueError as ex:
        print(ex)
        sys.exit(1)
    except CrawlerNotFound as ex:
        print(ex)
        sys.exit(1)
    except JobNotFound as ex:
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
