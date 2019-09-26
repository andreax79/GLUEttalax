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
__version__ = '0.3.0'
__all__ = [
    'Crawler',
    'CrawlerTimeout',
    'Job',
    'JobTimeout',
    'run_crawler',
    'list_crawlers',
    'run_job',
    'list_jobs',
    'list_runs'
]

def seconds(x): return x
def minutes(x): return x * 60

DEFAULT_CRAWLER_DELAY   = seconds(10)
DEFAULT_CRAWLER_TIMEOUT = minutes(10)
DEFAULT_JOB_DELAY       = seconds(10)

SUCCEEDED = 'SUCCEEDED'
FAILED = 'FAILED'

seconds_per_minute = 60
seconds_per_hour = 3600
seconds_per_day = 86400

TIME_INTERVALS = [1,
                  seconds_per_minute,
                  seconds_per_hour,
                  seconds_per_day]

SHORT_TIME_LABELS = [ 's', 'm', 'h', 'd' ]

def format_time(seconds=0):
    """
       Format a temporal interval in a human readable format

        :param seconds:      number of seconds
        :type seconds:       integer
        :return:             the formatted temporal interval
        :rtype:              str
    """
    negative = seconds < 0
    seconds = abs(seconds)
    empty = True
    cut = -1
    result = []
    for i in range(len(SHORT_TIME_LABELS)-1, -1, -1):
        if i == cut:
            break
        interval = TIME_INTERVALS[i];
        a = seconds // interval
        if a > 0 or (i == 0 and empty):
            if negative:
                part = "-%d" % int(a)
            else:
                part = str(int(a))
            part = part + SHORT_TIME_LABELS[i]
            result.append(part)
            seconds -= a * interval
            empty = False
    return " ".join(result)


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

    def __init__(self, name, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT, op_async=False):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.op_async = op_async
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
        if self.op_async:
            return
        start_time = time.time()
        while not self.is_ready:
            if time.time() > start_time + self.timeout:
                raise CrawlerTimeout()
            time.sleep(self.delay)


class JobTimeout(Exception):
    """ Raised when Glue Job a timeout error occurs """
    pass


class Job(object):

    def __init__(self, name, delay=DEFAULT_JOB_DELAY, timeout=None, op_async=False):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.op_async = op_async
        self.glue = boto3.client('glue')
        try:
            job = self.glue.get_job(JobName=self.name)['Job']
        except self.glue.exceptions.EntityNotFoundException as ex:
            raise JobNotFound(ex.message)
        if self.timeout is None:
            self.timeout = minutes(job['Timeout'])

    def get_runs(self):
        try:
            return list_runs(self.name)
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
        if self.op_async:
            return True
        while run_state not in [ SUCCEEDED, FAILED ]:
            if time.time() > start_time + self.timeout:
                raise JobTimeout()
            time.sleep(self.delay)
            run_state = self.get_run_state(job_run_id)
        return run_state == SUCCEEDED

def run_crawler(name, rerun=False, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT, op_async=False):
    timeout = int(timeout)
    return Crawler(name=name, delay=delay, timeout=timeout, op_async=op_async).run()

def list_crawlers(full=False):
    paginator = boto3.client('glue').get_paginator('get_crawlers')
    pages = paginator.paginate()
    crawlers = []
    for page in pages:
        for crawler in page['Crawlers']:
            crawlers.append(crawler if full else crawler['Name'])
    return crawlers

def run_job(name, delay=DEFAULT_JOB_DELAY, timeout=None, op_async=False, **kargs):
    return Job(name=name, delay=delay, timeout=timeout, op_async=op_async).run(**kargs)

def list_jobs(full=False):
    paginator = boto3.client('glue').get_paginator('get_jobs')
    pages = paginator.paginate()
    jobs = []
    for page in pages:
        for job in page['Jobs']:
            jobs.append(job if full else job['Name'])
    return jobs

def list_runs(name, lines=None, include_succeeded=True):
    paginator = boto3.client('glue').get_paginator('get_job_runs')
    pages = paginator.paginate(JobName=name)
    job_runs = []
    i = 0
    if lines:
        lines = int(lines)
    for page in pages:
        for job_run in page['JobRuns']:
            if not include_succeeded and job_run['JobRunState'] == SUCCEEDED:
                continue
            job_runs.append(job_run)
            i = i + 1
            if lines and i >= lines:
                break
        if lines and i >= lines:
            break
    return job_runs

def print_job_runs(name=None, include_succeeded=True, lines=None, header=True):
    fmt = '{JobRunState:>10} {AllocatedCapacity:>4} {ExecutionTime:10}  {StartedOn:19}   {JobName} {Arguments}'
    if header:
        print(fmt.format(
            JobRunState='Status',
            AllocatedCapacity='Cap',
            ExecutionTime='Exec time',
            StartedOn='Start time',
            JobName='Name and arguments',
            Arguments=''))
        print('-' * 70)
    if name is None:
        for job in list_jobs(full=True):
            print_job_runs(name=job['Name'], include_succeeded=include_succeeded, lines=lines or 1, header=False)
    else:
        try:
            for run in list_runs(name, include_succeeded=include_succeeded, lines=lines):
                run['ExecutionTime'] = format_time(run['ExecutionTime'])
                run['StartedOn'] = run['StartedOn'].isoformat(' ').split('.')[0]
                run['Arguments'] = ' '.join(['%s %s' % (k, v) for k, v in run['Arguments'].items()])
                print(fmt.format(**run))
        except IOError: # e.g. Broken pipe
            pass

_cmds = []

def cmd(f):
    " Command decorator "
    f.cmd = f.__name__[4:] if f.__name__.startswith('cmd_') else f.__name__
    _cmds.append(f)
    return f

def alias(*aliases):
    " Command alias decorator - accept a aliases as arguments "
    def wrap(f):
        def wrapped_f(*args, **kargs):
            f(*args, **kargs)
        wrapped_f.__name__ = f.__name__
        wrapped_f.help_text = getattr(f, 'help_text', None)
        wrapped_f.aliases = aliases
        return wrapped_f
    return wrap

def short_help(help_text):
    " Command help decorator "
    def wrap(f):
        def wrapped_f(*args, **kargs):
            f(*args, **kargs)
        wrapped_f.__name__ = f.__name__
        wrapped_f.help_text = help_text
        wrapped_f.aliases = getattr(f, 'aliases', None)
        return wrapped_f
    return wrap

def parse_args(args, defaults=None):
    " Parse command lines arguments "
    kargs = dict(defaults or {})
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
            if arg == '-async' or arg == '--async':
                kargs['op_async'] = True
            elif arg.startswith('--'):
                opt = arg[2:]
            else:
                raise ValueError('invalid option')
    if opt is not None:
        raise ValueError('missing value for {0}'.format(opt))
    return kargs

@cmd
@alias('lsc')
@short_help('\n\tPrint crawlers list')
def cmd_list_crawlers(argv, header=True):
    fmt = '{Name:40} {State:10} {CrawlElapsedTime}'
    if header:
        print(fmt.format(
            Name='Name',
            State='Status',
            CrawlElapsedTime=''))
        print('-' * 70)
    for crawler in list_crawlers(full=True):
        if crawler['State'] == 'RUNNING':
            crawler['CrawlElapsedTime'] = format_time(crawler['CrawlElapsedTime']/1000)
        else:
            crawler['CrawlElapsedTime'] = ''
        print(fmt.format(**crawler))

@cmd
@alias('lsj')
@short_help('\n\tPrint Glue jobs list')
def cmd_list_jobs(argv, header=True):
    fmt = '{Name:40} {AllocatedCapacity:8}  {MaxConcurrentRuns:10}'
    if header:
        print(fmt.format(
            Name='Name',
            AllocatedCapacity='Capacity',
            MaxConcurrentRuns='Max concurrent'))
        print('-' * 70)
    for job in list_jobs(full=True):
        job['MaxConcurrentRuns'] = job.get('ExecutionProperty', {}).get('MaxConcurrentRuns', '-')
        print(fmt.format(**job))

@cmd
@alias('runc')
@short_help('run_job <name> [--async] [--param=value ...]\n\tRun a Glue job')
def cmd_run_crawler(argv):
    if len(argv) < 2:
        raise ValueError('missing crawler name')
    name = argv[1]
    default_args = { 'op_async': False, 'timeout': DEFAULT_CRAWLER_TIMEOUT }
    kargs = parse_args(argv[2:], default_args)
    run_crawler(name, **kargs)

@cmd
@alias('lsr')
@short_help('[name] [--lines=num]\n\tPrint jobs history')
def cmd_list_runs(argv):
    if len(argv) > 1 and not argv[1].startswith('-'):
        name = argv[1]
        argv.pop(1)
    else:
        name = None
    default_args = { 'lines': None }
    kargs = parse_args(argv[1:], default_args)
    print_job_runs(name, lines=kargs['lines'])

@cmd
@alias('runj')
@short_help('<name> [--async] [--param=value ...]\n\tRun a Glue job')
def cmd_run_job(argv):
    if len(argv) < 2:
        raise ValueError('missing job name')
    name = argv[1]
    default_args = { 'op_async': False }
    kargs = parse_args(argv[2:], default_args)
    if run_job(name, **kargs):
        sys.exit(0)
    else:
        sys.exit(1)

@cmd
@alias('-h')
def cmd_help(argv):
    print('usage: gluettalax <command> [parameters]')
    print('')
    print('Commands:')
    # print(' list_crawlers')
    # print(' list_jobs')
    # print(' list_runs [name] [--lines=num]')
    # print(' run_crawler <name> [--async] [--timeout=seconds]')
    # print(' run_job <name> [--async] [--param=value ...]')
    for f in _cmds:
        help_text = getattr(f, 'help_text', '') or ''
        print(' {} {}'.format(f.cmd, help_text))
        print('')
    print('Command aliases:')
    for f in _cmds:
        aliases = sorted(getattr(f, 'aliases', []))
        if aliases and f.cmd != 'help':
            print(' {} -> {}'.format(' '.join(aliases), f.cmd))

def main():
    argv = sys.argv
    if len(argv) < 2:
        cmd_help(argv[1:])
        sys.exit(2)
    cmd = argv[1]
    try:
        for f in _cmds:
            if cmd == getattr(f, 'cmd') or cmd in getattr(f, 'aliases', []):
                f(argv[1:])
                sys.exit(0)
        print('Command not found')
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
