#!/usr/bin/env python3
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
import sys
import botocore.credentials
import botocore.session
from botocore.exceptions import BotoCoreError
import boto3
import time
import fnmatch
from collections import namedtuple
from inspect import currentframe, getframeinfo
from urllib.parse import urlparse

__author__ = 'Andrea Bonomi <andrea.bonomi@gmail.com>'
__version__ = '1.1.1'
__all__ = [
    'CrawlerTimeout',
    'GluettalaxException',
    'CrawlerTimeout',
    'CrawlerNotFound',
    'JobNotFound',
    'JobTimeout',
    'JobConcurrentRunsExceeded',
    'TableNotFound',
    'PartitionNotFound',
    'PartitionAlreadyExists',
    'InvalidOption',
    'GluettalaxCommandNotFound',
    'Crawler',
    'Job',
    'run_crawler',
    'list_crawlers',
    'run_job',
    'list_jobs',
    'list_runs',
    'list_partitions',
    'add_partition',
    'add_partitions_by_location',
    'delete_partition',
    'main',
    'gluettalax',
]


def seconds(x):
    return x


def minutes(x):
    return x * 60


def hours(x):
    return minutes(x) * 60


DEFAULT_CRAWLER_DELAY = seconds(10)
DEFAULT_CRAWLER_TIMEOUT = minutes(10)
DEFAULT_JOB_DELAY = seconds(10)

SUCCEEDED = 'SUCCEEDED'
FAILED = 'FAILED'

TIME_LABELS = (('s', 1), ('m', minutes(1)), ('h', hours(1)), ('d', hours(24)))


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
    for i in range(len(TIME_LABELS) - 1, -1, -1):
        if i == cut:
            break
        interval = TIME_LABELS[i][1]
        a = seconds // interval
        if a > 0 or (i == 0 and empty):
            if negative:
                part = '-' + str(int(a))
            else:
                part = str(int(a))
            part = part + TIME_LABELS[i][0]
            result.append(part)
            seconds -= a * interval
            empty = False
    return " ".join(result)


class GluettalaxException(Exception):
    "Generic GLUEttalax exception"


class GluettalaxWarning(GluettalaxException):
    "Generic GLUEttalax warning (exit with 0 status)"


class CrawlerTimeout(GluettalaxException):
    "Glue crawler timeout error"


class CrawlerNotFound(GluettalaxException):
    "Glue crawler not found"


class JobNotFound(GluettalaxException):
    "Glue job not found"


class JobTimeout(GluettalaxException):
    "Glue job timeout error"


class JobConcurrentRunsExceeded(GluettalaxException):
    "Too many concurrent execution of a Glue job"


class TableNotFound(GluettalaxException):
    "Glue table not found"


class PartitionNotFound(GluettalaxException):
    "Glue partition not found"


class PartitionAlreadyExists(GluettalaxWarning):
    "Glue partition already exists"


class InvalidOption(GluettalaxException):
    "Invalid option (command line argument)"


class GluettalaxCommandNotFound(GluettalaxException):
    "GLUEttalax command not found"


def get_glue():
    # botocore session cache
    cli_cache = os.path.join(os.path.expanduser('~'), '.aws/cli/cache')
    session = botocore.session.get_session()
    session.get_component('credential_provider').get_provider('assume-role').cache = botocore.credentials.JSONFileCache(
        cli_cache
    )
    # create boto3 client from session
    if 'AWS_REGION' in os.environ:
        return boto3.Session(botocore_session=session).client('glue', os.environ['AWS_REGION'])
    else:
        return boto3.Session(botocore_session=session).client('glue')


class Crawler(object):
    def __init__(self, name, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT, op_async=False):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.op_async = op_async
        self.glue = get_glue()

    @property
    def status(self):
        "Return the crawler status"
        try:
            return self.glue.get_crawler(Name=self.name)['Crawler']
        except self.glue.exceptions.EntityNotFoundException:
            raise CrawlerNotFound('Crawler {} not found'.format(self.name))

    @property
    def is_ready(self):
        "Return true if the craweler is in READY state"
        return self.status['State'] == 'READY'

    def run(self, rerun=False):
        """
        Start the crawler
        If the crawler is already running, restart the crawler only if rerun is True
        """
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


class Job(object):
    def __init__(self, name, delay=DEFAULT_JOB_DELAY, timeout=None, op_async=False):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.op_async = op_async
        self.glue = boto3.client('glue')
        try:
            job = self.glue.get_job(JobName=self.name)['Job']
        except self.glue.exceptions.EntityNotFoundException:
            raise JobNotFound('Job {} not found'.format(self.name))
        if self.timeout is None:
            self.timeout = minutes(job['Timeout'])

    def get_runs(self):
        try:
            return list_runs(self.name)
        except self.glue.exceptions.EntityNotFoundException:
            raise JobNotFound('Job {} not found'.format(self.name))

    def get_run_state(self, job_run_id):
        try:
            return self.glue.get_job_run(JobName=self.name, RunId=job_run_id)['JobRun']['JobRunState']
        except self.glue.exceptions.EntityNotFoundException:
            raise JobNotFound('Job {} not found'.format(self.name))

    def run(self, **kargs):
        arguments = dict([('--' + k, v) for k, v in kargs.items()])
        try:
            result = self.glue.start_job_run(JobName=self.name, Timeout=int(self.timeout / 60), Arguments=arguments)
        except self.glue.exceptions.EntityNotFoundException:
            raise JobNotFound('Job {} not found'.format(self.name))
        except self.glue.exceptions.ConcurrentRunsExceededException as ex:
            raise JobConcurrentRunsExceeded(ex.message)
        job_run_id = result['JobRunId']
        start_time = time.time()
        run_state = self.get_run_state(job_run_id)
        if self.op_async:
            return True
        while run_state not in [SUCCEEDED, FAILED]:
            if time.time() > start_time + self.timeout:
                raise JobTimeout()
            time.sleep(self.delay)
            run_state = self.get_run_state(job_run_id)
        return run_state == SUCCEEDED


def run_crawler(name, rerun=False, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT, op_async=False):
    timeout = int(timeout)
    return Crawler(name=name, delay=delay, timeout=timeout, op_async=op_async).run()


def list_crawlers(full=False):
    glue = get_glue()
    paginator = glue.get_paginator('get_crawlers')
    pages = paginator.paginate()
    crawlers = []
    for page in pages:
        for crawler in page['Crawlers']:
            crawlers.append(crawler if full else crawler['Name'])
    return crawlers


def run_job(name, delay=DEFAULT_JOB_DELAY, timeout=None, op_async=False, **kargs):
    return Job(name=name, delay=delay, timeout=timeout, op_async=op_async).run(**kargs)


def list_jobs(full=False):
    glue = get_glue()
    paginator = glue.get_paginator('get_jobs')
    pages = paginator.paginate()
    jobs = []
    for page in pages:
        for job in page['Jobs']:
            jobs.append(job if full else job['Name'])
    return jobs


def list_runs(name, lines=None, include_succeeded=True):
    glue = get_glue()
    try:
        paginator = glue.get_paginator('get_job_runs')
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
    except glue.exceptions.EntityNotFoundException:
        raise JobNotFound('Job {} not found'.format(name))


def print_job_runs(name=None, include_succeeded=True, lines=None, header=True):
    fmt = '{JobRunState:>10} {AllocatedCapacity:>4} {ExecutionTime:10}  {StartedOn:19}   {JobName} {Arguments}'
    if header:
        print(
            fmt.format(
                JobRunState='Status',
                AllocatedCapacity='Cap',
                ExecutionTime='Exec time',
                StartedOn='Start time',
                JobName='Name and arguments',
                Arguments='',
            )
        )
        print('-' * 70)
    if name is None:
        for job in list_jobs(full=True):
            print_job_runs(name=job['Name'], include_succeeded=include_succeeded, lines=lines or 1, header=False)
    else:
        try:
            for run in list_runs(name, include_succeeded=include_succeeded, lines=lines):
                run['ExecutionTime'] = format_time(run['ExecutionTime'])
                run['StartedOn'] = run['StartedOn'].isoformat(' ').split('.')[0]
                run['Arguments'] = ' '.join([k + ' ' + v for k, v in run['Arguments'].items()])
                print(fmt.format(**run))
        except IOError:  # e.g. Broken pipe
            pass


def get_partition_values(kargs, partition_keys):
    "Check and convert command line arguments to partitions dict"
    if len(kargs) != len(partition_keys):
        raise InvalidOption(
            '{} partitions required ({})'.format(
                len(partition_keys), ' '.join(['--{}=XXX'.format(x['Name']) for x in partition_keys])
            )
        )
    try:
        return [kargs[x['Name']] for x in partition_keys]
    except KeyError as ex:
        raise InvalidOption('missing --{} argument'.format(ex.args[0]))


Partitions = namedtuple('Partitions', ['partition_keys', 'max_lengths', 'data'])


def list_partitions(db, table, header=True):
    "List Glue partitions"
    # Get table metadata
    glue = get_glue()
    try:
        glue_table = glue.get_table(DatabaseName=db, Name=table)
    except glue.exceptions.EntityNotFoundException:
        raise TableNotFound('Table {} not found'.format(table))
    partition_keys = [x['Name'] for x in glue_table['Table']['PartitionKeys']]
    # Get partitions
    data = []
    lengths = [len(x) for x in partition_keys]  # calculate the labels lengths
    paginator = glue.get_paginator('get_partitions')
    pages = paginator.paginate(DatabaseName=db, TableName=table)
    for page in pages:
        for partition in page['Partitions']:
            values = partition['Values']
            lengths = [max(l, len(str(values[i]))) for i, l in enumerate(lengths)]  # values lengths
            location = partition['StorageDescriptor'].get('Location', '-')
            data.append(values + [location])
    data = sorted(data, key=lambda row: row[-1])
    return Partitions(partition_keys, lengths, data)


def add_partitions_by_location(db, table, location, kargs):
    glue = get_glue()
    s3 = boto3.resource('s3')
    # Get s3 partitions
    url = urlparse(location)
    bucket = s3.Bucket(url.netloc)
    bucket_files = [x.key for x in bucket.objects.filter(Prefix=url.path[1:]).all()]
    bucket_dirs = sorted(list(set([os.path.dirname(x) for x in bucket_files])))
    # Parsing table info required to create partitions from table
    glue_table = glue.get_table(DatabaseName=db, Name=table)
    input_format = glue_table['Table']['StorageDescriptor']['InputFormat']
    output_format = glue_table['Table']['StorageDescriptor']['OutputFormat']
    serde_info = glue_table['Table']['StorageDescriptor']['SerdeInfo']
    partition_keys = glue_table['Table']['PartitionKeys']
    # Iterate over dirs
    for path in bucket_dirs:
        partition_url = 's3://{}/{}/'.format(url.netloc, path)
        parts = path.split('/')
        try:
            index = [i for i, k in enumerate(parts) if k.startswith(partition_keys[0]['Name'] + '=')][0]
        except Exception:
            print('Skip {}'.format(partition_url))
        parts = parts[index:]
        partition_values = []
        for i, k in enumerate(partition_keys):
            if parts[i].startswith(k['Name'] + '='):
                partition_values.append(parts[i].split('=', 1)[1])
        if len(partition_values) != len(partition_keys):
            print('Skip {}'.format(partition_url))
        # Add partition
        partition_input = {
            'Values': partition_values,
            'StorageDescriptor': {
                'Location': partition_url,
                'InputFormat': input_format,
                'OutputFormat': output_format,
                'SerdeInfo': serde_info,
            },
        }
        try:
            glue.create_partition(DatabaseName=db, TableName=table, PartitionInput=partition_input)
            print('Partition [{}] added'.format(path))
        except glue.exceptions.AlreadyExistsException:
            print('Partition [{}] already exists'.format(path))


def add_partition(db, table, kargs):
    "Create a new Glue partition"
    glue = get_glue()
    location = kargs.get('location')
    if 'location' in kargs:
        del kargs['location']
    # Get glue table
    glue_table = glue.get_table(DatabaseName=db, Name=table)
    # Check partition keys
    partition_keys = glue_table['Table']['PartitionKeys']
    partition_values = get_partition_values(kargs, partition_keys)
    # Parsing table info required to create partitions from table
    input_format = glue_table['Table']['StorageDescriptor']['InputFormat']
    output_format = glue_table['Table']['StorageDescriptor']['OutputFormat']
    table_location = glue_table['Table']['StorageDescriptor']['Location']
    serde_info = glue_table['Table']['StorageDescriptor']['SerdeInfo']
    if not location:
        if not table_location.endswith('/'):
            table_location = table_location + '/'
        if all([x.startswith('partition_') for x in kargs]):
            # not-Hive style partitions
            path = '/'.join(partition_values) + '/'
        else:
            # Hive style partitions
            path = '/'.join(['{}={}'.format(x['Name'], kargs[x['Name']]) for x in partition_keys]) + '/'
        location = table_location + path
    partition_input = {
        'Values': partition_values,
        'StorageDescriptor': {
            'Location': location,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info,
        },
    }
    try:
        return glue.create_partition(DatabaseName=db, TableName=table, PartitionInput=partition_input)
    except glue.exceptions.AlreadyExistsException:
        raise PartitionAlreadyExists('Partition [{}] already exists'.format(', '.join(partition_values)))


def delete_partition(db, table, kargs):
    "Deletes a Glue partition"
    glue = get_glue()
    # Get glue table
    glue_table = glue.get_table(DatabaseName=db, Name=table)
    # Check partition keys
    partition_keys = glue_table['Table']['PartitionKeys']
    partition_values = get_partition_values(kargs, partition_keys)
    # Delete partition
    try:
        return glue.delete_partition(DatabaseName=db, TableName=table, PartitionValues=partition_values)
    except glue.exceptions.EntityNotFoundException:
        raise PartitionNotFound('Partition [{}] not found'.format(', '.join(partition_values)))


Table = namedtuple('Table', ['table_name', 'database_name'])


def list_tables():
    glue = get_glue()
    response = glue.search_tables()
    tables = []
    while response:
        for table in response['TableList']:
            tables.append(Table(table_name=table['Name'], database_name=table['DatabaseName']))
        if response.get('NextToken'):
            response = glue.search_tables(NextToken=response.get('NextToken'))
        else:
            response = None
    return tables


_cmds = []


def cmd(f):
    "Command decorator"
    f.cmd = f.__name__[4:] if f.__name__.startswith('cmd_') else f.__name__
    _cmds.append(f)
    return f


def alias(*aliases):
    "Command alias decorator - accept a aliases as arguments"

    def wrap(f):
        def wrapped_f(*args, **kargs):
            f(*args, **kargs)

        wrapped_f.__name__ = f.__name__
        wrapped_f.usage = getattr(f, 'usage', None)
        wrapped_f.__doc__ = getattr(f, '__doc__', None)
        wrapped_f.aliases = aliases
        return wrapped_f

    return wrap


def usage(usage):
    "Command usage decorator"

    def wrap(f):
        def wrapped_f(*args, **kargs):
            f(*args, **kargs)

        wrapped_f.__name__ = f.__name__
        wrapped_f.usage = usage
        wrapped_f.aliases = getattr(f, 'aliases', None)
        wrapped_f.__doc__ = getattr(f, '__doc__', None)
        return wrapped_f

    return wrap


def this_fn():
    "Return the caller function"
    caller = currentframe().f_back
    func_name = getframeinfo(caller)[2]
    return caller.f_back.f_locals.get(func_name, caller.f_globals.get(func_name))


def parse_usage(usage):
    "Parse usage help line"
    usage = usage.split('\n')[0].split()
    required = []
    optionals = []
    arguments = {}
    while usage:
        item = usage.pop(0)
        if not item.startswith('['):
            required.append(item)
        else:
            item = item.strip('[]')
            if item[0] != '-':
                optionals.append(item)
            else:
                item = item.lstrip('-')
                if '=' in item:
                    item = item.split('=')[0]
                    arguments[item] = str
                else:
                    arguments[item] = bool
    return required, optionals, arguments


def parse_args(args, usage, defaults=None):
    "Parse command lines arguments"
    required, optionals, arguments = parse_usage(usage)
    result = []
    kargs = dict(defaults or {})
    opt = None
    if args:
        args.pop(0)  # args[0] is the command
    while args:
        arg = args.pop(0)
        if opt is not None:  # arguments value
            value = arg
            kargs[opt] = value
            opt = None
        elif required:  # required positional argument
            result.append(arg)
            required.pop(0)
        elif optionals and arg[0] != '-':  # optional positional argument
            result.append(arg)
            optionals.pop(0)
        elif "=" in arg:  # argument --key=value
            (opt, next_arg) = arg.split("=", 1)
            if not opt.startswith('--'):
                raise InvalidOption('invalid option: ' + arg)
            opt = opt[2:]
            args.insert(0, next_arg)
        else:
            if not arg.startswith('--'):
                raise InvalidOption('invalid option: ' + arg)
            t = arg[2:]
            if arguments.get(t) == bool:  # boolean arg
                kargs['op_' + t] = True
            else:
                opt = t
    if opt is not None:
        raise InvalidOption('missing value for {0}'.format(opt))
    if required:  # check missing required values
        raise InvalidOption('missing {}'.format(required.pop(0)))
    while optionals:  # add missing optional values
        result.append(None)
        optionals.pop()
    if not result:  # no positional argument
        return kargs
    else:
        result.append(kargs)
        return result


@cmd
@alias('lsc')
@usage('[pattern] [--noheaders]')
def cmd_list_crawlers(argv):
    """
    List Glue crawlers.
    Example: list_crawlers 'test*' --noheaders
    """
    default_args = {'op_noheaders': False}
    pattern, kargs = parse_args(argv, this_fn().usage, default_args)
    header = not kargs['op_noheaders']
    fmt = '{Name:40} {State:10} {CrawlElapsedTime}'
    if header:
        print(fmt.format(Name='Name', State='Status', CrawlElapsedTime=''))
        print('-' * 70)
    for crawler in list_crawlers(full=True):
        if not pattern or fnmatch.fnmatch(crawler['Name'], pattern):
            if crawler['State'] == 'RUNNING':
                crawler['CrawlElapsedTime'] = format_time(crawler['CrawlElapsedTime'] / 1000)
            else:
                crawler['CrawlElapsedTime'] = ''
            print(fmt.format(**crawler))


@cmd
@alias('lsj')
@usage('[pattern] [--noheaders]')
def cmd_list_jobs(argv):
    """
    List Glue jobs.
    Example: list_jobs 'test*'
    """
    default_args = {'op_noheaders': False}
    pattern, kargs = parse_args(argv, this_fn().usage, default_args)
    header = not kargs['op_noheaders']
    fmt = '{Name:40} {AllocatedCapacity:8}  {MaxConcurrentRuns:10}'
    if header:
        print(fmt.format(Name='Name', AllocatedCapacity='Capacity', MaxConcurrentRuns='Max concurrent'))
        print('-' * 70)
    for job in list_jobs(full=True):
        if not pattern or fnmatch.fnmatch(job['Name'], pattern):
            job['MaxConcurrentRuns'] = job.get('ExecutionProperty', {}).get('MaxConcurrentRuns', '-')
            print(fmt.format(**job))


@cmd
@alias('runc')
@usage('<crawler_name> [--async] [--timeout=seconds]')
def cmd_run_crawler(argv):
    """
    Run a crawler. If not async, wait until execution is finished.
    Example: run_crawler my_usage_crawler --async
    """
    default_args = {'op_async': False, 'timeout': DEFAULT_CRAWLER_TIMEOUT}
    name, kargs = parse_args(argv, this_fn().usage, default_args)
    run_crawler(name, **kargs)


@cmd
@alias('lsr')
@usage('[job_name] [--lines=num] [--noheaders]')
def cmd_list_runs(argv):
    """
    Print Glue jobs history.
    Example: list_runs my_batch_job --lines 10
    """
    default_args = {'lines': None, 'op_noheaders': False}
    name, kargs = parse_args(argv, this_fn().usage, default_args)
    header = not kargs['op_noheaders']
    print_job_runs(name, lines=kargs['lines'], header=header)


@cmd
@alias('runj')
@usage('<job_name> [--async] [--param1=value...]')
def cmd_run_job(argv):
    """
    Run a Glue job. if not async, wait until execution is finished.
    Example: cmd_run_job --DATALAKE_BUCKET=test --THE_DATE=20191112 --HOUR=15
    """
    default_args = {'op_async': False}
    name, kargs = parse_args(argv, this_fn().usage, default_args)
    return 0 if run_job(name, **kargs) else 0


@cmd
@alias('lsp')
@usage('<db> <table> [pattern] [--noheaders]')
def cmd_list_partitions(argv):
    """
    List the partitions in a table.
    Example: list_partitions datalake usage
    """
    default_args = {'op_noheaders': False}
    db, table, pattern, kargs = parse_args(argv, this_fn().usage, default_args)
    header = not kargs['op_noheaders']
    result = list_partitions(db, table, header)
    fmt = '  '.join(['{:%d}' % x for x in result.max_lengths]) + '  {}'
    # Print header
    if header:
        print(fmt.format(*(result.partition_keys + ['Location'])))
        print('-' * 70)
    # Print partitions
    for line in result.data:
        if not pattern or any([fnmatch.fnmatch(x, pattern) for x in line]):
            print(fmt.format(*line))


@cmd
@alias('addp')
@usage('<db> <table> [--partition1=value...] [--location=path]')
def cmd_add_partition(argv):
    """
    Create a new Glue partition.
    Example: add_partition datalake usage --year=2019 --month=09
    """
    db, table, kargs = parse_args(argv, this_fn().usage)
    add_partition(db, table, kargs)
    print('Partition added')


@cmd
@usage('<db> <table> [s3_path]')
def cmd_add_partitions(argv):
    """
    Create new Glue partitions in a given location.
    Example: add_partition datalake usage s3://example/usage/year=2020/month=10
    """
    db, table, location, kargs = parse_args(argv, this_fn().usage)
    add_partitions_by_location(db, table, location, kargs)


@cmd
@alias('rmp')
@usage('<db> <table> [--partition1=value...]')
def cmd_del_partition(argv):
    """
    Delete a Glue partition.
    Example: del_partition datalake usage --year=2019 --month=09
    """
    db, table, kargs = parse_args(argv, this_fn().usage)
    delete_partition(db, table, kargs)
    print('Partition deleted')


@cmd
@alias('lst')
@usage('[pattern] [--noheaders]')
def cmd_list_tables(argv):
    """
    List Glue tables.
    Example: list_tables 'test*' --noheaders
    """
    default_args = {'op_noheaders': False}
    pattern, kargs = parse_args(argv, this_fn().usage, default_args)
    header = not kargs['op_noheaders']
    fmt = '{database_name:40} {table_name}'
    if header:
        print(fmt.format(database_name='Database', table_name='Name'))
        print('-' * 70)
    for table in list_tables():
        if not pattern or fnmatch.fnmatch(table.table_name, pattern):
            print(fmt.format(database_name=table.database_name, table_name=table.table_name))


@cmd
@alias('-h')
@usage('[command]')
def cmd_help(argv):
    """
    Display information about commands.
    """
    command, kargs = parse_args(argv, this_fn().usage)
    if command:
        f = lookup_cmd(command)
        usage_text = getattr(f, 'usage', '')
        help_text = (getattr(f, '__doc__', '') or '').rstrip('\n\t ')
        print('usage: gluettalax {} {} {}'.format(f.cmd, usage_text, help_text))
    else:
        print('usage: gluettalax <command> [parameters]')
        print('')
        print('Commands:')
        for f in sorted(_cmds, key=lambda x: getattr(x, 'cmd')):
            usage_text = getattr(f, 'usage', '')
            help_text = (getattr(f, '__doc__', '') or '').rstrip('\n\t ')
            print(' {} {} {}'.format(f.cmd, usage_text, help_text))
            print('')
        print('Command aliases:')
        for f in _cmds:
            aliases = sorted(getattr(f, 'aliases') or [])
            if aliases and f.cmd != 'help':
                print(' {} -> {}'.format(' '.join(aliases), f.cmd))


def lookup_cmd(cmd):
    for f in _cmds:
        if cmd == getattr(f, 'cmd') or cmd in (getattr(f, 'aliases') or []):
            return f
    raise GluettalaxCommandNotFound('Invalid command "{}"; use "help" for a list.'.format(cmd))


def main(argv):
    if len(argv) < 2:
        cmd_help(argv[1:])
        return 2
    try:
        f = lookup_cmd(argv[1])
        f(argv[1:])
        return 0
    except GluettalaxWarning as ex:
        print(ex)
        return 0
    except GluettalaxException as ex:
        print(ex)
        return 1
    except BotoCoreError as ex:
        print(ex)
        return 1


def gluettalax(*argv):
    "Run a Gluettalax command and return the exit code"
    return main(['gluettalax'] + list(argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
