import boto3
import botocore
import time

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


class Crawler(object):

    def __init__(self, name, delay=DEFAULT_CRAWLER_DELAY, timeout=DEFAULT_CRAWLER_TIMEOUT):
        self.name = name
        self.delay = delay
        self.timeout = timeout
        self.glue = boto3.client('glue')

    @property
    def status(self):
        return self.glue.get_crawler(Name=self.name)['Crawler']

    @property
    def is_ready(self):
        return self.status['State'] == 'READY'

    def run(self, rerun=False):
        if rerun:
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
        job = self.glue.get_job(JobName=self.name)['Job']
        if self.timeout is None:
            self.timeout = minutes(job['Timeout'])

    def get_runs(self):
        return self.glue.get_job_runs(JobName=self.name)['JobRuns']

    def get_run_state(self, job_run_id):
        return self.glue.get_job_run(JobName=self.name, RunId=job_run_id)['JobRun']['JobRunState']

    def run(self, **kargs):
        arguments = dict([('--%s' % k, v) for k, v in kargs.items()])
        result = self.glue.start_job_run(JobName=self.name, Timeout=int(self.timeout/60), Arguments=arguments)
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

