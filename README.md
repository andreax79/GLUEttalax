# GLUEttalax
Glue ETL without constipation

[![Build Status](https://github.com/andreax79/GLUEttalax/workflows/Tests/badge.svg)](https://github.com/andreax79/GLUEttalax/actions)
[![PyPI version](https://badge.fury.io/py/gluettalax.svg)](https://badge.fury.io/py/GLUEttalax)
[![PyPI](https://img.shields.io/pypi/pyversions/GLUEttalax.svg)](https://pypi.org/project/GLUEttalax)
[![Downloads](https://pepy.tech/badge/GLUEttalax/month)](https://pepy.tech/project/GLUEttalax)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

```
usage: gluettalax <command> [parameters]

Commands:
 add_partition <db> <table> [--partition1=value...] [--location=path]
    Create a new Glue partition.
    Example: add_partition datalake usage --year=2019 --month=09

 add_partitions <db> <table> [s3_path]
    Create new Glue partitions in a given location.
    Example: add_partition datalake usage s3://example/usage/year=2020/month=10

 del_partition <db> <table> [--partition1=value...]
    Delete a Glue partition.
    Example: del_partition datalake usage --year=2019 --month=09

 help [command]
    Display information about commands.

 list_crawlers [pattern] [--noheaders]
    List Glue crawlers.
    Example: list_crawlers 'test*' --noheaders

 list_jobs [pattern] [--noheaders]
    List Glue jobs.
    Example: list_jobs 'test*'

 list_partitions <db> <table> [pattern] [--noheaders]
    List the partitions in a table.
    Example: list_partitions datalake usage

 list_runs [job_name] [--lines=num] [--noheaders]
    Print Glue jobs history.
    Example: list_runs my_batch_job --lines 10

 list_tables [pattern] [--noheaders]
    List Glue tables.
    Example: list_tables 'test*' --noheaders

 run_crawler <crawler_name> [--async] [--timeout=seconds]
    Run a crawler. If not async, wait until execution is finished.
    Example: run_crawler my_usage_crawler --async

 run_job <job_name> [--async] [--param1=value...]
    Run a Glue job. if not async, wait until execution is finished.
    Example: cmd_run_job --DATALAKE_BUCKET=test --THE_DATE=20191112 --HOUR=15

Command aliases:
 lsc -> list_crawlers
 lsj -> list_jobs
 runc -> run_crawler
 lsr -> list_runs
 runj -> run_job
 lsp -> list_partitions
 addp -> add_partition
 rmp -> del_partition
 lst -> list_tables
