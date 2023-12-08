"""Collection of AWS-related utilities"""
import concurrent.futures
import contextlib
import logging
import threading
import os
from pathlib import Path
from typing import Collection

import backoff
import boto3
from boto3.exceptions import Boto3Error
from more_itertools import chunked
from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


def concurrent_s3_client_try_upload_file(bucket: str, key_prefix: str, files: Collection[Path]):
    """Upload s3 files concurrently, returning their s3 paths if all succeed."""
    logger.info(f"Uploading {len(files)} files to S3")
    max_workers = semaphore_size = min(8, os.cpu_count() + 4)
    sem = threading.Semaphore(semaphore_size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        s3pathss = []
        futures = []
        for f in files:
            sem.acquire()
            future = executor.submit(
                try_s3_client_try_upload_file,
                Filename=str(f),
                Bucket=bucket,
                Key=f"{key_prefix}/{f.name}",
            )
            future.add_done_callback(lambda _: sem.release())
            futures.append(future)
        s3paths = [s3path := future.result() for future in concurrent.futures.as_completed(futures)]
        s3pathss.extend(s3paths)

        return s3pathss


def giveup_s3_client_upload_file(e):
    """
    giveup function for use with @backoff decorator. This only checks for a local-testing condition of running into
    an expired AWS CLI/SDK session token.
    """
    if isinstance(e, boto3.exceptions.Boto3Error):
        if isinstance(e, boto3.exceptions.S3UploadFailedError):
            if "ExpiredToken" in e.args[0]:
                logger.error("Local testing error. Give up immediately.")
                return True
    return False


@backoff.on_exception(backoff.expo, exception=Boto3Error, max_tries=3, jitter=None, giveup=giveup_s3_client_upload_file)
def try_s3_client_try_upload_file(s3_client: S3Client = None, **kwargs):
    """
    Attempt to perform an s3 upload, retrying upon failure, returning back the S3 path.
    A default session-based S3 client is created on clients' behalf to facilitate parallelization of requests.
    """
    if s3_client is None:
        s3_client = boto3.session.Session().client("s3")
    s3path = f's3://{kwargs["Bucket"]}/{kwargs["Key"]}'

    logger.info(f'Uploading to {s3path}')
    s3_client.upload_file(**kwargs)
    logger.info(f'Uploaded to {s3path}')
    return s3path