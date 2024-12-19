"""Collection of AWS-related utilities"""

import concurrent.futures
import contextlib
import os
import threading
from pathlib import Path
from typing import Collection

import backoff
import boto3
from boto3.exceptions import Boto3Error
from mypy_boto3_s3 import S3Client

from commons.logger import logger
from util.backoff_util import giveup_s3_client_upload_file


def concurrent_s3_client_try_upload_file(bucket: str, key_prefix: str, files: Collection[Path]):
    """Upload s3 files concurrently, returning their s3 paths if all succeed."""
    logger.info(f"Uploading {len(files)} files to S3")
    max_workers = semaphore_size = min(8, os.cpu_count() + 4)
    sem = threading.Semaphore(semaphore_size)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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

        return s3paths


@backoff.on_exception(backoff.expo, exception=Boto3Error, max_tries=3, jitter=None, giveup=giveup_s3_client_upload_file)
def try_s3_client_try_upload_file(s3_client: S3Client = None, sem: threading.Semaphore = None, **kwargs):
    """
    Attempt to perform an s3 upload, retrying upon failure, returning back the S3 path.
    A default session-based S3 client is created on clients' behalf to facilitate parallelization of requests.
    """
    sem = sem if sem is not None else contextlib.nullcontext()
    with sem:
        if s3_client is None:
            s3_client = boto3.session.Session().client("s3")
        s3path = f's3://{kwargs["Bucket"]}/{kwargs["Key"]}'

        logger.info(f'Uploading to {s3path}')
        s3_client.upload_file(**kwargs)
        logger.info(f'Uploaded to {s3path}')
        return s3path