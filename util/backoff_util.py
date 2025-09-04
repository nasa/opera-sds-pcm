"""Utility functions used with backoff/retry decorators"""
import boto3
import requests

from opera_commons.logger import logger


def fatal_code(err: requests.exceptions.RequestException) -> bool:
    """Only retry for common transient errors"""
    return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]


def backoff_logger(details):
    """Log details about the current backoff/retry"""
    logger.warning(
        f"Backing off {details['target']} function for {details['wait']:0.1f} "
        f"seconds after {details['tries']} tries."
    )
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")


def giveup_s3_client_upload_file(e):
    """
    giveup function for use with @backoff decorator. This only checks for a
    local-testing condition of running into an expired AWS CLI/SDK session token.
    """
    if isinstance(e, boto3.exceptions.Boto3Error):
        if isinstance(e, boto3.exceptions.S3UploadFailedError):
            if "ExpiredToken" in e.args[0]:
                logger.error("Local testing error. Give up immediately.")
                return True
    return False
