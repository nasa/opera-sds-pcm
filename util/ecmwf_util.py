
"""
==============
ecmwf_util.py
==============

Contains utility functions for working with ECMWF (Troposphere) weather files.

"""

from datetime import datetime
from urllib.parse import urlparse

from commons.logger import logger

import botocore
import boto3

s3_client = boto3.client("s3")



def check_s3_for_ecmwf(ecmwf_s3_uri: str):
    """
    Checks for the existence of an ECMWF (troposphere) file in S3.

    Parameters
    ----------
    ecmwf_s3_uri : str
        S3 URI to ECMWF file to locate. Should begin with s3://

    Returns
    -------
    True if the file exists in S3, False otherwise

    Raises
    ------
    ValueError
        If the provided URI cannot be parsed as-expected.

    """
    try:
        parsed_uri = urlparse(ecmwf_s3_uri)
    except ValueError as err:
        logger.error("Failed to parse ECMWF S3 URI %s, reason: %s", ecmwf_s3_uri, str(err))
        raise

    bucket = parsed_uri.netloc
    key = parsed_uri.path

    # Strip leading forward slash
    if key.startswith("/"):
        key = key[1:]

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # File does not exist
            logger.warning("ECMWF file %s does not exist in bucket %s", key, bucket)
            return False
        else:
            # Some kind of unexpected error
            raise

    # If here, the file exists in S3
    return True

def ecmwf_key_for_datetime(dt : datetime):
    """
    Derives the expected S3 key of an ECMWF file corresponding to the provided
    datetime. This does not include a bucket name, or any key portions that
    occur prior to the date (YYYYMMDD) of the ECMWF file.

    Parameters
    ----------
    dt : datetime
        The datetime object to derive the ECMWF key path for.

    Returns
    -------
    ecmwf_key : str
        The S3 key to the location of the ECMWF file that corresponds to the
        provided datetime.

    """
    # Derive the YYYYMMDD key prefix
    prefix = dt.strftime("%Y%m%d")

    # Determine the 6-hour time window quadrant that corresponds to the provided hour
    if 0 <= dt.hour < 6:
        hour = "00"
    elif 6 <= dt.hour < 12:
        hour = "06"
    elif 12 <= dt.hour < 18:
        hour = "12"
    else:
        hour = "18"

    key_template = "{prefix}/D{month}{day}{hour}00{month}{day}{hour}001.subset.zz.nc"

    return key_template.format(
        prefix=prefix, month=dt.strftime("%m"), day=dt.strftime("%d"), hour=hour
    )


def find_ecmwf_for_datetime(dt : datetime, s3_prefix="s3://opera-ancillaries/ecmwf"):
    """
    Returns the S3 path to an ECMWF file corresponding to the provided datetime,
    if it exists.

    Parameters
    ----------
    dt : datetime.datetime
        Datetime object corresponding to the ECMWF file to find.
    s3_prefix : str, optional
        Prefix to be appended to the datetime-specific portion of the S3 URI.
        Should always start with "s3://". Defaults to s3://opera-ancillaries/ecmwf.

    Returns
    -------
    S3 URI for the location of the desired ECMWF file, if it exists. None otherwise.

    Raises
    ------
    ValueError
        If the provided s3_prefix does not begin with "s3://"

    """
    if not s3_prefix.startswith("s3://"):
        raise ValueError(f"Invalid S3 prefix ({s3_prefix}) provided. Must begin with \"s3://\"")

    ecmwf_key = ecmwf_key_for_datetime(dt)

    ecmwf_s3_path = "/".join([s3_prefix, ecmwf_key])

    return ecmwf_s3_path if check_s3_for_ecmwf(ecmwf_s3_path) else None