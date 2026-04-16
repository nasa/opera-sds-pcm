#!/usr/bin/env python3

"""
Script to submit L4_TROPO jobs for processing S3 objects.

This script can filter S3 objects using either a prefix, a single date, or a date range.
It will submit jobs to process all matching objects.

Usage Examples:
    # Process objects with a specific prefix
    python submit_tropo_jobs.py --bucket my-bucket --prefix some/prefix

    # Process objects for a single date
    python submit_tropo_jobs.py --bucket my-bucket --date 2024-03-20

    # Process objects for a date range (intraday)
    python submit_tropo_jobs.py --bucket my-bucket --datetime 2024-03-20T18:00:00

    # Process objects for a date range (inclusive)
    python submit_tropo_jobs.py --bucket my-bucket --start-datetime 2024-03-20T00:00:00 --end-datetime 2024-03-25T23:59:59

Note: You must provide either --prefix, --date, or --start-datetime with --end-datetime.
The script will exit with an error if no filtering option is specified.
"""

import argparse
import logging
import re
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import PurePath, Path
from typing import List, Optional, Set

import boto3
import dateutil

from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_s3_objects(bucket_name: str, prefix: Optional[str] = None) -> List[str]:
    """
    List objects in an S3 bucket that match the given prefix.
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Optional prefix to filter objects
        
    Returns:
        List of S3 object keys that match the criteria
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    
    objects = []
    for obj in bucket.objects.filter(Prefix=prefix):
        objects.append(obj.key)
    
    return objects

def submit_mozart_job_wrapper(
    s3_key: str,
    bucket_name: str,
    job_type: str = "job-SCIFLO_L4_TROPO",
    release: str = "3.2.0-rc.1.0"
) -> str:
    """
    Submit a job to Mozart API using hysds_commons.job_utils.submit_mozart_job.
    
    Args:
        s3_key: S3 key to process
        bucket_name: Name of the S3 bucket
        job_type: Type of job to submit
        release: Release version of the job
        
    Returns:
        str: The job ID returned by Mozart
    """ 
    # Create the full S3 path
    s3_path = f"s3://{bucket_name}/{s3_key}"
    prod_timestamp = re.search(r'\d{12}', PurePath(s3_key).name).group()
    
    # Create the product structure with metadata
    product = {
        "_id": s3_key,
        "_source": {
            "dataset": f"L4_TROPO-{s3_key}",
            "metadata": {
                "batch_id": s3_key,
                "product_paths": {"L4_TROPO": [s3_path]},  # The S3 paths to localize
                "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "FileName": PurePath(s3_key).name,
                "FileLocation": s3_path,
                "id": s3_key,
                "Files": [
                    {
                        "FileName": PurePath(s3_key).name,
                        "FileSize": 1, 
                        "FileLocation": s3_path,
                        "id": PurePath(s3_key).name,
                        "product_paths": "$.product_paths"
                    }
                ]
            }
        }
    }
    
    # Create the job parameters
    params = [
        {
            "name": "dataset_type",
            "from": "value",
            "type": "text",
            "value": "L4_TROPO"
        },
        {
            "name": "input_dataset_id",
            "from": "value",
            "type": "text",
            "value": s3_key
        },
        {
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": product["_source"]
        }
    ]
     
    try:
        job_id = try_submit_mozart_job(
            product=product,
            job_queue=f'opera-job_worker-{"sciflo-l4_tropo"}',
            rule_name=f"trigger-{job_type}",
            params=params,
            job_spec=f"{job_type}:{release}",
            job_name=f"job-WF-SCIFLO_L4_TROPO-for-{prod_timestamp}",
        )
        logger.info(f"Successfully submitted job for {s3_key}")
        return job_id
    except Exception as e:
        logger.error(f"Failed to submit job for {s3_key}: {str(e)}")
        raise

def get_prefix_from_date(date_dt: date) -> str:
    """
    Convert a date (YYYY-MM-DD) to the required prefix format (YYYYMMDD/ECMWF).
    
    Args:
        date_dt: datetime date.
        
    Returns:
        str: Prefix string in YYYYMMDD/ECMWF format
    """
    try:
        return f"{date_dt.strftime('%Y%m%d')}/ECMWF"
    except ValueError as e:
        raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format: {str(e)}")

def get_prefixes_from_date_range(start_dt: datetime, end_dt: datetime) -> Set[str]:
    """
    Generate a set of prefixes for all 6-hour chunks in the given range (inclusive).
    Each day is split into 4 chunks: 00:00, 06:00, 12:00, and 18:00.
    
    Args:
        start_dt: Start datetime string in YYYYmmddTHH:MM:SS format
        end_dt: End datetime string in YYYYmmddTHH:MM:SS format
        
    Returns:
        Set[str]: Set of prefix strings in YYYYmmddTHH0000 format
    """
    try:
        if end_dt < start_dt:
            raise ValueError("End datetime must be after or equal to start datetime")
            
        prefixes = set()
        current = start_dt

        # Find the first 6-hour chunk that current intersects with
        if current.hour < 6:
            current = current.replace(hour=0)
        elif current.hour < 12:
            current = current.replace(hour=6) 
        elif current.hour < 18:
            current = current.replace(hour=12)
        else:
            current = current.replace(hour=18)
        current = current.replace(minute=0, second=0, microsecond=0)

        # Generate all 6-hour chunks between start and end dates
        # make sure the whole range ends before end time
        while current + timedelta(hours=6) <= end_dt:
            prefixes.add(f'{current.strftime("%Y%m%d")}/ECMWF_TROP_{current.strftime("%Y%m%d%H00")}')
            current += timedelta(hours=6)
            
        return prefixes
    except ValueError as e:
        raise ValueError(f"Invalid datetime range: {str(e)}")

def get_prefix_from_datetime(dt: datetime) -> str:
    """
    Generate a set of prefixes for the 6-hour chunk in the given datetime.
    Each day is split into 4 chunks: 00:00, 06:00, 12:00, and 18:00.

    Args:
        dt: Start datetime string in YYYYmmddTHH:MM:SS format

    Returns:
        str: Prefix string in YYYYmmdd/ECMWF format
    """
    current = dt
    if current.hour not in (0,6,12,18):
        raise ValueError(f"Invalid hour of day {current.hour:02d}. Must be one of 00/06/12/18")

    current = current.replace(minute=0, second=0, microsecond=0)
    prefix = f'{current.strftime("%Y%m%d")}/ECMWF_TROP_{current.strftime("%Y%m%d%H00")}'
    return prefix

def get_prefix_from_age(age: int) -> Set[str]:
    """
    Generate a set of prefixes for a single day that is age days ago from the current date.
    
    Args:
        maxage: Number of days to look back from current date
        
    Returns:
        str: prefix string in YYYYmmdd/ECMWF format
    """
    # Calculate the date maxage days ago
    target_date = datetime.now(timezone.utc) - timedelta(days=age)
    # Format as YYYYMMDD/ECMWF
    return f"{target_date.strftime('%Y%m%d')}/ECMWF"

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", required=True, help="Source S3 bucket name")
    
    # Create mutually exclusive group for different filtering options
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument("--prefix", help="Prefix to filter S3 objects")
    filter_group.add_argument("--date", help="Date in YYYY-MM-DD format to filter S3 objects", type=date.fromisoformat)
    filter_group.add_argument("--datetime", help="Datetime in YYYY-MM-DDThh:mm:ss format to filter S3 objects", type=dateutil.parser.parse)
    filter_group.add_argument("--start-datetime", help="Start datetime in YYYY-MM-DDThh:mm:ss format for range filtering", type=dateutil.parser.parse)
    filter_group.add_argument("--forward-mode-age", help="Forward processing mode, integer days of previous day to process", type=int)
    
    # End datetime is not in the mutually exclusive group since it's used with start-datetime
    parser.add_argument("--end-datetime", help="End datetime in YYYY-MM-DDThh:mm:ss format for range filtering", type=dateutil.parser.parse)
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Determine the prefix(es) to use
    prefixes = set()
    if args.date:
        prefixes.add(get_prefix_from_date(args.date))
    elif args.datetime:
        prefixes.add(get_prefix_from_datetime(args.datetime))
    elif args.start_datetime:
        if not args.end_datetime:
            raise ValueError("--end-datetime is required when using --start-datetime")
        prefixes = get_prefixes_from_date_range(args.start_datetime, args.end_datetime)
    elif args.prefix:
        prefixes.add(args.prefix)
    elif args.forward_mode_age:
        prefixes.add(get_prefix_from_age(args.forward_mode_age))
    else:
        logger.error("No prefix specified. Please provide either --prefix, --date, or --start-datetime with --end-datetime")
        sys.exit(1)
    logger.info(f"Using prefixes: {', '.join(sorted(prefixes))}")
 
    # Get S3 objects that meet the criteria
    all_objects = []
    for prefix in prefixes:
        logger.info(f"Listing objects in bucket {args.bucket} with prefix {prefix}")
        objects = get_s3_objects(args.bucket, prefix)
        all_objects.extend(objects)
        logger.info(f"Found {len(objects)} objects with prefix {prefix}")

    settings = SettingsConf(file=str(Path("/export/home/hysdsops/verdi/etc/settings.yaml"))).cfg
    release_version = settings["RELEASE_VERSION"]

    # Submit jobs for each object
    for s3_key in all_objects:
        submit_mozart_job_wrapper(
            s3_key=s3_key,
            bucket_name=args.bucket,
            job_type="job-SCIFLO_L4_TROPO",
            release=release_version
        )

if __name__ == "__main__":
    main() 
