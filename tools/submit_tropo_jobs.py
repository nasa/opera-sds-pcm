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

    # Process objects for a date range (inclusive)
    python submit_tropo_jobs.py --bucket my-bucket --start-date 2024-03-20 --end-date 2024-03-25

Required Arguments:
    --bucket BUCKET    Source S3 bucket name

Optional Arguments:
    --prefix PREFIX    Prefix to filter S3 objects
    --date DATE        Date in YYYY-MM-DD format to filter S3 objects
    --start-date DATE  Start date in YYYY-MM-DD format for range filtering
    --end-date DATE    End date in YYYY-MM-DD format for range filtering
    --job-type TYPE    Type of job to submit (default: job-sciflo-l4_tropo)
    --release RELEASE  Release version of the job (default: 3.0.0-er.1.0-tropo)

Note: You must provide either --prefix, --date, or --start-date with --end-date.
The script will exit with an error if no filtering option is specified.
"""

import argparse
import logging
import sys
from typing import List, Optional, Set
from datetime import datetime, timezone, timedelta
from pathlib import PurePath

import boto3
from mypy_boto3_s3 import S3ServiceResource

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
    s3: S3ServiceResource = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    
    objects = []
    for obj in bucket.objects.filter(Prefix=prefix):
        objects.append(obj.key)
    
    return objects

def submit_mozart_job_wrapper(
    s3_key: str,
    bucket_name: str,
    job_type: str = "job-sciflo-l4_tropo",
    release: str = "3.0.0-er.1.0-tropo"
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
                        "FileSize": 1,  # TODO: Get actual file size if needed
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
            job_name=f"job-WF-{job_type}",
        )
        logger.info(f"Successfully submitted job for {s3_key}")
        return job_id
    except Exception as e:
        logger.error(f"Failed to submit job for {s3_key}: {str(e)}")
        raise

def get_prefix_from_date(date_str: str) -> str:
    """
    Convert a date string (YYYY-MM-DD) to the required prefix format (YYYYMMDD/ECMWF).
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        str: Prefix string in YYYYMMDD/ECMWF format
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{date_obj.strftime('%Y%m%d')}/ECMWF"
    except ValueError as e:
        raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format: {str(e)}")

def get_prefixes_from_date_range(start_date: str, end_date: str) -> Set[str]:
    """
    Generate a set of prefixes for all dates in the given range (inclusive).
    
    Args:
        start_date: Start date string in YYYY-MM-DD format
        end_date: End date string in YYYY-MM-DD format
        
    Returns:
        Set[str]: Set of prefix strings in YYYYMMDD/ECMWF format
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if end < start:
            raise ValueError("End date must be after or equal to start date")
            
        prefixes = set()
        current = start
        while current <= end:
            prefixes.add(get_prefix_from_date(current.strftime("%Y-%m-%d")))
            current += timedelta(days=1)
            
        return prefixes
    except ValueError as e:
        raise ValueError(f"Invalid date range: {str(e)}")

def parse_args():
    parser = argparse.ArgumentParser(description="Submit L4_TROPO jobs for S3 objects")
    parser.add_argument("--bucket", required=True, help="Source S3 bucket name")
    
    # Create mutually exclusive group for different filtering options
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument("--prefix", help="Prefix to filter S3 objects")
    filter_group.add_argument("--date", help="Date in YYYY-MM-DD format to filter S3 objects")
    filter_group.add_argument("--start-date", help="Start date in YYYY-MM-DD format for range filtering")
    
    # End date is not in the mutually exclusive group since it's used with start-date
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format for range filtering")
    
    parser.add_argument("--job-type", default="job-sciflo-l4_tropo", help="Type of job to submit")
    parser.add_argument("--release", default="3.0.0-er.1.0-tropo", help="Release version of the job")
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Determine the prefix(es) to use
    prefixes = set()
    if args.date:
        prefix = get_prefix_from_date(args.date)
        prefixes.add(prefix)
        logger.info(f"Using date-based prefix: {prefix}")
    elif args.start_date:
        if not args.end_date:
            raise ValueError("--end-date is required when using --start-date")
        prefixes = get_prefixes_from_date_range(args.start_date, args.end_date)
        logger.info(f"Using date range prefixes: {', '.join(sorted(prefixes))}")
    elif args.prefix:
        prefixes.add(args.prefix)
        logger.info(f"Using provided prefix: {args.prefix}")
    else:
        logger.error("No prefix specified. Please provide either --prefix, --date, or --start-date with --end-date")
        sys.exit(1)
 
    # Get S3 objects that meet the criteria
    all_objects = []
    for prefix in prefixes:
        logger.info(f"Listing objects in bucket {args.bucket} with prefix {prefix}")
        objects = get_s3_objects(args.bucket, prefix)
        all_objects.extend(objects)
        logger.info(f"Found {len(objects)} objects with prefix {prefix}")
    
    # Submit jobs for each object
    for s3_key in all_objects:
        submit_mozart_job_wrapper(
            s3_key=s3_key,
            bucket_name=args.bucket,
            job_type=args.job_type,
            release=args.release
        )

if __name__ == "__main__":
    main() 