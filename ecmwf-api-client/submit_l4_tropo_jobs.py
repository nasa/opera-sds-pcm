#!/usr/bin/env python3

import argparse
import logging
import sys
from typing import List, Optional
import boto3
from mypy_boto3_s3 import S3ServiceResource
import uuid
from util.job_submitter import try_submit_mozart_job
from datetime import datetime, timezone
from pathlib import PurePath
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_new_data(s3_key: str) -> bool:
    """
    Stub function to determine if an S3 object meets the criteria for L4_TROPO processing.
    
    Args:
        s3_key: The S3 object key to check
        
    Returns:
        bool: True if the object meets the criteria, False otherwise
    """
    # This is just a placeholder - customize based on actual requirements
    return True

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
        if is_new_data(obj.key):
            objects.append(obj.key)
    
    return objects

def submit_mozart_job_wrapper(
    s3_key: str,
    bucket_name: str,
    job_type: str = "job-sciflo-l4_tropo",
    job_queue: str = "opera-job_worker-sciflo-l4_tropo",
    release: str = "3.0.0-er.1.0-tropo"
) -> str:
    """
    Submit a job to Mozart API using hysds_commons.job_utils.submit_mozart_job.
    
    Args:
        s3_key: S3 key to process
        bucket_name: Name of the S3 bucket
        job_type: Type of job to submit
        job_queue: Queue to submit the job to
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
                "id": s3_key,
                "Files": [
                    {
                        "FileName": PurePath(s3_key).name,
                        "FileSize": 1,  # TODO: Get actual file size if needed
                        "FileLocation": os.path.dirname(s3_path),
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

def parse_args():
    parser = argparse.ArgumentParser(description="Submit L4_TROPO jobs for S3 objects")
    parser.add_argument("--bucket", required=True, help="Source S3 bucket name")
    parser.add_argument("--prefix", help="Prefix to filter S3 objects") 
    parser.add_argument("--job-type", default="job-sciflo-l4_tropo", help="Type of job to submit")
    parser.add_argument("--job-queue", default="opera-job_worker-sciflo-l4_tropo", help="Queue to submit jobs to")
    parser.add_argument("--release", default="3.0.0-er.1.0-tropo", help="Release version of the job")
    
    return parser.parse_args()

def main():
    args = parse_args()
 
    # Get S3 objects that meet the criteria
    logger.info(f"Listing objects in bucket {args.bucket} with prefix {args.prefix}")
    s3_objects = get_s3_objects(args.bucket, args.prefix)
    logger.info(f"Found {len(s3_objects)} objects meeting criteria")
    
    # Submit jobs for each object
    for s3_key in s3_objects:
        submit_mozart_job_wrapper(
            s3_key=s3_key,
            bucket_name=args.bucket,
            job_type=args.job_type,
            job_queue=args.job_queue,
            release=args.release
        )

if __name__ == "__main__":
    main() 