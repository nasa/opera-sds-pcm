import logging
from datetime import datetime, timezone,timedelta
import boto3


def get_s3_objects(bucket_name: str, prefix = None):
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

def get_prefixes_from_date_range(start_datetime: str, end_datetime: str):
    """
    Generate a set of prefixes for all 6-hour chunks in the given range (inclusive).
    Each day is split into 4 chunks: 00:00, 06:00, 12:00, and 18:00.
    
    Args:
        start_datetime: Start datetime string in YYYYmmddTHH:MM:SS format
        end_datetime: End datetime string in YYYYmmddTHH:MM:SS format
        
    Returns:
        Set[str]: Set of prefix strings in YYYYmmddTHH0000 format
    """
    try:
        start = datetime.fromisoformat(start_datetime)
        end = datetime.fromisoformat(end_datetime)
        
        if end < start:
            raise ValueError("End datetime must be after or equal to start datetime")
            
        prefixes = set()
        current = start

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
        while current + timedelta(hours=6) <= end:
            prefixes.add(f'{current.strftime("%Y%m%d")}/ECMWF_TROP_{current.strftime("%Y%m%d%H00")}')
            current += timedelta(hours=6)
            
        return prefixes
    except ValueError as e:
        raise ValueError(f"Invalid datetime range: {str(e)}")


def get_prefix_from_age(age: int):
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



def get_tropo_objects(bucket, date=None,start_datetime=None, end_datetime=None, prefix=None, forward_mode_age=None): 

    # Determine the prefix(es) to use
    prefixes = set()
    if date:
        prefix = get_prefix_from_date(date)
        prefixes.add(prefix)
        logging.info(f"Using date-based prefix: {prefix}")
    elif start_datetime:
        if not end_datetime:
            raise ValueError("--end-datetime is required when using --start-datetime")
        prefixes = get_prefixes_from_date_range(start_datetime, end_datetime)
        logging.info(f"Using datetime range prefixes: {', '.join(sorted(prefixes))}")
    elif prefix:
        prefixes.add(prefix)
        logging.info(f"Using provided prefix: {prefix}")
    elif forward_mode_age:
        prefixes.add(get_prefix_from_age(forward_mode_age))
        logging.info(f"Using forward mode prefixes for age days in the past: {prefixes}")
    else:
        logging.error("No prefix specified. Please provide either --prefix, --date, or --start-datetime with --end-datetime")
        raise Exception("No prefix specified. Please provide either --prefix, --date, or --start-datetime with --end-datetime")
 
    # Get S3 objects that meet the criteria
    all_objects = []
    for prefix in prefixes:
        logging.info(f"Listing objects in bucket {bucket} with prefix {prefix}")
        objects = get_s3_objects(bucket, prefix)
        all_objects.extend(objects)
        logging.info(f"Found {len(objects)} objects with prefix {prefix}")

    return all_objects