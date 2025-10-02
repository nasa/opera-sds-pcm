#!/usr/bin/env python3
"""
CCSLC Deletion Utility

A utility for selective deletion of CCSLC (Compact Copied SLC) data to enable 
DISP-S1 reprocessing of specific frames. This tool allows deletion based on:
- Frame IDs
- Date ranges
- Burst IDs
- Granule IDs

Features:
- Dry-run mode to preview deletions
- Comprehensive logging
- Input validation
- Safe deletion with confirmation prompts
- Integration with existing OPERA data management systems
"""

import argparse
import boto3
import logging
import re
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict

# Add the parent directory to the Python path to enable imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import OPERA-specific modules
from data_subscriber import cslc_utils
from data_subscriber.cslc_utils import parse_cslc_native_id, localize_disp_frame_burst_hist
from util.conf_util import SettingsConf
from opera_commons.logger import get_logger

# Configure logging
logger = get_logger(__name__)

class CCSLCDeletionUtility:
    """
    Main class for CCSLC deletion operations.
    """
    
    def __init__(self, dry_run: bool = False, verbose: bool = False):
        """
        Initialize the CCSLC deletion utility.
        
        Args:
            dry_run: If True, only preview deletions without executing them
            verbose: If True, enable verbose logging
        """
        self.dry_run = dry_run
        self.verbose = verbose
        self.settings = SettingsConf().cfg
        
        # Configure logging level
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize AWS clients
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        
        # Load DISP-S1 burst database
        self.disp_burst_map, self.burst_to_frames, self.frame_to_bursts = localize_disp_frame_burst_hist()
        
        # CCSLC filename pattern
        self.ccslc_pattern = re.compile(
            r'(?P<id>(?P<project>OPERA)_(?P<level>L2)_(?P<product_type>COMPRESSED-CSLC)-(?P<source>S1)_'
            r'(?P<disp_frame_id>F\d{5})_(?P<burst_id>\w{4}-\w{6}-\w{3})_'
            r'(?P<ref_date_time>\d{8})T000000Z_(?P<first_date_time>\d{8})T000000Z_'
            r'(?P<last_date_time>\d{8})T000000Z_(?P<creation_ts>(?P<cre_year>\d{4})'
            r'(?P<cre_month>\d{2})(?P<cre_day>\d{2})T(?P<cre_hour>\d{2})'
            r'(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_'
            r'(?P<product_version>v\d+[.]\d+))$'
        )
        
        # Get bucket configuration
        self.lts_bucket = self.settings.get('LTS_BUCKET')
        if not self.lts_bucket:
            raise ValueError("LTS_BUCKET not configured in settings")
        
        logger.info(f"Initialized CCSLC deletion utility (dry_run={dry_run})")
        logger.info(f"Using LTS bucket: {self.lts_bucket}")
    
    def validate_frame_id(self, frame_id: int) -> bool:
        """
        Validate that a frame ID exists in the DISP-S1 burst database.
        
        Args:
            frame_id: Frame ID to validate
            
        Returns:
            True if frame ID is valid, False otherwise
        """
        return frame_id in self.disp_burst_map
    
    def validate_burst_id(self, burst_id: str) -> bool:
        """
        Validate that a burst ID exists in the DISP-S1 burst database.
        
        Args:
            burst_id: Burst ID to validate (e.g., 'T175-374393-IW1')
            
        Returns:
            True if burst ID is valid, False otherwise
        """
        return burst_id in self.burst_to_frames
    
    def validate_granule_id(self, granule_id: str) -> bool:
        """
        Validate that a granule ID follows the CCSLC naming convention.
        
        Args:
            granule_id: Granule ID to validate
            
        Returns:
            True if granule ID is valid, False otherwise
        """
        return bool(self.ccslc_pattern.match(granule_id))
    
    def parse_granule_id(self, granule_id: str) -> Optional[Dict[str, str]]:
        """
        Parse a CCSLC granule ID to extract metadata.
        
        Args:
            granule_id: Granule ID to parse
            
        Returns:
            Dictionary with parsed metadata or None if invalid
        """
        match = self.ccslc_pattern.match(granule_id)
        if match:
            return match.groupdict()
        return None
    
    def get_ccslc_objects_by_frame(self, frame_id: int) -> List[Dict[str, str]]:
        """
        Get all CCSLC objects for a specific frame ID.
        
        Args:
            frame_id: Frame ID to search for
            
        Returns:
            List of dictionaries containing S3 object information
        """
        prefix = f"products/CSLC_S1_COMPRESSED/"
        objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.lts_bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        filename = Path(key).name
                        
                        # Parse the filename to extract frame ID
                        parsed = self.parse_granule_id(filename)
                        if parsed and int(parsed['disp_frame_id'][1:]) == frame_id:
                            objects.append({
                                'key': key,
                                'filename': filename,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                'metadata': parsed
                            })
            
            logger.info(f"Found {len(objects)} CCSLC objects for frame {frame_id}")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing CCSLC objects for frame {frame_id}: {e}")
            return []
    
    def get_ccslc_objects_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, str]]:
        """
        Get all CCSLC objects within a date range.
        
        Args:
            start_date: Start date for the range
            end_date: End date for the range
            
        Returns:
            List of dictionaries containing S3 object information
        """
        prefix = f"products/CSLC_S1_COMPRESSED/"
        objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.lts_bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        filename = Path(key).name
                        
                        # Parse the filename to extract creation timestamp
                        parsed = self.parse_granule_id(filename)
                        if parsed:
                            creation_ts = datetime.strptime(parsed['creation_ts'], '%Y%m%dT%H%M%SZ')
                            
                            if start_date <= creation_ts <= end_date:
                                objects.append({
                                    'key': key,
                                    'filename': filename,
                                    'size': obj['Size'],
                                    'last_modified': obj['LastModified'],
                                    'metadata': parsed
                                })
            
            logger.info(f"Found {len(objects)} CCSLC objects in date range {start_date} to {end_date}")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing CCSLC objects for date range: {e}")
            return []
    
    def get_ccslc_objects_by_burst_id(self, burst_id: str) -> List[Dict[str, str]]:
        """
        Get all CCSLC objects for a specific burst ID.
        
        Args:
            burst_id: Burst ID to search for
            
        Returns:
            List of dictionaries containing S3 object information
        """
        prefix = f"products/CSLC_S1_COMPRESSED/"
        objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.lts_bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        filename = Path(key).name
                        
                        # Parse the filename to extract burst ID
                        parsed = self.parse_granule_id(filename)
                        if parsed and parsed['burst_id'] == burst_id:
                            objects.append({
                                'key': key,
                                'filename': filename,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                'metadata': parsed
                            })
            
            logger.info(f"Found {len(objects)} CCSLC objects for burst {burst_id}")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing CCSLC objects for burst {burst_id}: {e}")
            return []
    
    def get_ccslc_objects_by_granule_ids(self, granule_ids: List[str]) -> List[Dict[str, str]]:
        """
        Get CCSLC objects for specific granule IDs.
        
        Args:
            granule_ids: List of granule IDs to search for
            
        Returns:
            List of dictionaries containing S3 object information
        """
        objects = []
        
        for granule_id in granule_ids:
            if not self.validate_granule_id(granule_id):
                logger.warning(f"Invalid granule ID format: {granule_id}")
                continue
            
            # Construct S3 key from granule ID
            key = f"products/CSLC_S1_COMPRESSED/{granule_id}/{granule_id}.h5"
            
            try:
                # Check if object exists
                response = self.s3_client.head_object(Bucket=self.lts_bucket, Key=key)
                
                objects.append({
                    'key': key,
                    'filename': f"{granule_id}.h5",
                    'size': response['ContentLength'],
                    'last_modified': response['LastModified'],
                    'metadata': self.parse_granule_id(granule_id)
                })
                
            except self.s3_client.exceptions.NoSuchKey:
                logger.warning(f"CCSLC object not found: {granule_id}")
            except Exception as e:
                logger.error(f"Error checking CCSLC object {granule_id}: {e}")
        
        logger.info(f"Found {len(objects)} CCSLC objects for {len(granule_ids)} granule IDs")
        return objects
    
    def delete_objects(self, objects: List[Dict[str, str]]) -> Tuple[int, int]:
        """
        Delete CCSLC objects from S3.
        
        Args:
            objects: List of object dictionaries to delete
            
        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        if not objects:
            logger.info("No objects to delete")
            return 0, 0
        
        if self.dry_run:
            logger.info(f"DRY RUN: Would delete {len(objects)} CCSLC objects")
            for obj in objects:
                logger.info(f"DRY RUN: Would delete s3://{self.lts_bucket}/{obj['key']}")
            return len(objects), 0
        
        # Confirm deletion
        total_size = sum(obj['size'] for obj in objects)
        size_mb = total_size / (1024 * 1024)
        
        print(f"\nAbout to delete {len(objects)} CCSLC objects ({size_mb:.2f} MB)")
        print("Objects to be deleted:")
        for obj in objects[:10]:  # Show first 10 objects
            print(f"  - {obj['filename']}")
        if len(objects) > 10:
            print(f"  ... and {len(objects) - 10} more objects")
        
        response = input("\nAre you sure you want to delete these objects? Type 'yes' to continue: ")
        if response.lower() != 'yes':
            logger.info("Deletion cancelled by user")
            return 0, 0
        
        # Perform deletion
        successful = 0
        failed = 0
        
        # Delete objects in batches of 1000 (S3 limit)
        batch_size = 1000
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]
            
            delete_objects = [{'Key': obj['key']} for obj in batch]
            
            try:
                response = self.s3_client.delete_objects(
                    Bucket=self.lts_bucket,
                    Delete={'Objects': delete_objects}
                )
                
                # Count successful deletions
                if 'Deleted' in response:
                    successful += len(response['Deleted'])
                    for deleted in response['Deleted']:
                        logger.info(f"Deleted: s3://{self.lts_bucket}/{deleted['Key']}")
                
                # Count failed deletions
                if 'Errors' in response:
                    failed += len(response['Errors'])
                    for error in response['Errors']:
                        logger.error(f"Failed to delete s3://{self.lts_bucket}/{error['Key']}: {error['Message']}")
                
            except Exception as e:
                logger.error(f"Error deleting batch: {e}")
                failed += len(batch)
        
        logger.info(f"Deletion complete: {successful} successful, {failed} failed")
        return successful, failed
    
    def delete_by_frames(self, frame_ids: List[int]) -> Tuple[int, int]:
        """
        Delete CCSLC objects for specific frame IDs.
        
        Args:
            frame_ids: List of frame IDs to delete
            
        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        logger.info(f"Deleting CCSLC objects for frames: {frame_ids}")
        
        # Validate frame IDs
        invalid_frames = [fid for fid in frame_ids if not self.validate_frame_id(fid)]
        if invalid_frames:
            logger.error(f"Invalid frame IDs: {invalid_frames}")
            return 0, 0
        
        # Collect all objects to delete
        all_objects = []
        for frame_id in frame_ids:
            objects = self.get_ccslc_objects_by_frame(frame_id)
            all_objects.extend(objects)
        
        return self.delete_objects(all_objects)
    
    def delete_by_date_range(self, start_date: datetime, end_date: datetime) -> Tuple[int, int]:
        """
        Delete CCSLC objects within a date range.
        
        Args:
            start_date: Start date for the range
            end_date: End date for the range
            
        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        logger.info(f"Deleting CCSLC objects in date range: {start_date} to {end_date}")
        
        objects = self.get_ccslc_objects_by_date_range(start_date, end_date)
        return self.delete_objects(objects)
    
    def delete_by_burst_ids(self, burst_ids: List[str]) -> Tuple[int, int]:
        """
        Delete CCSLC objects for specific burst IDs.
        
        Args:
            burst_ids: List of burst IDs to delete
            
        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        logger.info(f"Deleting CCSLC objects for bursts: {burst_ids}")
        
        # Validate burst IDs
        invalid_bursts = [bid for bid in burst_ids if not self.validate_burst_id(bid)]
        if invalid_bursts:
            logger.error(f"Invalid burst IDs: {invalid_bursts}")
            return 0, 0
        
        # Collect all objects to delete
        all_objects = []
        for burst_id in burst_ids:
            objects = self.get_ccslc_objects_by_burst_id(burst_id)
            all_objects.extend(objects)
        
        return self.delete_objects(all_objects)
    
    def delete_by_granule_ids(self, granule_ids: List[str]) -> Tuple[int, int]:
        """
        Delete CCSLC objects for specific granule IDs.
        
        Args:
            granule_ids: List of granule IDs to delete
            
        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        logger.info(f"Deleting CCSLC objects for granule IDs: {granule_ids}")
        
        objects = self.get_ccslc_objects_by_granule_ids(granule_ids)
        return self.delete_objects(objects)


def main():
    """Main entry point for the CCSLC deletion utility."""
    
    parser = argparse.ArgumentParser(
        description="CCSLC Deletion Utility - Selective deletion of CCSLC data for DISP-S1 reprocessing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete CCSLC data for specific frames
  python ccslc_deletion_utility.py frames --frame-ids 10859,10860 --dry-run
  
  # Delete CCSLC data within a date range
  python ccslc_deletion_utility.py date-range --start-date 2023-01-01 --end-date 2023-01-31
  
  # Delete CCSLC data for specific burst IDs
  python ccslc_deletion_utility.py bursts --burst-ids T175-374393-IW1,T175-374394-IW1
  
  # Delete CCSLC data for specific granule IDs
  python ccslc_deletion_utility.py granules --granule-ids OPERA_L2_COMPRESSED-CSLC-S1_F10859_T175-374393-IW1_20230101T000000Z_20230101T000000Z_20230131T000000Z_20230201T120000Z_VV_v1.0
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Deletion command")
    
    # Frames subcommand
    frames_parser = subparsers.add_parser("frames", help="Delete CCSLC data by frame IDs")
    frames_parser.add_argument("--frame-ids", required=True,
                              help="Comma-separated list of frame IDs (e.g., '10859,10860')")
    frames_parser.add_argument("--dry-run", action="store_true", 
                              help="Preview deletions without executing them")
    frames_parser.add_argument("--verbose", "-v", action="store_true",
                              help="Enable verbose logging")
    
    # Date range subcommand
    date_parser = subparsers.add_parser("date-range", help="Delete CCSLC data by date range")
    date_parser.add_argument("--start-date", required=True,
                            help="Start date (YYYY-MM-DD format)")
    date_parser.add_argument("--end-date", required=True,
                            help="End date (YYYY-MM-DD format)")
    date_parser.add_argument("--dry-run", action="store_true", 
                            help="Preview deletions without executing them")
    date_parser.add_argument("--verbose", "-v", action="store_true",
                            help="Enable verbose logging")
    
    # Burst IDs subcommand
    bursts_parser = subparsers.add_parser("bursts", help="Delete CCSLC data by burst IDs")
    bursts_parser.add_argument("--burst-ids", required=True,
                               help="Comma-separated list of burst IDs (e.g., 'T175-374393-IW1,T175-374394-IW1')")
    bursts_parser.add_argument("--dry-run", action="store_true", 
                               help="Preview deletions without executing them")
    bursts_parser.add_argument("--verbose", "-v", action="store_true",
                               help="Enable verbose logging")
    
    # Granule IDs subcommand
    granules_parser = subparsers.add_parser("granules", help="Delete CCSLC data by granule IDs")
    granules_parser.add_argument("--granule-ids", required=True,
                                 help="Comma-separated list of granule IDs")
    granules_parser.add_argument("--dry-run", action="store_true", 
                                 help="Preview deletions without executing them")
    granules_parser.add_argument("--verbose", "-v", action="store_true",
                                 help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        # Initialize utility
        utility = CCSLCDeletionUtility(dry_run=args.dry_run, verbose=args.verbose)
        
        # Execute command
        if args.command == "frames":
            frame_ids = [int(fid.strip()) for fid in args.frame_ids.split(",")]
            successful, failed = utility.delete_by_frames(frame_ids)
            
        elif args.command == "date-range":
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            successful, failed = utility.delete_by_date_range(start_date, end_date)
            
        elif args.command == "bursts":
            burst_ids = [bid.strip() for bid in args.burst_ids.split(",")]
            successful, failed = utility.delete_by_burst_ids(burst_ids)
            
        elif args.command == "granules":
            granule_ids = [gid.strip() for gid in args.granule_ids.split(",")]
            successful, failed = utility.delete_by_granule_ids(granule_ids)
        
        # Print summary
        print(f"\nDeletion Summary:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        
        if args.dry_run:
            print(f"\nNote: This was a dry run - no objects were actually deleted")
        
        sys.exit(0 if failed == 0 else 1)
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
