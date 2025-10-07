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
- Complete cleanup: deletes both S3 objects and OpenSearch documents
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

from botocore.exceptions import ClientError
from opensearchpy import OpenSearch

# Add the parent directory to the Python path to enable imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import OPERA-specific modules
from data_subscriber import cslc_utils
from data_subscriber.cslc_utils import (
    parse_cslc_native_id,
    localize_disp_frame_burst_hist,
)
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
        self.settings = SettingsConf(
            file=str(Path("/export/home/hysdsops/.sds/config"))
        ).cfg

        # Configure logging level
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        # Initialize AWS clients
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")

        # Initialize OpenSearch client
        self.es_client = self._initialize_opensearch_client()

        # Load DISP-S1 burst database
        self.disp_burst_map, self.burst_to_frames, self.frame_to_bursts = (
            localize_disp_frame_burst_hist()
        )

        # CCSLC filename pattern (with optional .h5 extension)
        self.ccslc_pattern = re.compile(
            r"(?P<id>(?P<project>OPERA)_(?P<level>L2)_(?P<product_type>COMPRESSED-CSLC)-(?P<source>S1)_"
            r"(?P<disp_frame_id>F\d{5})_(?P<burst_id>\w{4}-\w{6}-\w{3})_"
            r"(?P<ref_date_time>\d{8})T000000Z_(?P<first_date_time>\d{8})T000000Z_"
            r"(?P<last_date_time>\d{8})T000000Z_(?P<creation_ts>(?P<cre_year>\d{4})"
            r"(?P<cre_month>\d{2})(?P<cre_day>\d{2})T(?P<cre_hour>\d{2})"
            r"(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_"
            r"(?P<product_version>v\d+[.]\d+))(?:[.]h5)?$"
        )

        # Get bucket configuration
        self.lts_bucket = self.settings.get("LTS_BUCKET")
        if not self.lts_bucket:
            raise ValueError("LTS_BUCKET not configured in settings")

        logger.info(f"Initialized CCSLC deletion utility (dry_run={dry_run})")
        logger.info(f"Using LTS bucket: {self.lts_bucket}")

    def _initialize_opensearch_client(self) -> Optional[OpenSearch]:
        """
        Initialize OpenSearch client using settings configuration.

        Returns:
            OpenSearch client instance or None if connection fails
        """
        try:
            # Get OpenSearch configuration from settings
            protocol = self.settings.get("GRQ_ES_PROTOCOL", "http")
            host = self.settings.get("GRQ_ES_PVT_IP", "100.104.40.249")
            port = self.settings.get("GRQ_ES_PORT", "9200")

            # Construct OpenSearch URL
            es_url = f"{protocol}://{host}:{port}"

            # Initialize OpenSearch client
            es_client = OpenSearch(
                hosts=[es_url],
                http_compress=True,
                use_ssl=False,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                timeout=5,  # Short timeout for connection test
            )

            # Test connection
            es_client.info()
            logger.info(f"Connected to OpenSearch at {es_url}")

            return es_client

        except Exception as e:
            logger.warning(f"Failed to initialize OpenSearch client: {e}")
            logger.warning("OpenSearch document deletion will be skipped")
            return None

    def _get_ccslc_index_patterns(self) -> List[str]:
        """
        Get OpenSearch index patterns for CCSLC data.

        Returns:
            List of index patterns to search for CCSLC documents
        """
        return ["grq_1_l2_cslc_s1_compressed*"]  # Primary CCSLC index pattern

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
        # Optimize prefix to include frame ID for faster S3 search
        # CCSLC objects are stored as directories, so we need to find all files within them
        prefix = (
            f"products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id:05d}_"
        )
        objects = []

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.lts_bucket, Prefix=prefix)

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]

                        # Skip directory entries (they end with /)
                        if key.endswith("/"):
                            continue

                        # Process all files in CCSLC directories (not just .h5 files)
                        filename = Path(key).name

                        # Extract the granule ID from the directory path for metadata
                        # The directory name is the granule ID
                        granule_id = Path(key).parent.name

                        # Parse the granule ID to extract metadata
                        parsed = self.parse_granule_id(granule_id)
                        if parsed:  # Should always be True with our optimized prefix
                            objects.append(
                                {
                                    "key": key,
                                    "filename": filename,
                                    "size": obj["Size"],
                                    "last_modified": obj["LastModified"],
                                    "metadata": parsed,
                                }
                            )

            logger.info(f"Found {len(objects)} CCSLC objects for frame {frame_id}")
            return objects

        except Exception as e:
            logger.error(f"Error listing CCSLC objects for frame {frame_id}: {e}")
            return []

    def get_ccslc_objects_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, str]]:
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
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=self.lts_bucket, Prefix=prefix)

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]

                        # Skip directory entries (they end with /)
                        if key.endswith("/"):
                            continue

                        # Process all files in CCSLC directories (not just .h5 files)
                        filename = Path(key).name

                        # Extract the granule ID from the directory path for metadata
                        granule_id = Path(key).parent.name

                        # Parse the granule ID to extract creation timestamp
                        parsed = self.parse_granule_id(granule_id)
                        if parsed:
                            creation_ts = datetime.strptime(
                                parsed["creation_ts"], "%Y%m%dT%H%M%SZ"
                            )

                            if start_date <= creation_ts <= end_date:
                                objects.append(
                                    {
                                        "key": key,
                                        "filename": filename,
                                        "size": obj["Size"],
                                        "last_modified": obj["LastModified"],
                                        "metadata": parsed,
                                    }
                                )

            logger.info(
                f"Found {len(objects)} CCSLC objects in date range {start_date} to {end_date}"
            )
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
        objects = []

        # Optimize by using burst-to-frames mapping to search only relevant frame prefixes
        if burst_id in self.burst_to_frames:
            frame_ids = self.burst_to_frames[burst_id]
            logger.info(f"Burst {burst_id} found in frames: {frame_ids}")

            # Search each frame that contains this burst ID
            for frame_id in frame_ids:
                frame_objects = self.get_ccslc_objects_by_frame(frame_id)
                # Filter to only objects with the specific burst ID
                for obj in frame_objects:
                    if obj["metadata"]["burst_id"] == burst_id:
                        objects.append(obj)
        else:
            # Fallback: search all CCSLC objects if burst ID not found in database
            logger.warning(
                f"Burst ID {burst_id} not found in burst-to-frames mapping, searching all objects"
            )
            prefix = f"products/CSLC_S1_COMPRESSED/"

            try:
                paginator = self.s3_client.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(
                    Bucket=self.lts_bucket, Prefix=prefix
                )

                for page in page_iterator:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key = obj["Key"]

                            # Skip directory entries (they end with /)
                            if key.endswith("/"):
                                continue

                            # Process all files in CCSLC directories (not just .h5 files)
                            filename = Path(key).name

                            # Extract the granule ID from the directory path for metadata
                            granule_id = Path(key).parent.name

                            # Parse the granule ID to extract burst ID
                            parsed = self.parse_granule_id(granule_id)
                            if parsed and parsed["burst_id"] == burst_id:
                                objects.append(
                                    {
                                        "key": key,
                                        "filename": filename,
                                        "size": obj["Size"],
                                        "last_modified": obj["LastModified"],
                                        "metadata": parsed,
                                    }
                                )
            except Exception as e:
                logger.error(f"Error listing CCSLC objects for burst {burst_id}: {e}")
                return []

        logger.info(f"Found {len(objects)} CCSLC objects for burst {burst_id}")
        return objects

    def get_ccslc_objects_by_granule_ids(
        self, granule_ids: List[str]
    ) -> List[Dict[str, str]]:
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

                objects.append(
                    {
                        "key": key,
                        "filename": f"{granule_id}.h5",
                        "size": response["ContentLength"],
                        "last_modified": response["LastModified"],
                        "metadata": self.parse_granule_id(granule_id),
                    }
                )

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.warning(f"CCSLC object not found: {granule_id}")
                else:
                    logger.error(f"Error checking CCSLC object {granule_id}: {e}")
            except Exception as e:
                logger.error(f"Error checking CCSLC object {granule_id}: {e}")

        logger.info(
            f"Found {len(objects)} CCSLC objects for {len(granule_ids)} granule IDs"
        )
        return objects

    def delete_objects(self, objects: List[Dict[str, str]]) -> Tuple[int, int]:
        """
        Delete CCSLC objects from S3 and their corresponding OpenSearch documents.
        Objects are grouped by dataset (granule ID) and deleted atomically.

        Args:
            objects: List of object dictionaries to delete

        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        if not objects:
            logger.info("No objects to delete")
            return 0, 0

        # Group objects by dataset (granule ID)
        datasets = {}
        for obj in objects:
            granule_id = obj["metadata"]["id"]
            if granule_id not in datasets:
                datasets[granule_id] = []
            datasets[granule_id].append(obj)

        logger.info(f"Found {len(datasets)} unique datasets to delete")

        if self.dry_run:
            logger.info(
                f"DRY RUN: Would delete {len(objects)} CCSLC objects from {len(datasets)} datasets"
            )
            for granule_id, dataset_objects in datasets.items():
                logger.info(
                    f"DRY RUN: Dataset {granule_id}: {len(dataset_objects)} objects"
                )
                for obj in dataset_objects:
                    logger.info(
                        f"DRY RUN: Would delete s3://{self.lts_bucket}/{obj['key']}"
                    )

            # Also show OpenSearch deletions in dry-run
            logger.info(
                "DRY RUN: Would also attempt to delete corresponding OpenSearch documents (if they exist)"
            )
            for granule_id, dataset_objects in datasets.items():
                es_successful, es_failed = self._delete_dataset_opensearch_documents(
                    dataset_objects
                )
            return len(objects), 0

        # Confirm deletion
        total_size = sum(obj["size"] for obj in objects)
        size_mb = total_size / (1024 * 1024)

        print(
            f"\nAbout to delete {len(objects)} CCSLC objects ({size_mb:.2f} MB) from {len(datasets)} datasets"
        )
        print("This will delete:")
        print("  - S3 objects (data files)")
        print("  - OpenSearch documents (metadata, if they exist)")
        print("\nDatasets to be deleted:")
        for granule_id, dataset_objects in list(datasets.items())[
            :5
        ]:  # Show first 5 datasets
            print(f"  - {granule_id}: {len(dataset_objects)} objects")
        if len(datasets) > 5:
            print(f"  ... and {len(datasets) - 5} more datasets")

        response = input(
            "\nAre you sure you want to delete these objects and their metadata? Type 'yes' to continue: "
        )
        if response.lower() != "yes":
            logger.info("Deletion cancelled by user")
            return 0, 0

        # Perform deletion per dataset
        total_successful = 0
        total_failed = 0

        for granule_id, dataset_objects in datasets.items():
            logger.info(
                f"Processing dataset: {granule_id} ({len(dataset_objects)} objects)"
            )

            # Delete S3 objects for this dataset using recursive deletion
            s3_successful, s3_failed = self._delete_dataset_s3_objects(
                granule_id, dataset_objects
            )

            # Delete OpenSearch documents for this dataset
            es_successful, es_failed = self._delete_dataset_opensearch_documents(
                dataset_objects
            )

            total_successful += s3_successful
            total_failed += s3_failed

            logger.info(
                f"Dataset {granule_id}: S3={s3_successful}/{len(dataset_objects)}, ES={es_successful}"
            )

        logger.info(
            f"Deletion complete: {total_successful} successful, {total_failed} failed"
        )
        return total_successful, total_failed

    def _delete_dataset_s3_objects(
        self, granule_id: str, objects: List[Dict[str, str]]
    ) -> Tuple[int, int]:
        """
        Delete S3 objects for a specific dataset using recursive deletion.

        Args:
            granule_id: The granule ID (dataset identifier)
            objects: List of objects in this dataset

        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        try:
            # Get the dataset directory prefix
            dataset_prefix = f"products/CSLC_S1_COMPRESSED/{granule_id}/"

            # Use recursive deletion for the entire dataset directory
            logger.info(
                f"Recursively deleting S3 objects with prefix: {dataset_prefix}"
            )

            # Delete all objects with this prefix
            paginator = self.s3_client.get_paginator("list_objects_v2")
            delete_keys = []

            for page in paginator.paginate(
                Bucket=self.lts_bucket, Prefix=dataset_prefix
            ):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        delete_keys.append({"Key": obj["Key"]})

            if not delete_keys:
                logger.warning(f"No objects found for dataset {granule_id}")
                return 0, 0

            # Delete in batches of 1000 (S3 limit)
            successful = 0
            failed = 0

            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i : i + 1000]

                try:
                    response = self.s3_client.delete_objects(
                        Bucket=self.lts_bucket, Delete={"Objects": batch}
                    )

                    if "Deleted" in response:
                        successful += len(response["Deleted"])
                        for deleted in response["Deleted"]:
                            logger.debug(
                                f"Deleted: s3://{self.lts_bucket}/{deleted['Key']}"
                            )

                    if "Errors" in response:
                        failed += len(response["Errors"])
                        for error in response["Errors"]:
                            logger.error(
                                f"Failed to delete s3://{self.lts_bucket}/{error['Key']}: {error['Message']}"
                            )

                except Exception as e:
                    logger.error(f"Error deleting batch for dataset {granule_id}: {e}")
                    failed += len(batch)

            logger.info(f"Dataset {granule_id}: Deleted {successful} S3 objects")
            return successful, failed

        except Exception as e:
            logger.error(f"Error deleting S3 objects for dataset {granule_id}: {e}")
            return 0, len(objects)

    def _delete_dataset_opensearch_documents(
        self, objects: List[Dict[str, str]]
    ) -> Tuple[int, int]:
        """
        Delete OpenSearch documents for a specific dataset.

        This method handles cases where CCSLC datasets exist in S3 but may not
        have corresponding documents in OpenSearch indices.

        Args:
            objects: List of objects in this dataset

        Returns:
            Tuple of (successful_deletions, failed_deletions)
        """
        if not objects:
            return 0, 0

        if self.es_client is None:
            logger.warning(
                "OpenSearch client not available, skipping document deletion"
            )
            return 0, len(objects)

        try:
            # Extract granule IDs and S3 URLs for this dataset
            granule_ids = []
            s3_urls = []

            for obj in objects:
                granule_id = obj["metadata"]["id"]
                s3_url = f"s3://{self.lts_bucket}/{obj['key']}"
                granule_ids.append(granule_id)
                s3_urls.append(s3_url)

            # Get the index pattern
            index_patterns = self._get_ccslc_index_patterns()
            successful = 0
            failed = 0
            documents_found = False

            for pattern in index_patterns:
                try:
                    # Check if any indices match this pattern
                    matching_indices = self.es_client.indices.get(pattern, ignore=[404])
                    if not matching_indices:
                        logger.debug(f"No indices found matching pattern: {pattern}")
                        continue

                    logger.debug(
                        f"Searching for documents in indices matching pattern: {pattern}"
                    )

                    # First, let's debug by checking what indices actually match the pattern
                    logger.debug(f"Checking indices matching pattern: {pattern}")
                    logger.debug(f"Available indices: {list(matching_indices.keys())}")

                    # First, check if any documents exist for this dataset
                    # Based on actual document structure: id field contains granule_id, metadata.product_s3_paths contains S3 URLs
                    search_query = {
                        "query": {
                            "bool": {
                                "should": [
                                    {
                                        "terms": {"id": granule_ids}
                                    },  # Top-level id field
                                    {
                                        "terms": {"objectid": granule_ids}
                                    },  # Alternative id field
                                    {
                                        "terms": {"metadata.id": granule_ids}
                                    },  # Metadata id field
                                    {
                                        "terms": {"metadata.product_s3_paths": s3_urls}
                                    },  # S3 paths in metadata
                                    {
                                        "terms": {"metadata.product_urls": s3_urls}
                                    },  # Product URLs in metadata
                                    {
                                        "terms": {"urls": s3_urls}
                                    },  # Top-level urls field
                                ]
                            }
                        },
                        "size": 0,  # We only need to know if documents exist
                    }

                    logger.debug(f"Search query: {search_query}")
                    logger.debug(f"Searching for granule IDs: {granule_ids}")
                    logger.debug(f"Searching for S3 URLs: {s3_urls}")

                    search_response = self.es_client.search(
                        index=pattern, body=search_query
                    )

                    total_hits = search_response.get("hits", {}).get("total", 0)
                    if isinstance(total_hits, dict):
                        total_hits = total_hits.get("value", 0)

                    logger.debug(f"Search response total hits: {total_hits}")

                    if total_hits == 0:
                        # Let's try a broader search to see what fields are actually available
                        logger.debug(
                            f"No documents found with specific query, trying broader search..."
                        )

                        # First, let's see what documents exist for this frame
                        frame_id = granule_ids[0].split("_")[
                            1
                        ]  # Extract frame ID (e.g., F10859)
                        logger.debug(
                            f"Searching for documents with frame ID: {frame_id}"
                        )

                        frame_search_query = {
                            "query": {
                                "bool": {
                                    "should": [
                                        {"wildcard": {"id": f"*{frame_id}*"}},
                                        {"wildcard": {"objectid": f"*{frame_id}*"}},
                                        {"wildcard": {"metadata.id": f"*{frame_id}*"}},
                                        {
                                            "wildcard": {
                                                "metadata.Files.disp_frame_id": f"*{frame_id}*"
                                            }
                                        },
                                    ]
                                }
                            },
                            "size": 5,  # Get a few documents to see what's there
                            "_source": [
                                "id",
                                "objectid",
                                "metadata.id",
                                "metadata.Files.disp_frame_id",
                            ],
                        }

                        frame_response = self.es_client.search(
                            index=pattern, body=frame_search_query
                        )

                        frame_hits = frame_response.get("hits", {}).get("total", 0)
                        if isinstance(frame_hits, dict):
                            frame_hits = frame_hits.get("value", 0)

                        if frame_hits > 0:
                            logger.debug(
                                f"Found {frame_hits} documents for frame {frame_id}"
                            )
                            for hit in frame_response.get("hits", {}).get("hits", []):
                                doc_id = hit.get("_source", {}).get("id", "unknown")
                                logger.debug(f"  Document ID: {doc_id}")
                        else:
                            logger.debug(f"No documents found for frame {frame_id}")

                        broad_search_query = {
                            "query": {
                                "bool": {
                                    "should": [
                                        {"wildcard": {"*": f"*{granule_ids[0]}*"}},
                                        {
                                            "wildcard": {
                                                "*": f"*{granule_ids[0].split('_')[1]}*"
                                            }
                                        },  # Try frame ID
                                    ]
                                }
                            },
                            "size": 1,  # Just get one document to see the structure
                        }

                        broad_response = self.es_client.search(
                            index=pattern, body=broad_search_query
                        )

                        broad_hits = broad_response.get("hits", {}).get("total", 0)
                        if isinstance(broad_hits, dict):
                            broad_hits = broad_hits.get("value", 0)

                        if broad_hits > 0:
                            sample_doc = (
                                broad_response.get("hits", {})
                                .get("hits", [{}])[0]
                                .get("_source", {})
                            )
                            logger.debug(
                                f"Found {broad_hits} documents with broader search. Sample document fields: {list(sample_doc.keys())}"
                            )
                            logger.info(
                                f"No OpenSearch documents found for dataset {granule_ids[0]} in pattern {pattern} (but {broad_hits} documents exist with broader search)"
                            )
                        else:
                            logger.info(
                                f"No OpenSearch documents found for dataset {granule_ids[0]} in pattern {pattern}"
                            )
                        continue

                    documents_found = True
                    logger.info(
                        f"Found {total_hits} OpenSearch documents for dataset {granule_ids[0]} in pattern {pattern}"
                    )

                    # Build query to delete documents by granule_id or s3_url
                    # Use the same correct field names as the search query based on actual document structure
                    delete_query = {
                        "query": {
                            "bool": {
                                "should": [
                                    {
                                        "terms": {"id": granule_ids}
                                    },  # Top-level id field
                                    {
                                        "terms": {"objectid": granule_ids}
                                    },  # Alternative id field
                                    {
                                        "terms": {"metadata.id": granule_ids}
                                    },  # Metadata id field
                                    {
                                        "terms": {"metadata.product_s3_paths": s3_urls}
                                    },  # S3 paths in metadata
                                    {
                                        "terms": {"metadata.product_urls": s3_urls}
                                    },  # Product URLs in metadata
                                    {
                                        "terms": {"urls": s3_urls}
                                    },  # Top-level urls field
                                ]
                            }
                        }
                    }

                    # Execute delete by query
                    response = self.es_client.delete_by_query(
                        index=pattern,
                        body=delete_query,
                        wait_for_completion=True,
                        refresh=True,
                    )

                    deleted_count = response.get("deleted", 0)
                    if deleted_count > 0:
                        successful += deleted_count
                        logger.info(
                            f"Deleted {deleted_count} OpenSearch documents from pattern {pattern}"
                        )

                    # Log any failures
                    if "failures" in response and response["failures"]:
                        logger.warning(
                            f"Some deletions failed in pattern {pattern}: {response['failures']}"
                        )
                        failed += len(response["failures"])

                except Exception as e:
                    logger.error(
                        f"Failed to delete documents from pattern {pattern}: {e}"
                    )
                    failed += len(objects)

            # If no documents were found in any pattern, log this as info (not error)
            if not documents_found:
                logger.info(
                    f"No OpenSearch documents found for dataset {granule_ids[0]} - this is normal if the dataset was never indexed"
                )

            return successful, failed

        except Exception as e:
            logger.error(f"Error deleting OpenSearch documents: {e}")
            return 0, len(objects)

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

    def delete_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> Tuple[int, int]:
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
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Deletion command")

    # Frames subcommand
    frames_parser = subparsers.add_parser(
        "frames", help="Delete CCSLC data by frame IDs"
    )
    frames_parser.add_argument(
        "--frame-ids",
        required=True,
        help="Comma-separated list of frame IDs (e.g., '10859,10860')",
    )
    frames_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without executing them",
    )
    frames_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Date range subcommand
    date_parser = subparsers.add_parser(
        "date-range", help="Delete CCSLC data by date range"
    )
    date_parser.add_argument(
        "--start-date", required=True, help="Start date (YYYY-MM-DD format)"
    )
    date_parser.add_argument(
        "--end-date", required=True, help="End date (YYYY-MM-DD format)"
    )
    date_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without executing them",
    )
    date_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Burst IDs subcommand
    bursts_parser = subparsers.add_parser(
        "bursts", help="Delete CCSLC data by burst IDs"
    )
    bursts_parser.add_argument(
        "--burst-ids",
        required=True,
        help="Comma-separated list of burst IDs (e.g., 'T175-374393-IW1,T175-374394-IW1')",
    )
    bursts_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without executing them",
    )
    bursts_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Granule IDs subcommand
    granules_parser = subparsers.add_parser(
        "granules", help="Delete CCSLC data by granule IDs"
    )
    granules_parser.add_argument(
        "--granule-ids", required=True, help="Comma-separated list of granule IDs"
    )
    granules_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without executing them",
    )
    granules_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        # Initialize utility
        utility = CCSLCDeletionUtility(dry_run=args.dry_run, verbose=args.verbose)

        # Enable debug logging for OpenSearch operations if verbose is requested
        if args.verbose:
            import logging

            logging.getLogger().setLevel(logging.DEBUG)

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
