#!/usr/bin/env python3

import argparse
import csv
import sys
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import dateutil.parser

from commons.es_connection import get_grq_es
from commons.logger import get_logger
import sys
import os
from rtc_utils import rtc_granule_regex, determine_acquisition_cycle
from data_subscriber.dist_s1_utils import parse_local_burst_db_pickle, localize_dist_burst_db
import re

'''Given a cmr survey csv file, populate the cmr_rtc_cache index with RTC granules from it'''

logger = get_logger()

def parse_rtc_granule_metadata(granule_id: str, bursts_to_products: dict = None) -> Dict[str, Any]:
    """
    Parse RTC granule metadata from granule ID.
    
    Args:
        granule_id: RTC granule ID (e.g., "OPERA_L2_RTC-S1_T168-359429-IW2_20231217T052415Z_20231220T055805Z_S1A_30_v1.0")
        bursts_to_products: Dictionary mapping burst_id to product_ids
    
    Returns:
        Dictionary containing parsed metadata
    """
    # Parse using regex pattern
    match = re.match(rtc_granule_regex, granule_id)
    if not match:
        logger.warning(f"Could not parse granule ID: {granule_id}")
        return None
    
    # Extract metadata
    burst_id = match.group("burst_id")
    acquisition_ts = match.group("acquisition_ts")
    revision_ts = match.group("creation_ts")
    sensor = match.group("sensor")
    product_version = match.group("product_version")
    
    # Parse acquisition and creation timestamps
    acquisition_dt = dateutil.parser.isoparse(acquisition_ts)
    revision_dt = dateutil.parser.isoparse(revision_ts)
    
    # Determine acquisition cycle
    acquisition_cycle = determine_acquisition_cycle(burst_id, acquisition_ts, granule_id)
    
    # If we have the DIST-S1 database, try to get the actual product_id
    '''if burst_id in bursts_to_products:
        product_ids = list(bursts_to_products[burst_id])
        if product_ids:
            product_id = product_ids[0]
            tile_id, acquisition_group = product_id.split("_")
            batch_id = f"{product_id}_{acquisition_cycle}"
            download_batch_id = f"p{product_id}_a{acquisition_cycle}"
            unique_id = f"{download_batch_id}_{burst_id}"
    else:
        return None'''
    
    # Because one burst can belong to multiple tiles and products having those information as lists won't be performant and not necessary
    return {
        "granule_id": granule_id,
        "burst_id": burst_id,
        "acquisition_timestamp": acquisition_dt,
        "revision_timestamp": revision_dt,
        "sensor": sensor,
        "product_version": product_version,
        "acquisition_cycle": acquisition_cycle
    }
    ''',
        "tile_id": tile_id,
        "acquisition_group": int(acquisition_group),
        "product_id": product_id,
        "batch_id": batch_id,
        "download_batch_id": download_batch_id,
        "unique_id": unique_id
    }'''

def read_cmr_survey_csv(csv_file: str, bursts_to_products: dict = None) -> List[Dict[str, Any]]:
    """
    Read CMR survey CSV file and extract RTC granule information.
    
    Args:
        csv_file: Path to the CSV file
        bursts_to_products: Dictionary mapping burst_id to product_ids
    
    Returns:
        List of dictionaries containing granule metadata
    """
    granules = []
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Process each row
        for index, row in df.iterrows():
            granule_id = row['# Granule ID']
            
            # Parse granule metadata
            metadata = parse_rtc_granule_metadata(granule_id, bursts_to_products)
            if metadata:
                granules.append(metadata)
                
        logger.info(f"Processed {len(granules)} RTC granules from CSV file")
        
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    return granules

def populate_cmr_rtc_cache(granules: List[Dict[str, Any]], es_conn) -> None:
    """
    Populate the cmr_rtc_cache index with RTC granule data.
    
    Args:
        granules: List of granule metadata dictionaries
        es_conn: ElasticSearch connection
    """
    index_name = "cmr_rtc_cache"
    
    # Index granules
    logger.info(f"Indexing {len(granules)} granules to {index_name}")
    
    for i, granule in enumerate(granules):
        try:
            # Use granule_id as document ID
            doc_id = granule["granule_id"]
            
            # Prepare document for indexing
            doc = {
                "granule_id": granule["granule_id"],
                "burst_id": granule["burst_id"],
                "acquisition_timestamp": granule["acquisition_timestamp"],
                "revision_timestamp": granule["revision_timestamp"],
                "sensor": granule["sensor"],
                "product_version": granule["product_version"],
                "acquisition_cycle": granule["acquisition_cycle"],
                "creation_timestamp": datetime.now()
            }
            
            # Index document
            es_conn.es.index(index=index_name, id=doc_id, body=doc)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Indexed {i + 1} granules...")
                
        except Exception as e:
            logger.error(f"Error indexing granule {granule['granule_id']}: {e}")
            continue
    
    # Refresh index
    es_conn.es.indices.refresh(index=index_name)
    logger.info(f"Successfully indexed {len(granules)} granules to {index_name}")

def main():
    parser = argparse.ArgumentParser(
        description="Populate GRQ ElasticSearch cmr_rtc_cache index with RTC granules from CMR survey CSV"
    )
    parser.add_argument("csv_file", help="Path to the CMR survey CSV file (e.g., cmr_survey.csv.raw.csv)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--db-file", help="Path to the DIST-S1 burst database pickle file")
    
    args = parser.parse_args()

    if args.db_file:
        # First see if a pickle file exists
        pickle_file_name = args.db_file + ".pickle"
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = parse_local_burst_db_pickle(args.db_file, pickle_file_name)
    else:
        dist_products, bursts_to_products, product_to_bursts, all_tile_ids = localize_dist_burst_db()
    
    if args.verbose:
        logger.setLevel("DEBUG")
    
    try:
        # Read CSV file
        logger.info(f"Reading CMR survey CSV file: {args.csv_file}")
        granules = read_cmr_survey_csv(args.csv_file, bursts_to_products)
        
        if not granules:
            logger.warning("No granules found in CSV file")
            return
        
        # Get ElasticSearch connection
        logger.info("Connecting to GRQ ElasticSearch")
        es_conn = get_grq_es(logger)
        
        # Populate cache
        logger.info("Populating cmr_rtc_cache index")
        populate_cmr_rtc_cache(granules, es_conn)
        
        logger.info("Successfully completed population of cmr_rtc_cache index")
        
    except Exception as e:
        logger.error(f"Error populating cmr_rtc_cache: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


