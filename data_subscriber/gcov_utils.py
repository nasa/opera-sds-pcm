from functools import cache
from dataclasses import dataclass
import os
import uuid

from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB
from data_subscriber.cslc_utils import get_s3_resource_from_settings
from opera_commons.logger import get_logger
from hysds_commons.job_utils import submit_mozart_job

DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH = "MGRS_collection_db_DSWx-NI_v0.1.sqlite"
logger = get_logger()

@dataclass
class DswxNiProductsToProcess:
    mgrs_set_id: str
    cycle_number: int
    gcov_input_product_urls: list[str]

@cache
def load_mgrs_track_frame_db(mgrs_track_frame_db_file=None):
    """
    Load the MGRS track frame database that maps frame numbers to MGRS set IDs.

    Cached function to avoid re-downloading the database file on every query.
    
    Args:
        db_file_path: Path to the database file
        
    Returns:
        Dictionary mapping frame numbers to MGRS set IDs
    """
    try: 
        if mgrs_track_frame_db_file:
            file = mgrs_track_frame_db_file
        else:
            s3, path, file, db_file_url = get_s3_resource_from_settings("DSWX_NI_MGRS_TILE_COLLECTION_DB_S3PATH")
            logger.info(f"Loading MGRS track frame database from {db_file_url}")
            s3.Object(db_file_url.netloc, path).download_file(file)
    except Exception:
        logger.warning(f"Could not download DSWx-NI mgrs tile collection database."
                            f"Attempting to use local copy at {DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH}.")
        if not os.path.exists(DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH):
            raise FileNotFoundError(f"Local copy of DSWx-NI mgrs tile collection database not found at {DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH}")
        file = DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH

    return MGRSTrackFrameDB(file)

def meets_criteria_for_processing(mgrs_set_id, cycle_number, related_gcov_products):
    return True

def split_mgrs_set_id_and_cycle_number(mgrs_set_id_and_cycle_number):
    return mgrs_set_id_and_cycle_number.split("_")

def get_gcov_products_to_process(mgrs_sets_and_cycle_numbers, es_conn):
    sets_to_process = []
    for mgrs_set_id, cycle_number in mgrs_sets_and_cycle_numbers:
        logger.info(f"Retrieving related GCOV products for (MGRS set, cycle) pair: ({mgrs_set_id}, {cycle_number})")
        related_gcov_products = es_conn.get_gcov_products_from_catalog(mgrs_set_id, cycle_number) 
        if meets_criteria_for_processing(mgrs_set_id, cycle_number, related_gcov_products):
            gcov_input_product_urls = [product["_source"]["s3_download_url"] for product in related_gcov_products]
            logger.info(f"({mgrs_set_id}, {cycle_number}) meets criteria for processing")
            sets_to_process.append(DswxNiProductsToProcess(mgrs_set_id, cycle_number, gcov_input_product_urls))
    logger.info(f"Found {len(sets_to_process)} unique MGRS sets and cycle numbers to process")
    return sets_to_process

def submit_dswx_ni_job(params: list[dict[str, str]],
                       job_queue: str,
                       job_name = None,
                       payload_hash = None,
                       release_version = None) -> str:
    """
    Submit a DSWx-NI job to Mozart.
    
    Args:
        release_version: Release version for the job
        product_type: Type of product (e.g., 'dswx_ni')
        params: List of job parameters
        job_queue: Queue to submit the job to
        job_name: Optional custom job name
        payload_hash: Optional payload hash for deduplication
        
    Returns:
        str: Job ID of the submitted job
    """
    job_type = "job-SCIFLO_L3_DSWx_NI"
    job_spec_str = f"{job_type}:{release_version}"
    
    if not job_name:
        job_name = f"job-WF-{job_type}"

    return submit_mozart_job(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec_str
        },
        product={},
        rule={
            "rule_name": f"trigger-{job_type}",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=job_queue,
        job_name=job_name,
        payload_hash=payload_hash,
        enable_dedup=None
    )
 
def submit_gcov_download_job(params: list[dict[str, str]],
                            product: dict,
                            job_queue: str,
                            job_name = None,
                            release_version = None) -> str:
    job_spec_str = f"job-gcov_download:{release_version}"
    return submit_mozart_job(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec_str
        },
        product=product,
        rule={
            "rule_name": f"trigger-gcov_download",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
    )
