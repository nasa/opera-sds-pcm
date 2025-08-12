import asyncio
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Optional
import os
import re
import uuid

from commons.logger import get_logger
from data_subscriber.query import BaseQuery, DateTimeRange
from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB
from data_subscriber.gcov.gcov_catalog import GcovGranule
from hysds_commons.job_utils import submit_mozart_job
from data_subscriber.cslc_utils import get_s3_resource_from_settings

DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH = "MGRS_collection_db_DSWx-NI_v0.1.sqlite"

@dataclass
class DswxNiProductsToProcess:
    mgrs_set_id: str
    cycle_number: int
    gcov_input_product_urls: list[str]

class NisarGcovCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.logger = get_logger()

        # source track frame db from ancillary bucket or loads local copy
        self.mgrs_track_frame_db = self._load_mgrs_track_frame_db()
        
        self.mgrs_sets_to_process = {}

    @cache
    def _load_mgrs_track_frame_db(self):
        """
        Load the MGRS track frame database that maps frame numbers to MGRS set IDs.

        Cached function to avoid re-downloading the database file on every query.
        
        Args:
            db_file_path: Path to the database file
            
        Returns:
            Dictionary mapping frame numbers to MGRS set IDs
        """
        try: 
            s3, path, file, db_file_url = get_s3_resource_from_settings("DSWX_NI_MGRS_TILE_COLLECTION_DB_S3PATH")
            self.logger.info(f"Loading MGRS track frame database from {db_file_url}")
            s3.Object(db_file_url.netloc, path).download_file(file)
        except Exception:
            self.logger.warning(f"Could not download DSWx-NI mgrs tile collection database."
                                f"Attempting to use local copy at {DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH}.")
            if not os.path.exists(DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH):
                raise FileNotFoundError(f"Local copy of DSWx-NI mgrs tile collection database not found at {DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH}")
            file = DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH
 
        return MGRSTrackFrameDB(file)

    def query_cmr(self, timerange, now):
        """
        Query CMR for NISAR L2 GCOV products.
        
        Args:
            timerange: DateTimeRange object containing start and end dates
            now: Current datetime
            
        Returns:
            List of granules from CMR
        """
        self.logger.info(f"Query CMR for NISAR L2 GCOV products with timerange: {timerange}")
        
        cmr_granules = super().query_cmr(timerange, now)
        return cmr_granules
    
    def determine_download_granules(self, cmr_granules):
        query_dt = datetime.now()
        gcov_granules, mgrs_sets_and_cycle_numbers = self._convert_query_result_to_gcov_granules(cmr_granules)
        self.logger.info(f"Found {len(gcov_granules)} GCOV granules")
        self.logger.info(f"Found {len(mgrs_sets_and_cycle_numbers)} unique MGRS sets and cycle numbers")
        
        self._catalog_granules(gcov_granules, query_dt)

        sets_to_process = []
        for mgrs_set_id, cycle_number in mgrs_sets_and_cycle_numbers:
            self.logger.info(f"Retrieving related GCOV products for (MGRS set, cycle) pair: ({mgrs_set_id}, {cycle_number})")
            related_gcov_products = self.es_conn.get_gcov_products_from_catalog(mgrs_set_id, cycle_number) 
            if self.meets_criteria_for_processing(mgrs_set_id, cycle_number, related_gcov_products):
                gcov_input_product_urls = [product["_source"]["s3_download_url"] for product in related_gcov_products]
                self.logger.info(f"({mgrs_set_id}, {cycle_number}) meets criteria for processing")
                sets_to_process.append(DswxNiProductsToProcess(mgrs_set_id, cycle_number, gcov_input_product_urls))

        self.logger.info(f"Found {len(sets_to_process)} unique MGRS sets and cycle numbers to process")

        return sets_to_process

    def submit_dswx_ni_job_submission_handler(self, sets_to_process, query_timerange):
        self.logger.info(f"Triggering DSWx-NI jobs for {len(sets_to_process)} unique MGRS sets and cycle numbers to process")
        jobs = self.trigger_dswx_ni_jobs(sets_to_process)
        return jobs

    def _catalog_granules(self, granules, query_dt):
        for granule in granules:
            self.logger.info(f"Cataloging GCOV granule: {granule.native_id}")
            self.es_conn.update_granule_index(granule, self.job_id, query_dt)
    
    def create_dswx_ni_job_params(self, set_to_process):
        metadata = {
            "dataset": f"L3_DSWx_NI-{set_to_process.mgrs_set_id}-{set_to_process.cycle_number}",
            "metadata": {
                "mgrs_set_id": set_to_process.mgrs_set_id,
                "cycle_number": set_to_process.cycle_number,
                "product_paths": {"L2_NISAR_GCOV": set_to_process.gcov_input_product_urls},  # The S3 paths to localize
                "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "FileName": set_to_process.mgrs_set_id,
                "FileLocation": set_to_process.gcov_input_product_urls, 
                "id": set_to_process.mgrs_set_id,
                "Files": [
                    {
                        "FileName": PurePath(s3_path).name,
                        "FileSize": 1, 
                        "FileLocation": s3_path,
                        "id": PurePath(s3_path).name,
                        "product_paths": "$.product_paths"
                    } for s3_path in set_to_process.gcov_input_product_urls
                ]
            }
        }
        return [{
            "name": "mgrs_set_id",
            "from": "value",
            "type": "text",
            "value": set_to_process.mgrs_set_id
        }, {
            "name": "cycle_number",
            "from": "value",
            "type": "text",
            "value": set_to_process.cycle_number
        }, {
            "name": "gcov_input_product_urls",
            "from": "value",
            "type": "object",
            "value": set_to_process.gcov_input_product_urls
        },
        {
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": metadata
        }]

    def trigger_dswx_ni_jobs(self, sets_to_process):
        return [submit_dswx_ni_job(
            params=self.create_dswx_ni_job_params(set_to_process),
            job_queue=self.args.job_queue,
            job_name=f"job-WF-SCIFLO_L3_DSWx_NI-{set_to_process.mgrs_set_id}-{set_to_process.cycle_number}",
            release_version=self.settings["RELEASE_VERSION"]
        ) for set_to_process in sets_to_process]

    def catalog_granules(self, granules, query_dt):
        return

    def meets_criteria_for_processing(self, mgrs_set_id, cycle_number, related_gcov_products):
        return True
    
    def _convert_query_result_to_gcov_granules(self, granules: list) -> list[GcovGranule]:
        """
        Convert a list of CMR granule dicts to a list of GcovGranule objects.
        """
        gcov_granules = []
        mgrs_sets_and_cycle_numbers = set() # set of tuples for uniquqly identifying L3 products(mgrs_set_id, cycle_number)
        for granule in granules:
            granule_id = granule.get("granule_id")
            native_id = granule_id

            # Find s3_download_url
            s3_download_url = None
            # matches s3://*001.h5
            # input example: s3://sds-n-cumulus-test-nisar-products/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"
            s3_regex = r'^s3:\/\/.*\d\d\d\.h5$' 
            for url in granule.get("related_urls", []):
                if re.match(s3_regex, url):
                    s3_download_url = url
                    break

            # Track, frame and cycle number
            track_number = self._extract_track_id(granule)
            frame_number = self._extract_frame_id(granule)
            cycle_number = self._extract_cycle_number(granule)


            # MGRS set id: use DB lookup
            mgrs_set_id = None
            try:
                mgrs_sets = self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets({(frame_number, track_number)})
                if mgrs_sets:
                    mgrs_set_id = next(iter(mgrs_sets.keys()))
            except Exception:
                self.logger.warning(f"Error getting MGRS set ID for granule {granule_id}")
                mgrs_set_id = None

            # Acquisition times
            revision_dt = datetime.fromisoformat(granule.get("revision_date").replace("Z", "+00:00"))
            acquisition_start_time = datetime.fromisoformat(granule.get("temporal_extent_beginning_datetime").replace("Z", "+00:00"))

            mgrs_sets_and_cycle_numbers.add((mgrs_set_id, cycle_number))
            gcov_granules.append(GcovGranule(
                native_id=native_id,
                granule_id=granule_id,
                s3_download_url=s3_download_url,
                track_number=track_number,
                frame_number=frame_number,
                cycle_number=cycle_number,
                mgrs_set_id=mgrs_set_id,
                revision_dt=revision_dt,
                acquisition_start_time=acquisition_start_time,
            ))
        return gcov_granules, mgrs_sets_and_cycle_numbers
    
    def _get_frames_and_track_ids_from_granules(self, granules):
        """
        Extract frame numbers and track IDs from a list of granules.
        
        Args:
            granules: List of granules from CMR
            
        Returns:
            Set of tuples (frame number, track ID)
        """
        return set((self._extract_frame_id(granule), self._extract_track_id(granule)) for granule in granules)     
 
    def _extract_frame_id(self, granule):
        """
        Extract the frame ID from a granule.
        
        Args:
            granule: Granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
            
        Returns:
            int: Frame ID
        """
        # This might be in the granule_id or in some metadata field
        granule_id = granule["granule_id"]
        return int(granule_id.split("_")[7])

    def _extract_track_id(self, granule):
        """
        Extract the track ID from a granule.
        
        Args:
            granule: Granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
            
        Returns:
            int: Track ID
        """
        # This might be in the granule_id or in some metadata field
        granule_id = granule["granule_id"]
        return int(granule_id.split("_")[5])
    
    def _extract_cycle_number(self, granule):
        """
        Extract the cycle number from a granule.
        
        Args:
            granule: Granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
            
        Returns:
            int: Cycle number
        """
        # This might be in the granule_id or in some metadata field
        granule_id = granule["granule_id"]
        return int(granule_id.split("_")[4])


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
