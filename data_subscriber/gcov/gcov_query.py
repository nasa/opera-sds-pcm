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

from opera_commons.logger import get_logger
from data_subscriber.query import BaseQuery, DateTimeRange
from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB
from data_subscriber.gcov.gcov_catalog import GcovGranule
from data_subscriber.gcov.gcov_granule_util import extract_track_id, extract_frame_id, extract_cycle_number
from hysds_commons.job_utils import submit_mozart_job
from data_subscriber.gcov_utils import load_mgrs_track_frame_db, get_gcov_products_to_process

DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH = "MGRS_collection_db_DSWx-NI_v0.1.sqlite"

class NisarGcovCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, mgrs_track_frame_db_file=None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.logger = get_logger()

        # source track frame db from ancillary bucket or loads local copy
        self.mgrs_track_frame_db = load_mgrs_track_frame_db(mgrs_track_frame_db_file=mgrs_track_frame_db_file)
        
        self.mgrs_sets_to_process = {}

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

        return mgrs_sets_and_cycle_numbers
    
    def submit_gcov_download_job_submission_handler(self, mgrs_sets_and_cycle_numbers, query_timerange):
        self.logger.info(f"Triggering GCOV jobs for {len(sets_to_process)} unique MGRS sets and cycle numbers to process")
        jobs = self.trigger_gcov_download_jobs(mgrs_sets_and_cycle_numbers)
        return jobs
    
    def create_gcov_download_product(self, mgrs_set, cycle_number):
        return {
            "_source": {
                "metadata": {
                    "batch_id": f"{mgrs_set}_{cycle_number}"
                }
            }
        }

    def trigger_gcov_download_jobs(self, mgrs_sets_and_cycle_numbers):
        jobs = []
        for mgrs_set, cycle_number in mgrs_sets_and_cycle_numbers:
            product = self.create_gcov_download_product((mgrs_set, cycle_number))
            jobs.append(submit_gcov_download_job(
                        params=self.create_gcov_download_job_params(self.args,
                                                                    product=product,
                                                                    batch_ids=[f"{mgrs_set}_{cycle_number}" 
                                                                                for mgrs_set, cycle_number in mgrs_sets_and_cycle_numbers],
                                                                    release_version=self.args.release_version),
                        job_queue=self.args.job_queue,
                        job_name=f"job-WF-gcov_download",
                        release_version=self.settings["RELEASE_VERSION"]
        ))
        return jobs

    def _catalog_granules(self, granules, query_dt):
        for granule in granules:
            self.logger.info(f"Cataloging GCOV granule: {granule.native_id}")
            self.es_conn.update_granule_index(granule, self.job_id, query_dt)
    
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
            track_number = extract_track_id(granule)
            frame_number = extract_frame_id(granule)
            cycle_number = extract_cycle_number(granule)


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

    def create_gcov_download_job_params(self, args=None, product=None, batch_ids=None, release_version: str = None):
        return [
            {
                "name": "batch_ids",
                "value": "--batch-ids " + " ".join(batch_ids) if batch_ids else "",
                "from": "value"
            },
            {
                "name": "smoke_run",
                "value": "--smoke-run" if args.smoke_run else "",
                "from": "value"
            },
            {
                "name": "dry_run",
                "value": "--dry-run" if args.dry_run else "",
                "from": "value"
            },
            {
                "name": "endpoint",
                "value": f"--endpoint={args.endpoint}",
                "from": "value"
            },
            {
                "name": "transfer_protocol",
                "value": f"--transfer-protocol={args.transfer_protocol}",
                "from": "value"
            },
            {
                "name": "proc_mode",
                "value": f"--processing-mode={args.proc_mode}",
                "from": "value"
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": json.dumps(product["_source"])
            },
            {
                "name": "dataset_type",
                "from": "value",
                "type": "text",
                "value": self.dataset_type
            },
            {
                "name": "input_dataset_id",
                "type": "text",
                "from": "value",
                "value": product["_source"]["metadata"]["batch_id"]
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": product["_source"]
            }
        ]