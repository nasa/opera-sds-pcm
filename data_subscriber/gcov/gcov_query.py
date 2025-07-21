import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import Optional

from commons.logger import get_logger
from data_subscriber.query import CmrQuery, DateTimeRange
from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB
from data_subscriber.gcov.gcov_catalog import GcovGranule



@dataclass
class DswxNiProductsToProcess:
    mgrs_set_id: str
    cycle_number: int
    gcov_input_product_urls: list[str]

class NisarGcovCmrQuery(CmrQuery):
    """
    CMR Query class for NISAR GCOV products to support DSWx-NI triggering.
    This class queries CMR for L2 GCOV products and prepares them for cataloging.
    """

    def __init__(self, args, token, es_conn, cmr, job_id, settings, mgrs_track_frame_db_file=None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.logger = get_logger()
        
        # If an MGRS track frame database is provided, use it; otherwise we'll need to implement
        # logic to load it from a default location
        self.mgrs_track_frame_db = self._load_mgrs_track_frame_db(mgrs_track_frame_db_file)
        
        self.mgrs_sets_to_process = {}

    def _load_mgrs_track_frame_db(self, db_file_path):
        """
        Load the MGRS track frame database that maps frame numbers to MGRS set IDs.
        
        Args:
            db_file_path: Path to the database file
            
        Returns:
            Dictionary mapping frame numbers to MGRS set IDs
        """
        self.logger.info(f"Loading MGRS track frame database from {db_file_path}")

        # TODO: default file path for mgrs track frame db
        if not db_file_path:
            raise ValueError("Path to database file must be provided")
        return MGRSTrackFrameDB(db_file_path)

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
        
        # Execute the CMR query
        query_dt = datetime.now()
        cmr_granules = super().query_cmr(timerange, now)
        gcov_granules, mgrs_sets_and_cycle_numbers = self._convert_query_result_to_gcov_granules(cmr_granules)
        self.logger.info(f"Found {len(gcov_granules)} GCOV granules")
        self.logger.info(f"Found {len(mgrs_sets_and_cycle_numbers)} unique MGRS sets and cycle numbers")
        
        for granule in gcov_granules:
            self.logger.info(f"Cataloging GCOV granule: {granule.native_id}")
            self.catalog.update_granule_index(granule, self.job_id, query_dt)

        sets_to_process = []
        for mgrs_set_id, cycle_number in mgrs_sets_and_cycle_numbers:
            self.logger.info(f"Retrieving related GCOV products for (MGRS set, cycle) pair: ({mgrs_set_id}, {cycle_number})")
            related_gcov_products = self.catalog.get_related_gcov_products_from_catalog(mgrs_set_id, cycle_number) 
            if self.meets_criteria_for_processing(mgrs_set_id, cycle_number, related_gcov_products):
                gcov_input_product_urls = [product["_source"]["s3_download_url"] for product in related_gcov_products["hits"]]
                self.logger.info(f"({mgrs_set_id}, {cycle_number}) meets criteria for processing")
                sets_to_process.append(DswxNiProductsToProcess(mgrs_set_id, cycle_number, gcov_input_product_urls))

        self.logger.info(f"Found {len(sets_to_process)} unique MGRS sets and cycle numbers to process")

        jobs = self.trigger_dswx_ni_jobs(sets_to_process)
        return sets_to_process
    
    def meets_criteria_for_processing(self, mgrs_set_id, cycle_number, related_gcov_products):
        return True
    
    def _convert_query_result_to_gcov_granules(self, granules: list) -> list[GcovGranules]:
        """
        Convert a list of CMR granule dicts to a list of GcovGranule objects.
        """
        gcov_granules = []
        mgrs_sets_and_cycle_numbers = set() # set of tuples for uniquqly identifying L3 products(mgrs_set_id, cycle_number)
        for granule in granules:
            meta = granule.get("meta", {})
            umm = granule.get("umm", {})
            granule_id = umm.get("GranuleUR", "")
            native_id = meta.get("native-id", "")

            # Find s3_download_url
            s3_download_url = None
            for url in umm.get("RelatedUrls", []):
                if url.get("Type") == "GET DATA VIA DIRECT ACCESS" and url.get("Format") == "HDF5":
                    s3_download_url = url.get("URL")
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
            temporal = umm.get("TemporalExtent", {}).get("RangeDateTime", {})
            revision_dt = datetime.fromisoformat(meta.get("revision-date", "").replace("Z", "+00:00"))
            acq_start = temporal.get("BeginningDateTime")
            acq_end = temporal.get("EndingDateTime")
            acquisition_start_time = datetime.fromisoformat(acq_start.replace("Z", "+00:00")) if acq_start else None
            acquisition_end_time = datetime.fromisoformat(acq_end.replace("Z", "+00:00")) if acq_end else None

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
                acquisition_end_time=acquisition_end_time
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
        return int(granule_id.split("_")[3])
