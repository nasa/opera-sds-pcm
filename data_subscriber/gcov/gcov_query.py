import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime

from commons.logger import get_logger
from data_subscriber.query import CmrQuery, DateTimeRange
from data_subscriber.cmr import CMR_TIME_FORMAT, async_query_cmr
from data_subscriber.gcov.mgrs_track_collections_db import MGRSTrackFrameDB


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
        
        # This will be populated during job determination
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
        granules = super().query_cmr(timerange, now)
                
        return granules
    
    def _get_frames_from_granules(self, granules):
        """
        Extract frame numbers from a list of granules.
        
        Args:
            granules: List of granules from CMR
            
        Returns:
            List of frame numbers
        """
        return [self._extract_frame_id(granule) for granule in granules]
 
    def _extract_frame_id(self, granule):
        """
        Extract the frame ID from a granule.
        
        Args:
            granule: Granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
            
        Returns:
            Frame ID or None if not found
        """
        # This might be in the granule_id or in some metadata field
        granule_id = granule["granule_id"]
        return int(granule_id.split("_")[7])
    
    def _get_mgrs_sets_from_granules(self, granules):
        """
        Lookup MGRS set IDs with frames from a list of granules.
        
        Args:
            granules: List of granules from CMR
            
        Returns:
            Dictionary mapping MGRS set IDs to frame numbers
        """
        frames = self._get_frames_from_granules(granules)
        mgrs_sets_with_frames = self.mgrs_track_frame_db.frame_numbers_to_mgrs_sets_with_frames(frames)
        return mgrs_sets_with_frames
