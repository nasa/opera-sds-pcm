"""
Provides utility functions for extracting fields from GCOV granules.
"""

from datetime import datetime
from typing import Tuple


def extract_frames_and_track_ids_from_granules(granules):
    """
    Extract frame numbers and track IDs from a list of granules.
    
    Args:
        granules: List of granules from CMR
        
    Returns:
        Set of tuples (frame number, track ID)
    """
    return set((extract_frame_id(granule), extract_track_id(granule)) for granule in granules)     


def extract_frame_id(granule):
    """
    Extract the frame ID from a granule.
    
    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
        
    Returns:
        int: Frame ID
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return int(granule_id.split("_")[7])


def extract_track_id(granule):
    """
    Extract the track ID from a granule.
    
    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
        
    Returns:
        int: Track ID
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return int(granule_id.split("_")[5])


def extract_cycle_number(granule):
    """
    Extract the cycle number from a granule.
    
    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
        
    Returns:
        int: Cycle number
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return int(granule_id.split("_")[4])


def extract_orbit_direction(granule) -> str:
    """
    Extract the orbit direction from a granule.

    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules

    Returns:
        str: Orbit direction ('A' or 'D')
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return str(granule_id.split("_")[6])


def extract_acquisition_time_range(granule) -> Tuple[datetime, datetime]:
    """
    Extract the acquisition start and end time from a granule.

    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules

    Returns:
        Tuple[datetime, datetime]: Acquisition start and end time
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule

    granule_fields = granule_id.split("_")

    start = granule_fields[11]
    end = granule_fields[12]

    return datetime.strptime(start, "%Y%m%dT%H%M%S"), datetime.strptime(end, "%Y%m%dT%H%M%S")

