"""
Provides utility functions for extracting fields from GCOV granules.
"""

from datetime import datetime
from typing import Tuple


CYCLE_IDX = 4
TRACK_IDX = 5
ORBIT_DIR_IDX = 6
FRAME_IDX = 7
MODE_IDX = 8
POLARIZATION_IDX = 9
START_DT_IDX = 11
END_DT_IDX = 12
CRID_IDX = 13


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
    return int(granule_id.split("_")[FRAME_IDX])


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
    return int(granule_id.split("_")[TRACK_IDX])


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
    return int(granule_id.split("_")[CYCLE_IDX])


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
    return str(granule_id.split("_")[ORBIT_DIR_IDX])


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

    start = granule_fields[START_DT_IDX]
    end = granule_fields[END_DT_IDX]

    return datetime.strptime(start, "%Y%m%dT%H%M%S"), datetime.strptime(end, "%Y%m%dT%H%M%S")


def extract_polarization(granule) -> str:
    """
    Extract the primary-band polarization from a granule.

    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules

    Returns:
        str: Polarization
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return granule_id.split("_")[POLARIZATION_IDX][:2]


def extract_bandwidth_mode(granule) -> str:
    """
    Extract the primary-band bandwidth mode from a granule.

    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules

    Returns:
        str: Bandwidth mode
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return granule_id.split("_")[MODE_IDX][:2]


def extract_crid(granule) -> str:
    """
    Extract the CRID from a granule.

    Args:
        granule: Granule ID string or granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules

    Returns:
        str: CRID
    """
    # This might be in the granule_id or in some metadata field
    if isinstance(granule, dict):
        granule_id = granule["granule_id"]
    else:
        granule_id = granule
    return granule_id.split("_")[CRID_IDX]
