"""
Provides utility functions for extracting fields from GCOV granules.
"""
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
        granule: Granule dictionary from data_subscriber.cmr.response_jsons_to_cmr_granules
        
    Returns:
        int: Frame ID
    """
    # This might be in the granule_id or in some metadata field
    granule_id = granule["granule_id"]
    return int(granule_id.split("_")[7])

def extract_track_id(granule):
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

def extract_cycle_number(granule):
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