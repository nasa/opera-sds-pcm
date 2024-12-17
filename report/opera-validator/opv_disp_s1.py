from collections import defaultdict
import logging
import requests
import re
import sys
import datetime
import pandas as pd

from opv_util import retrieve_r3_products, BURST_AND_DATE_GRANULE_PATTERN, get_granules_from_query
from data_subscriber import es_conn_util
from data_subscriber.cslc_utils import parse_cslc_native_id, localize_disp_frame_burst_hist, build_cslc_native_ids

_DISP_S1_INDEX_PATTERNS = "grq_v*_l3_disp_s1*"
_DISP_S1_PRODUCT_TYPE = "OPERA_L3_DISP-S1_V1"

def get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames, frame_to_bursts):
    """
    Looks something like:
    {8889:
      {0: ['S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1', 'S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1'] },
      {12: ['S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1', 'S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1'] }},
    8890:
      {0: ['S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1', 'S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1'] },
      {24: ['S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1', 'S1B_IW_SLC__1SDV_20210701T235959_20210702T000026_027000_033D7D_1'] }}
    }
    """

    # Create a map of frame IDs to acquisition day index to the granule ID
    frame_to_dayindex_to_granule = defaultdict(lambda: defaultdict(set))
    for granule_id in granule_ids:
        burst_id, acquisition_dts, acquisition_cycles, frame_ids = parse_cslc_native_id(granule_id, burst_to_frames,
                                                                                        frame_to_bursts)
        for frame_id in frame_ids:

            # 1. If the frame does not show up in the database file, skip it
            if frame_id not in frames_to_validate:
                logging.debug(f"Frame ID {frame_id} is not in the list of frames to validate. Skipping.")
                continue

            # 2. If the acquisition cycle is not in the database file, skip it
            acq_cycle = acquisition_cycles[frame_id]
            if acq_cycle < 0 or \
                    acq_cycle < frame_to_bursts[frame_id].sensing_datetime_days_index[-1] and acq_cycle not in frame_to_bursts[frame_id].sensing_datetime_days_index:
                logging.debug(f"Frame ID {frame_id} has no acquisition cycle {acq_cycle} in the database file. Skipping.")
                continue

            frame_to_dayindex_to_granule[frame_id][acq_cycle].add(granule_id)

    return frame_to_dayindex_to_granule

def filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames):
    '''
    Given a dictionary of frame IDs to day indices to granule IDs, filter for the frame that should trigger the DISP-S1 job.
    The frame at given day index is triggered if its granule burst ids is a subset of the corresponding list in the database.
    This is a purely deductive function. Remove any day indices that do not meet the criteria.
    WARNING! The input dictionary is modified in place. It's also being returned for convenience.
    '''

    for frame_id in frame_to_dayindex_to_granule:
        for day_index in list(frame_to_dayindex_to_granule[frame_id].keys()):
            granule_ids = frame_to_dayindex_to_granule[frame_id][day_index]
            burst_set = set()
            for granule_id in granule_ids:
                burst_id, _, _, _ = parse_cslc_native_id(granule_id, burst_to_frames, frame_to_bursts)
                burst_set.add(burst_id)
            if burst_set.issuperset(frame_to_bursts[frame_id].burst_ids):
                continue
            else:
                frame_to_dayindex_to_granule[frame_id].pop(day_index)

    # If the frame has no day indices left, remove it completely
    for frame_id in list(frame_to_dayindex_to_granule.keys()):
        if len(frame_to_dayindex_to_granule[frame_id].keys()) == 0:
            frame_to_dayindex_to_granule.pop(frame_id)

    return frame_to_dayindex_to_granule
def validate_disp_s1(start_date, end_date, timestamp, input_endpoint, output_endpoint, disp_s1_frames_only, shortname='OPERA_L2_CSLC-S1_V1'):

    # Process the disp s1 consistent database file
    frame_to_bursts, burst_to_frames, _ = localize_disp_frame_burst_hist()

    # If no frame list is provided, we will validate for all frames DISP-S1 is supposed to process.
    if disp_s1_frames_only is not None:
        frames_to_validate = set([int(f) for f in disp_s1_frames_only.split(',')])
        granules = []
        for f in frames_to_validate:
            for burst_id in frame_to_bursts[f].burst_ids:
                #TODO: Make the opv_utils function work so that they can use more than one native-id[] parameter. Currently this is slow and a bit ugly
                extra_params = {"options[native-id][pattern]": "true", "native-id[]": "OPERA_L2_CSLC-S1_"+burst_id+"*"} # build_cslc_native_ids returns a tuple
                granules.extend(get_granules_from_query(start=start_date, end=end_date, timestamp=timestamp, endpoint=input_endpoint,
                                                   provider="ASF", shortname=shortname, extra_params=extra_params))
    else:
        frames_to_validate = set(frame_to_bursts.keys())
        granules = get_granules_from_query(start=start_date, end=end_date, timestamp=timestamp, endpoint=input_endpoint, provider="ASF",
                                       shortname=shortname)

    if (granules):
        granule_ids = [granule.get("umm").get("GranuleUR") for granule in granules]
    else:
        logging.error("Problem querying for granules. Unable to proceed.")
        sys.exit(1)

    print(granule_ids)

    # Determine which frame-dayindex pairs were supposed to have been processed. Remove any one that weren't supposed to have been processed.
    frame_to_dayindex_to_granule = get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames, frame_to_bursts)
    granules_should_trigger = filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames)

    # Initialize smallest and greatest time to be something very large and very small
    smallest_date = datetime.datetime.strptime("2099-12-31T23:59:59.999999Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    greatest_date = datetime.datetime.strptime("1999-01-01T00:00:00.000000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info("Should have generated the following DISP-S1 products:")
    total_triggered = 0
    for frame_id in granules_should_trigger:
        for day_index in granules_should_trigger[frame_id]:
            total_triggered += 1
            logging.info("Frame ID: %s, Day Index: %s, Num CSLCs: %d, CSLCs: %s", frame_id, day_index, len(granules_should_trigger[frame_id][day_index]), granules_should_trigger[frame_id][day_index])
            for granule_id in granules_should_trigger[frame_id][day_index]:
                _, acquisition_dts, _, _ = parse_cslc_native_id(granule_id, burst_to_frames, frame_to_bursts)
                smallest_date = min(acquisition_dts, smallest_date)
                greatest_date = max(acquisition_dts, greatest_date)

    logging.info(f"Total number of DISP-S1 products that should have been generated: {total_triggered}")

    logging.info(f"Earliest acquisition date: {smallest_date}, Latest acquisition date: {greatest_date}")

    # Retrieve all DISP-S1 products from CMR within the acquisition time range
    all_disp_s1 = retrieve_r3_products(smallest_date, greatest_date, output_endpoint, _DISP_S1_PRODUCT_TYPE)
    filtered_disp_s1 = []
    for disp_s1 in all_disp_s1:

        # Getting to the frame_id is a bit of a pain
        for attrib in disp_s1.get("umm").get("AdditionalAttributes"):
            if attrib["Name"] == "FRAME_NUMBER":
                if int(attrib["Values"][0]) in frames_to_validate: # Should only ever belong to one frame
                    filtered_disp_s1.append(disp_s1.get("umm").get("GranuleUR"))

    logging.info("Found {len(filtered_disp_s1)} DISP-S1 products:")
    logging.info(filtered_disp_s1)

    return None # TODO

def validate_disp_s1_with_products(smallest_date, greatest_date, endpoint, df, logger):
    """
    Validates that the granules from the CMR query are accurately reflected in the DataFrame provided.
    It extracts granule information based on the input dates and checks which granules are missing from the DataFrame.
    The function then updates the DataFrame to include a count of unprocessed bursts based on the missing granules. 
    The logic can be summarized as:
    1. Gather list of expected CSLC granule IDs (provided dataframe)
    2. Query CMR for list of actual CSLC granule IDs used for DISP-S1 production, aggregate these into a list
    3. Compare list (1) with list (2) and return a new dataframe containing a column 'Unprocessed CSLC Native IDs' with the
       discrepancies

    :param smallest_date: datetime.datetime
        The earliest date in the range (ISO 8601 format).
    :param greatest_date: datetime.datetime
        The latest date in the range (ISO 8601 format).
    :param endpoint: str
        CMR environment ('UAT' or 'OPS') to specify the operational setting for the data query.
    :param df: pandas.DataFrame
        A DataFrame containing columns with granule identifiers which will be checked against the CMR query results.

    :return: pandas.DataFrame or bool
        A modified DataFrame with additional columns 'Unprocessed RTC Native IDs' and 'Unprocessed RTC Native IDs Count' showing
        granules not found in the CMR results and their count respectively. Returns False if the CMR query fails.

    Raises:
        requests.exceptions.RequestException if the CMR query fails, which is logged as an error.
    """

    all_granules = retrieve_r3_products(smallest_date, greatest_date, endpoint, 'OPERA_L2_CSLC-S1_V1')

    #es_util = es_conn_util.get_es_connection(logger)

    try:
        # Extract MGRS tiles and create the mapping to InputGranules
        available_cslc_bursts = []
        for item in all_granules:
            print(item)

            '''ccslc = es_util.query(
                index=_DISP_S1_INDEX_PATTERNS,
                body={"query": {"bool": {"must": [
                    {"term": {"metadata.ccslc_m_index.keyword": ccslc_m_index}},
                    {"term": {"metadata.frame_id": frame_id}}
                ]}}})'''

            input_granules = item['umm']['InputGranules']

            # Extract the granule burst ID from the full path
            for path in input_granules:
                match = re.search(BURST_AND_DATE_GRANULE_PATTERN, path)
                if match:
                    t_number = match.group(1)
                    orbit_number = match.group(2)
                    iw_number = match.group(3).lower()
                    burst_id = f't{t_number}_{orbit_number}_{iw_number}'
                    available_cslc_bursts.append(burst_id)

        # Function to identify missing bursts
        def filter_and_find_missing(row):
            cslc_bursts_in_df_row = set(row['Covered CSLC Native IDs'].split(', '))

            unprocessed_rtc_bursts = cslc_bursts_in_df_row - available_cslc_bursts
            if unprocessed_rtc_bursts:
                return ', '.join(unprocessed_rtc_bursts)
            return None  # or pd.NA 

        # Function to count missing bursts
        def count_missing(row):
            count = len(row['Unprocessed CSLC Native IDs'].split(', '))
            return count

        # Apply the function and create a new column 'Unprocessed CSLC Native IDs'
        df['Unprocessed CSLC Native IDs'] = df.apply(filter_and_find_missing, axis=1)
        df = df.dropna(subset=['Unprocessed CSLC Native IDs'])

        # Using loc to safely modify the DataFrame without triggering SettingWithCopyWarning
        df.loc[:, 'Unprocessed CSLC Native IDs Count'] = df.apply(count_missing, axis=1)

        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from CMR: {e}")

    return False


def map_cslc_bursts_to_frames(burst_ids, bursts_to_frames, frames_to_bursts):
    """
    Maps CSLC burst IDs to their corresponding frame IDs and identifies matching bursts within those frames.
    The logic this function performs can be summarized as:
    1. Take list of burst_ids and identify all associated frame IDs
    2. Take frame IDs from (1) and find all possible burst IDs associated with those frame IDs
    3. Take burst IDs from (2) and mark the ones available from burst IDs from (1)
    4. Return a dataframe that lists all frame IDs, all possible burst IDs from (2), and matched burst IDs from (1)

    :burst_ids: List of burst IDs to map.
    :bursts_to_frames: Dict that maps bursts to frames.
    :frames_to_bursts: Dict that maps frames to bursts.
    :return: A DataFrame with columns for frame IDs, all possible bursts, their counts, matching bursts, and their counts.
    """

    print(burst_ids)

    # Step 1: Map the burst IDs to their corresponding frame IDs
    frame_ids = set()
    for burst_id in burst_ids:
        frames = bursts_to_frames[burst_id]
        frame_ids.update(frames)

    # Step 2: For each frame ID, get all associated burst IDs from the frames_to_bursts mapping
    data = []
    for frame_id in frame_ids:
        frame_id_str = str(frame_id)
        associated_bursts = frames_to_bursts[int(frame_id_str)].burst_ids

        # Find the intersection of associated bursts and the input burst_ids
        matching_bursts = [burst for burst in burst_ids if burst in associated_bursts]
        if (len(associated_bursts) == 0 and len(matching_bursts) == 0):
            continue  # Ignore matching burst counts that are zero in number

        print("Got here")

        # Append the result to the data list
        data.append({
            "Frame ID": frame_id,
            "All Possible Bursts": associated_bursts,
            'All Possible Bursts Count': len(associated_bursts),
            "Matching Bursts": matching_bursts,
            'Matching Bursts Count': len(matching_bursts)
        })

    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    return df