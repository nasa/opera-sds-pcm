from collections import defaultdict
import logging
import requests
import re
import sys
import datetime
import pandas as pd
import pickle

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

def match_up_disp_s1(data_should_trigger, data):

    # Create dictionary data structure for should_trigger
    frame_to_dayindex_to_granule = defaultdict(lambda: defaultdict(set))
    for item in data_should_trigger:
        frame_to_dayindex_to_granule[item['Frame ID']][item['Acq Day Index']] = item['All Bursts']

    # Remove any bursts that have COMPRESSED string in them
    ''' for disp_s1 in data:
        disp_s1['All Bursts'] = [b for b in disp_s1['All Bursts'] if 'COMPRESSED' not in b]
        disp_s1['All Bursts Count'] = len(disp_s1['All Bursts'])
    # pickle it out
    with open('data2.pkl', 'wb') as f:
        pickle.dump(data, f)
    '''

    for disp_s1 in data:
        matching_count = 0
        matching_bursts = []
        all_bursts_set = set([b.split("/")[-1][:-3] for b in disp_s1['All Bursts']]) # Get rid of the full file path and .h5 extension
        for acq_index in disp_s1['All Acq Day Indices']:
            if acq_index in frame_to_dayindex_to_granule[disp_s1['Frame ID']]:
                frame_data = frame_to_dayindex_to_granule[disp_s1['Frame ID']]
                acq_index_data  = frame_data[acq_index]
                intsect = all_bursts_set.intersection(acq_index_data)
                matching_count += len(intsect)
                matching_bursts.extend(list(intsect))
                all_bursts_set = all_bursts_set - intsect

        disp_s1['Matching Bursts'] = matching_bursts
        disp_s1['Matching Bursts Count'] = matching_count

        disp_s1['Unmatching Bursts'] = list(all_bursts_set)
        disp_s1['Unmatching Bursts Count'] = len(all_bursts_set)

    return data

def validate_disp_s1(start_date, end_date, timestamp, input_endpoint, output_endpoint, disp_s1_frames_only, shortname='OPERA_L2_CSLC-S1_V1'):
    """
        Validates that the granules from the CMR query are accurately reflected in the DataFrame provided.
        It extracts granule information based on the input dates and checks which granules are missing from the DataFrame.
        The function then updates the DataFrame to include a count of unprocessed bursts based on the missing granules.
        The logic can be summarized as:
        1. Gather list of expected CSLC granule IDs (provided dataframe)
        2. Query CMR for list of actual CSLC granule IDs used for DISP-S1 production, aggregate these into a list
        3. Compare list (1) with list (2) and return a new dataframe containing a column 'Unprocessed CSLC Native IDs' with the
           discrepancies

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

    # Determine which frame-dayindex pairs were supposed to have been processed. Remove any one that weren't supposed to have been processed.
    frame_to_dayindex_to_granule = get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames, frame_to_bursts)
    granules_should_trigger = filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames)
    data_should_trigger = []

    # Initialize smallest and greatest time to be something very large and very small
    smallest_date = datetime.datetime.strptime("2099-12-31T23:59:59.999999Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    greatest_date = datetime.datetime.strptime("1999-01-01T00:00:00.000000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info("Should have generated the following DISP-S1 products:")
    total_triggered = 0
    for frame_id in granules_should_trigger:
        for day_index in granules_should_trigger[frame_id]:
            total_triggered += 1
            logging.info("Frame ID: %s, Day Index: %s, Num CSLCs: %d, CSLCs: %s", frame_id, day_index, len(granules_should_trigger[frame_id][day_index]), granules_should_trigger[frame_id][day_index])
            data_should_trigger.append({
                'Frame ID': frame_id,
                'Acq Day Index': day_index,
                "All Bursts": granules_should_trigger[frame_id][day_index],
                'All Bursts Count': len(granules_should_trigger[frame_id][day_index])
            })
            for granule_id in granules_should_trigger[frame_id][day_index]:
                _, acquisition_dts, _, _ = parse_cslc_native_id(granule_id, burst_to_frames, frame_to_bursts)
                smallest_date = min(acquisition_dts, smallest_date)
                greatest_date = max(acquisition_dts, greatest_date)
    should_df = pd.DataFrame(data_should_trigger)

    # Pickle out the data_should_trigger dictionary for later use
    '''with open('data_should_trigger.pkl', 'wb') as f:
        pickle.dump(data_should_trigger, f)'''

    logging.info(f"Total number of DISP-S1 products that should have been generated: {total_triggered}")

    logging.info(f"Earliest acquisition date: {smallest_date}, Latest acquisition date: {greatest_date}")

    # Retrieve all DISP-S1 products from CMR within the acquisition time range
    all_disp_s1 = retrieve_r3_products(smallest_date, greatest_date, output_endpoint, _DISP_S1_PRODUCT_TYPE)
    filtered_disp_s1 = []
    for disp_s1 in all_disp_s1:

        # Getting to the frame_id is a bit of a pain
        for attrib in disp_s1.get("umm").get("AdditionalAttributes"):
            if attrib["Name"] == "FRAME_NUMBER" and int(attrib["Values"][0]) in frames_to_validate: # Should only ever belong to one frame

                # Need to perform secondary filter. Not sure if we always need to do this or temporarily so.
                actual_temporal_time = datetime.datetime.strptime(disp_s1.get("umm").get("TemporalExtent")['RangeDateTime']['EndingDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                if actual_temporal_time >= smallest_date and actual_temporal_time <= greatest_date:
                    filtered_disp_s1.append(disp_s1.get("umm").get("GranuleUR"))

    logging.info(f"Found {len(filtered_disp_s1)} DISP-S1 products:")
    logging.info(filtered_disp_s1)

    # Now query GRQ ES DISP-S1 product table to get more metadata and the input file lists for each DISP-S1 product
    # And then create a DataFrame out of it
    data = []
    es_util = es_conn_util.get_es_connection(None)
    for granule_id in filtered_disp_s1:
        disp_s1s = es_util.query(
            index=_DISP_S1_INDEX_PATTERNS,
            body={"query": {"bool": {"must": [
                {"match": {"id.keyword": granule_id}}
            ]}}})
        if len(disp_s1s) == 0:
            logging.error(f"Expected DISP-S1 product with ID {granule_id} in GRQ ES but not found.")
            continue
        assert len(disp_s1s) <= 1, "Expected at most one DISP-S1 product match by ID in GRQ ES. Delete the duplicate(s) and re-run"
        disp_s1 = disp_s1s[0]

        metadata = disp_s1["_source"]["metadata"]

        # Only use the CSLC input files
        all_bursts = [s.split("/")[-1] \
                      for s in metadata["lineage"] if "CSLC" in s and not "STATIC" in s and not "COMPRESSED" in s]

        #from "f8889_a168_f8889_a156_f8889_a144 to [168, 156, 144]
        all_acq_day_indices =  [int(s.split("_")[0]) for s in metadata["input_granule_id"].split("_a")[1:]]

        data.append({
            'Product ID': granule_id,
            'Frame ID': metadata["frame_id"],
            'Last Acq Day Index': metadata["acquisition_cycle"],
            'All Acq Day Indices': all_acq_day_indices,
            "All Bursts": all_bursts,
            'All Bursts Count': len(all_bursts),
            'Matching Bursts': [],
            'Matching Bursts Count': 0,
            'Unmatching Bursts': [],
            'Unmatching Bursts Count': 0
        })

    # Pickle out the data dictionary for later use
    '''with open('data.pkl', 'wb') as f:
        pickle.dump(data, f)'''

    # Match up data
    data = match_up_disp_s1(data_should_trigger, data)

    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    return should_df, df