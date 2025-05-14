from collections import defaultdict
import logging
import copy
import sys
import datetime
import pandas as pd
import pickle

from report.opera_validator.opv_util import retrieve_r3_products, BURST_AND_DATE_GRANULE_PATTERN, get_granules_from_query
from data_subscriber import es_conn_util
from data_subscriber.cslc_utils import parse_cslc_native_id, localize_disp_frame_burst_hist, build_cslc_native_ids

_DISP_S1_INDEX_PATTERNS = "grq_v*_l3_disp_s1*"
_DISP_S1_PRODUCT_TYPE = "OPERA_L3_DISP-S1_V1"

def get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames, frame_to_bursts, processing_mode):
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
                    (processing_mode == "historical" and acq_cycle not in frame_to_bursts[frame_id].sensing_datetime_days_index):
                logging.info(f"Frame ID {frame_id} acquisition index {acq_cycle} is either 0 or not in the database file while in historical mode. Skipping.")
                continue

            frame_to_dayindex_to_granule[frame_id][acq_cycle].add(granule_id)

    return frame_to_dayindex_to_granule

def filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames):
    '''
    Given a dictionary of frame IDs to day indices to granule IDs, filter for the frame that should trigger the DISP-S1 job.
    The frame at given day index is triggered if its granule burst ids is a subset of the corresponding list in the database.
    This is a purely deductive function. Remove any day indices that do not meet the criteria.

    Also remove duplicate CSLC granules (defined by the same burst id, differed by production time)

    WARNING! The input dictionary is modified in place. It's also being returned for convenience.
    '''

    for frame_id in frame_to_dayindex_to_granule:
        for day_index in list(frame_to_dayindex_to_granule[frame_id].keys()):
            granule_ids = frame_to_dayindex_to_granule[frame_id][day_index]
            burst_set = set()
            unique_granules = {} # burst_id -> granule_id
            for granule_id in granule_ids:
                burst_id, _, _, _ = parse_cslc_native_id(granule_id, burst_to_frames, frame_to_bursts)
                if burst_id in frame_to_bursts[frame_id].burst_ids:
                    burst_set.add(burst_id)

                    # If we have duplicate burst ids, keep the one with the latest production time
                    if burst_id in unique_granules:
                        production_time_old = unique_granules[burst_id].split("_")[-4]
                        production_time_new = granule_id.split("_")[-4]
                        if production_time_new > production_time_old:
                            unique_granules[burst_id] = granule_id
                    else:
                        unique_granules[burst_id] = granule_id

            if burst_set.issuperset(frame_to_bursts[frame_id].burst_ids):
                frame_to_dayindex_to_granule[frame_id][day_index] = unique_granules.values()
            else:
                frame_to_dayindex_to_granule[frame_id].pop(day_index)

    # If the frame has no day indices left, remove it completely
    for frame_id in list(frame_to_dayindex_to_granule.keys()):
        if len(frame_to_dayindex_to_granule[frame_id].keys()) == 0:
            frame_to_dayindex_to_granule.pop(frame_id)

    return frame_to_dayindex_to_granule

def match_up_disp_s1(data_should_trigger, disp_s1s, processing_mode, k, frame_to_bursts):

    k_set_map = defaultdict(lambda: defaultdict(set)) # Used for determining non-k-complete acq indices in historical mode

    # Create dictionary data structure for should_trigger
    frame_to_dayindex_to_granule = defaultdict(lambda: defaultdict(set))
    for item in data_should_trigger:
        frame = item['Frame ID']
        acq_index = item['Acq Day Index']
        frame_to_dayindex_to_granule[frame][acq_index] = item['All Bursts']

        if processing_mode == "historical":
            # # Group acq indices by frame ID and then by k-set number
            index_number = frame_to_bursts[frame].sensing_datetime_days_index.index(acq_index)  # note "index" is overloaded term here
            k_set = index_number // k
            k_set_map[frame][k_set].add(acq_index)
            logging.debug(f"Frame {frame} Acq Index {acq_index} K-Set {k_set}")

    # Determine all frame / acq indices that weren't part of a k-complete set, only applicable in historical mode
    skip_cslc_validation = set()
    for frame_id in k_set_map:
        for k_set in k_set_map[frame_id]:
            if len(k_set_map[frame_id][k_set]) < k:
                for acq_index in k_set_map[frame_id][k_set]:
                    skip_cslc_validation.add((frame_id, acq_index))
                    logging.info(f"Frame {frame_id} Acq Index {acq_index} K-Set {k_set} is not k-complete so will ignore during validation.")

                    # Also add the last acq index of that k-set to the skip list to cover all products. Products have knowledge of the last acq index only.
                    # Tricky! If we are at the last k-set, the last acq index of this k-set won't be a full-k
                    last_acq_index_index = (k_set + 1) * k - 1
                    if last_acq_index_index < len(frame_to_bursts[frame_id].sensing_datetime_days_index):
                        last_acq_index = frame_to_bursts[frame_id].sensing_datetime_days_index[last_acq_index_index]
                        skip_cslc_validation.add((frame_id, last_acq_index))
                        logging.info(f"Frame {frame_id} Acq Index {last_acq_index}, which is the last acq index in that k-set to cover the DISP-S1 products.")

    # Pickle out data (for unit test purposes) while removing any bursts that have COMPRESSED string in them
    ''' for disp_s1 in data:
        disp_s1['All Bursts'] = [b for b in disp_s1['All Bursts'] if 'COMPRESSED' not in b]
        disp_s1['All Bursts Count'] = len(disp_s1['All Bursts'])
    # pickle it out
    with open('data2.pkl', 'wb') as f:
        pickle.dump(data, f)
    '''

    passing = True

    # Account for produced DISP-S1 products by comparing to available CSLC bursts
    for disp_s1 in disp_s1s:
        matching_count = 0
        matching_bursts = []
        frame_id = disp_s1['Frame ID']
        all_bursts_set = set([b.split("/")[-1] for b in disp_s1['All Bursts']]) # Get rid of the full file path
        for acq_index in disp_s1['All Acq Day Indices']:
            if acq_index in frame_to_dayindex_to_granule[frame_id]:
                frame_data = frame_to_dayindex_to_granule[frame_id]
                acq_index_data  = frame_data[acq_index]
                intsect = all_bursts_set.intersection(acq_index_data)
                matching_count += len(intsect)
                matching_bursts.extend(list(intsect))
                all_bursts_set = all_bursts_set - intsect

                # Now remove all CSLC products that are being account for by this disp-s1 product so that we can determine any unprocessed CSLC bursts
                '''for burst in intsect:
                    acq_index_data.remove(burst)'''

        disp_s1['Matching Bursts'] = matching_bursts
        disp_s1['Matching Bursts Count'] = matching_count
        if matching_count != disp_s1['All Bursts Count'] and (frame_id, disp_s1['Last Acq Day Index']) not in skip_cslc_validation:
            passing = False
            logging.warning(f"Product {disp_s1['Product ID']} has {disp_s1['All Bursts Count']} bursts but only {matching_count} were found.")

        disp_s1['Unmatching Bursts'] = list(all_bursts_set)
        disp_s1['Unmatching Bursts Count'] = len(all_bursts_set)
        if len(all_bursts_set) > 0 and (frame_id, disp_s1['Last Acq Day Index']) not in skip_cslc_validation:
            passing = False
            logging.debug(f"Product {disp_s1['Product ID']} has {len(all_bursts_set)} unmatching bursts: {all_bursts_set}")

    # Supplement disp_s1 data structure with what should have also been triggered
    # If we are in historical mode, we need to remove any cslc acq indices that aren't up to k
    disp_frame_acq_day_indices = defaultdict(set)
    for disp_s1 in disp_s1s:
        for acq_index in disp_s1['All Acq Day Indices']:
            disp_frame_acq_day_indices[disp_s1['Frame ID']].add(acq_index)
    for item in data_should_trigger:
        acq_index = item['Acq Day Index']
        frame = item['Frame ID']
        if acq_index not in disp_frame_acq_day_indices[frame]:

            if (frame, acq_index) in skip_cslc_validation:
                logging.info(f"Frame {frame} Acq Index {acq_index} is not k-complete so will ignore during validation")
                continue

            passing = False
            matching_bursts = []
            unmatching_bursts = item['All Bursts']
            matching_bursts_count = len(matching_bursts)
            unmatching_bursts_count = len(unmatching_bursts)

            disp_s1s.append({
                'Product ID': "UNPROCESSED",
                'Frame ID': frame,
                'Last Acq Day Index': acq_index,
                'All Acq Day Indices': "N/A",
                'All Bursts': item['All Bursts'],
                'All Bursts Count': item['All Bursts Count'],
                'Matching Bursts': matching_bursts,
                'Matching Bursts Count': matching_bursts_count,
                'Unmatching Bursts': unmatching_bursts,
                'Unmatching Bursts Count': unmatching_bursts_count
            })

    # Print out all frame_to_dayindex_to_granule content
    '''for frame_id in frame_to_dayindex_to_granule:
        for day_index in frame_to_dayindex_to_granule[frame_id]:
            len_unprocessed = len(frame_to_dayindex_to_granule[frame_id][day_index])
            if len_unprocessed > 0:
                logging.debug(f"Frame {frame_id} Day Index {day_index} has {len_unprocessed} unprocessed bursts")
                passing = False'''

    return passing, disp_s1s, frame_to_dayindex_to_granule

def retrieve_disp_s1_from_cmr(smallest_date, greatest_date, output_endpoint, frames_to_validate):
    # Retrieve all DISP-S1 products from CMR within the acquisition time range as a list of granuleIDs
    all_disp_s1 = retrieve_r3_products(smallest_date, greatest_date, output_endpoint, _DISP_S1_PRODUCT_TYPE)
    filtered_disp_s1 = []
    for disp_s1 in all_disp_s1:

        # Getting to the frame_id is a bit of a pain
        for attrib in disp_s1.get("umm").get("AdditionalAttributes"):
            if attrib["Name"] == "FRAME_NUMBER" and int(
                    attrib["Values"][0]) in frames_to_validate:  # Should only ever belong to one frame

                # Need to perform secondary filter. Not sure if we always need to do this or temporarily so.
                actual_temporal_time = datetime.datetime.strptime(
                    disp_s1.get("umm").get("TemporalExtent")['RangeDateTime']['EndingDateTime'], "%Y-%m-%dT%H:%M:%SZ")
                if actual_temporal_time >= smallest_date and actual_temporal_time <= greatest_date:
                    filtered_disp_s1.append(disp_s1.get("umm").get("GranuleUR"))

    return filtered_disp_s1

def retrieve_disp_s1_from_grq(smallest_date, greatest_date, frames_to_validate):
    # Retrieve all DISP-S1 products from GRQ ES within the acquisition time range as a list of granuleIDs

    # There is currently no way to query by acquistion time so we will have to retrieve everything first and then filter
    # in code by acquisition time
    es_util = es_conn_util.get_es_connection(None)
    disp_s1s = es_util.query(
        index=_DISP_S1_INDEX_PATTERNS,
        body={"query": {"bool": {"must": [
            {"terms": {"metadata.frame_id": [str(f) for f in frames_to_validate]}}
        ]}}})

    filtered_disp_s1 = []
    for disp_s1 in disp_s1s:
        actual_temporal_time = datetime.datetime.strptime(
            disp_s1["_source"]["metadata"]["Files"][0]["sec_datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if actual_temporal_time >= smallest_date and actual_temporal_time <= greatest_date:
            filtered_disp_s1.append(disp_s1["_source"]["id"])
    return filtered_disp_s1

def validate_disp_s1(start_date, end_date, timestamp, input_endpoint, output_endpoint, disp_s1_frames_only,
                     disp_s1_validate_with_grq, processing_mode, k, shortname='OPERA_L2_CSLC-S1_V1'):
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

        :return: (passing, should_df, df)
            passing - Overall boolean value indicating if the validation passed or failed.
            should_df - DataFrame containing the expected granules that should have been processed.
            df - DataFrame containing the actual granules that were processed.

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
    frame_to_dayindex_to_granule = get_frame_to_dayindex_to_granule(granule_ids, frames_to_validate, burst_to_frames, frame_to_bursts, processing_mode)
    granules_should_trigger = filter_for_trigger_frame(frame_to_dayindex_to_granule, frame_to_bursts, burst_to_frames)
    data_should_trigger = []

    # Initialize smallest and greatest time to be something very large and very small
    smallest_date = datetime.datetime.strptime("2099-12-31T23:59:59.999999Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    greatest_date = datetime.datetime.strptime("1999-01-01T00:00:00.000000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

    logging.debug("Should have generated the following DISP-S1 products:")
    total_triggered = 0
    for frame_id in granules_should_trigger:
        for day_index in granules_should_trigger[frame_id]:
            total_triggered += 1
            logging.debug("Frame ID: %s, Day Index: %s, Num CSLCs: %d, CSLCs: %s", frame_id, day_index, len(granules_should_trigger[frame_id][day_index]), granules_should_trigger[frame_id][day_index])
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

    # Pickle out the data_should_trigger dictionary for later use
    '''with open('data_should_trigger.pkl', 'wb') as f:
        pickle.dump(data_should_trigger, f)'''

    logging.info(f"Total number of DISP-S1 products that should have been generated: {total_triggered}")
    logging.info(f"Earliest acquisition date: {smallest_date}, Latest acquisition date: {greatest_date}")

    if disp_s1_validate_with_grq:
        filtered_disp_s1 = retrieve_disp_s1_from_grq(smallest_date, greatest_date, frames_to_validate)
    else:
        filtered_disp_s1 = retrieve_disp_s1_from_cmr(smallest_date, greatest_date, output_endpoint, frames_to_validate)

    logging.info(f"Found {len(filtered_disp_s1)} DISP-S1 products:")
    logging.debug(filtered_disp_s1)

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
        # Get rid of the full file path and .h5 extension
        all_bursts = [s.split("/")[-1][:-3] \
                      for s in metadata["lineage"] if "CSLC" in s and not "STATIC" in s and not "COMPRESSED" in s]

        #from "f8889_a168_f8889_a156_f8889_a144 to [168, 156, 144]
        all_acq_day_indices =  [int(s.split("_")[0]) for s in metadata["input_granule_id"].split("_a")[1:]]

        # If the processing mode is not historical, use the latest acquisition day index to filter out all_bursts
        if processing_mode != "historical":
            latest_acq_day_index = max(all_acq_day_indices)
            all_acq_day_indices = [latest_acq_day_index]
            historical_all_bursts = copy.deepcopy(all_bursts)
            for g in historical_all_bursts:
                _, _, acquisition_cycles, _ = parse_cslc_native_id(g, burst_to_frames, frame_to_bursts)
                #print(g, acquisition_cycles, latest_acq_day_index)
                if latest_acq_day_index not in list(acquisition_cycles.values()):
                    #print("Removing", g)
                    all_bursts.remove(g)


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
    passing, data, frame_to_dayindex_to_granule = match_up_disp_s1(data_should_trigger, data, processing_mode, k, frame_to_bursts)
    should_df = pd.DataFrame(data_should_trigger)

    # Create a DataFrame from the data
    df = pd.DataFrame(data)
    df.sort_values(["Frame ID", "Last Acq Day Index", "Product ID"], inplace=True)
    return passing, should_df, df