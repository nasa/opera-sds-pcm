#!/usr/bin/env python
"""
Trackframe Evaluator job
"""
import os
import json
from datetime import datetime

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext
from util.common_util import get_latest_product_sort_list
from util.common_util import convert_datetime
from util.common_util import create_expiration_time
from util.common_util import create_state_config_dataset
from util.common_util import backoff_wrapper
from util.common_util import create_info_message_files
from util.conf_util import SettingsConf
from util.stuf_util import get_stuf_info_from_xml

from commons.es_connection import get_grq_es
from commons.logger import logger
from commons.constants import product_metadata as pm
from commons.constants import constants
from commons.constants import short_info_msg as short_msg

from cop import cop_catalog

from collections import OrderedDict

from shapely.geometry import mapping

from Track_Frame_Accountability.catalog import ES_INDEX as TRACK_FRAME_ACCOUNTABILITY_INDEX
from util.trackframe_util import get_track_frames, CycleInfo, get_beam_mode_name

from l0b2l1 import l0blist2l1

ancillary_es = get_grq_es(logger)  # getting GRQ's es connection


ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


def create_accountability_entries(state_config, state_config_name, track_frame_id, l0b_rrsds):
    l0b_rrsd_ids = list(
        map(lambda rrsd: rrsd if "." not in rrsd else rrsd.split(".")[0], l0b_rrsds))
    track_frame_record = {
        "created_at": convert_datetime(datetime.utcnow()),
        "last_modified": convert_datetime(datetime.utcnow()),
        "begin_time": state_config[pm.TRACK_FRAME_BEGIN_TIME],
        "end_time": state_config[pm.TRACK_FRAME_END_TIME],
        "processing_begin_time": state_config[pm.PROCESSING_START_TIME],
        "processing_end_time": state_config[pm.PROCESSING_END_TIME],
        "frame_coverage": state_config[pm.FRAME_COVERAGE],
        "data_source": state_config[pm.DATA_SOURCE],
        "beam_name": state_config[pm.BEAM_NAME],
        "track_frame-state-config_id": state_config_name,
        "track_frame_id": track_frame_id,
        "track_frame_is_complete": state_config[pm.IS_COMPLETE],
        "track_frame_is_urgent": state_config[pm.IS_URGENT],
        "track_frame_force_submit": state_config[pm.FORCE_SUBMIT],
        "L0B_L_RRSD_ids": l0b_rrsd_ids,
        "refrec_id": "",
    }
    try:
        _id = ""
        ancillary_es.index_document(
            index=TRACK_FRAME_ACCOUNTABILITY_INDEX, id=_id, body=track_frame_record)
    except Exception as e:
        logger.info("Failed to create new track_frame entry")
        logger.error(str(e))


def find_state_config(track_frame_id):
    state_config_id = "{}_state-config".format(track_frame_id)
    result = dict()

    existing_document = ancillary_es.get_by_id(
        id=state_config_id,
        index="grq_*_{}".format(pm.TRACK_FRAME_STATE_CONFIG.lower()),
        ignore=[404],
    )

    if existing_document.get("found", False):
        result = existing_document.get("_source", {}).get("metadata", {})

    return result


def create_state_config(track_frame_id, track_frame_info, found_rrsds, is_complete, track_frame_start_time,
                        track_frame_end_time, geojson, processing_start_time, processing_end_time,
                        beam_name, frame_coverage="full", data_source="mixed", submitted_by_timer=None):
    state_config = dict()
    state_config[pm.FOUND_L0B_RRSDS] = list()
    state_config[pm.L0B_RRSD_PRODUCT_PATHS] = list()
    state_config[pm.TRACK_FRAME_BEGIN_TIME] = track_frame_start_time
    state_config[pm.TRACK_FRAME_END_TIME] = track_frame_end_time
    state_config[pm.PROCESSING_START_TIME] = processing_start_time
    state_config[pm.PROCESSING_END_TIME] = processing_end_time
    state_config[pm.IS_COMPLETE] = is_complete
    state_config[pm.IS_URGENT] = False
    state_config[pm.FORCE_SUBMIT] = False
    state_config[pm.SUBMITTED_BY_TIMER] = submitted_by_timer
    state_config[pm.FRAME_COVERAGE] = frame_coverage
    state_config[pm.DATA_SOURCE] = data_source
    state_config[pm.BEAM_NAME] = beam_name

    for rrsd, product_path in found_rrsds.items():
        state_config[pm.FOUND_L0B_RRSDS].append(rrsd)
        state_config[pm.L0B_RRSD_PRODUCT_PATHS].append(product_path)

    state_config.update(track_frame_info)

    # TODO: Need to set the default latency for the track frame evaluator timers
    settings = SettingsConf().cfg
    latency = settings.get(constants.NOMINAL_LATENCY, {}).get(
        constants.TRACK_FRAME_EVALUATOR, 720)

    logger.info("Compiled State Config: {}".format(
        json.dumps(state_config, indent=2)))
    logger.info("State Config Name: {}_state-config".format(track_frame_id))

    dataset_name = "{}_state-config".format(track_frame_id)

    create_state_config_dataset(dataset_name=dataset_name,
                                metadata=state_config,
                                start_time=processing_start_time,
                                end_time=processing_end_time,
                                geojson=geojson,
                                expiration_time=create_expiration_time(int(latency)))

    create_accountability_entries(
        state_config, dataset_name, track_frame_id, found_rrsds)


def get_l0b_records(begin_date_time, end_date_time, conditions=None):
    # Need to use this function as that would return the latest version of each product found
    l0b_records = ancillary_es.perform_aggregate_range_intersection_query(
        beginning_date_time=begin_date_time,
        ending_date_time=end_date_time,
        met_field_beginning_date_time="starttime",
        met_field_ending_date_time="endtime",
        aggregate_field="metadata.{}.keyword".format(pm.PCM_RETRIEVAL_ID),
        conditions=conditions,
        size=100,
        sort_list=get_latest_product_sort_list(),
        index="grq_*_{}".format(pm.L0B_L_RRSD.lower()))
    return l0b_records


def is_track_frame_complete(track_frame_id):
    is_complete = False
    existing_document = backoff_wrapper(ancillary_es.get_by_id,
                                        id="{}_state-config".format(track_frame_id),
                                        index="grq_1_track_frame-state-config",
                                        ignore=[404])
    if existing_document.get("found", False):
        if existing_document.get("_source", {}).get("metadata", {}).get(pm.IS_COMPLETE) is True:
            is_complete = True

    return is_complete


def get_beam_mode_names(l0b_records, track_frame_start_time, track_frame_end_time):
    """
    Gets the beam mode names associated with the given track frame time range.

    :param track_frame_start_time:
    :param track_frame_end_time:
    :return:
    """
    beam_modes = dict()
    observations = ancillary_es.perform_es_range_intersection_query(
        track_frame_start_time,
        track_frame_end_time,
        cop_catalog.CMD_LSAR_START_DATETIME_ISO,
        cop_catalog.CMD_LSAR_END_DATETIME_ISO,
        size=100,
        sort=["{}:asc".format(cop_catalog.CMD_LSAR_START_DATETIME_ISO)],
        index=cop_catalog.ES_INDEX)
    if len(observations.get("hits", {}).get("hits", [])) == 0:
        raise Exception("No observations in the COP that intersect {} to {}".format(track_frame_start_time,
                                                                                    track_frame_end_time))
    for observation in observations.get("hits", {}).get("hits", []):
        obs_source = observation.get("_source", {})
        obs_id = obs_source.get(cop_catalog.REFOBS_ID)
        # This is the radar mode
        lsar_config_id = obs_source.get(cop_catalog.LSAR_CONFIG_ID)
        beam_mode_names = get_beam_mode_name(lsar_config_id)
        if beam_mode_names is None:
            raise Exception("Cannot find an equivalent beam mode name for "
                            "observation {} with lsar_config_id {}".format(obs_id, lsar_config_id))
        else:
            logger.info("{} has beam mode name {}".format(obs_id, beam_mode_names))
            # Find the L0B that's associated with this observation and store it
            matching_l0b_rec = None
            for l0b_record in l0b_records:
                l0b_met = l0b_record.get("_source", {}).get("metadata", {})
                if l0b_met.get(pm.OBS_ID) == obs_id:
                    matching_l0b_rec = l0b_record
                    logger.info("Found L0B record that is associated with observation {}: {}".format(
                        obs_id, l0b_met.get(pm.FILE_NAME)))
                    break
            beam_modes[obs_id] = {
                "beam_names": beam_mode_names,
                "l0b_record": matching_l0b_rec
            }

    return beam_modes


def get_processing_start_and_stop_times(track_frame_start_time, track_frame_end_time, l0b_start_time, l0b_end_time):
    """
    Gets the appropriate processing start and stop times, for use in the State Configs, in order to properly process
    full and partial frames.

    :param track_frame_start_time:
    :param track_frame_end_time:
    :param l0b_start_time:
    :param l0b_end_time:
    :return:
    """

    # In all cases, if the L0B start time is less than the track frame start time, use the track frame start time
    # else use the L0B start time
    #
    # If the L0B start time is greater than the track frame end time, don't use it
    #
    # if L0B end time is less than the track frame end time, use the L0B end time.
    # Otherwise, use the track frame end time.
    # If the end time of the L0B is less than the start time of the track frame start time, don't use it.

    processing_start_time = None
    processing_end_time = None

    if l0b_start_time <= track_frame_start_time:
        processing_start_time = track_frame_start_time
    elif l0b_start_time < track_frame_end_time:
        processing_start_time = l0b_start_time

    if l0b_end_time <= track_frame_end_time:
        processing_end_time = l0b_end_time
    elif l0b_end_time > track_frame_start_time:
        processing_end_time = track_frame_end_time

    return processing_start_time, processing_end_time


def create_state_config_id(cycle, track, frame, frame_coverage, data_sources, beam_name, index_counter=0):
    # This ID now includes an index at the end that may need to be updated
    # accordingly if we have to account for producing partial individual frames with
    # the same beam mode within the same track frame.
    return "track_frame_{:03d}_{:03d}_{:03d}_{}_{}_{}_{}".format(cycle, track, frame,
                                                                 frame_coverage, data_sources,
                                                                 beam_name, index_counter)


def get_track_frame_info(tf_db_record):
    """
    Processes a track frame database record and converts it into a dictionary for use with downstream processing

    :param tf_db_record: The Track Frame database record
    :return: A dictionary of track frame information and a geojson object
    """
    track_frame_info = {
        pm.CYCLE_NUMBER: tf_db_record.cycle,
        pm.RELATIVE_ORBIT_NUMBER: tf_db_record.track,
        pm.TRACK_FRAME: tf_db_record.frame
    }
    # Get the GeoJson location. Example of how it is supposed to look like in the datasets.json:
    # "location": {
    #    "type": "polygon",
    #    "coordinates": [
    #        [
    #            [-122.9059682940358,40.47090915967475],
    #            [-121.6679748715316,37.84406528996276],
    #            [-120.7310161872557,38.28728069813177],
    #            [-121.7043611684245,39.94137004454238],
    #            [-121.9536916840953,40.67097860759095],
    #            [-122.3100379696548,40.7267890636145],
    #            [-122.7640648263371,40.5457010812299],
    #            [-122.9059682940358,40.47090915967475]
    #        ]
    #    ]
    # }
    geojson = None
    geometry_info = tf_db_record.get("geometry", None)
    if geometry_info:
        geojson = mapping(geometry_info)
        track_frame_info[pm.TRACK_FRAME_POLYGON] = geojson
        track_frame_dict = tf_db_record.to_dict()
        # Remove this as we're already cataloging it
        track_frame_dict.pop("geometry")
        # Convert known datetime objects to string representations
        track_frame_dict["start_time_utc"] = convert_datetime(track_frame_dict["start_time_utc"])
        track_frame_dict["end_time_utc"] = convert_datetime(track_frame_dict["end_time_utc"])
        track_frame_info[pm.TRACK_FRAME_DB_INFO] = track_frame_dict
    else:
        logger.warning("Missing Geometry Information from Track Frame Record:\n{}".format(tf_db_record))

    return track_frame_info, geojson


@exec_wrapper
def evaluate():
    jc = JobContext("_context.json")
    job_context = jc.ctx
    logger.debug("job_context: {}".format(json.dumps(job_context, indent=2)))

    product_metadata = job_context.get("product_metadata")
    metadata = product_metadata.get("metadata")

    input_begin_date_time = metadata.get(pm.RANGE_START_DATE_TIME, None)
    input_end_date_time = metadata.get(pm.RANGE_STOP_DATE_TIME, None)

    if input_begin_date_time is None or input_end_date_time is None:
        raise RuntimeError("Missing {} and/or {} in the job context".format(pm.RANGE_START_DATE_TIME,
                                                                            pm.RANGE_STOP_DATE_TIME))

    # Call Albert's changes to get the orbit information to feed into the track frame utility function
    (
        orbit_num,
        cycle_num,
        relative_orbit_num,
        orbit_start_time,
        orbit_end_time,
        ctz,
        orbit_dir,
        eq_cross_time,
    ) = get_stuf_info_from_xml(input_begin_date_time, input_end_date_time)
    cycle_info = CycleInfo(
        id=cycle_num,
        ctz_utc=ctz,
        time_corrections_table=None
    )
    # Get track frames via the get_track_frames function
    track_frame_records = get_track_frames(
        [cycle_info], input_begin_date_time, input_end_date_time)

    already_completed_state_configs = list()
    missing_track_frames = list()
    detected_cal_frames = list()
    missing_l0bs_for_track_frame = list()
    for index, row in track_frame_records.iterrows():
        if row.cycle is None or row.track is None or row.frame is None:
            logger.warning("Missing cycle, track, or frame in the returned track frame record: "
                           "cycle={}, track={}, frame={}, row={}".format(row.cycle, row.track, row.frame, row))
            logger.info("Will not create state config.")
            missing_track_frames.append("cycle={}, track={}, frame={}, row={}".format(row.cycle,
                                                                                      row.track,
                                                                                      row.frame,
                                                                                      row))
            continue
        logger.info("Processing Cycle={}, Track={}, Frame={}".format(
            row.cycle, row.track, row.frame))
        track_frame_start_time = convert_datetime(row.start_time_utc)
        track_frame_end_time = convert_datetime(row.end_time_utc)
        logger.info("Finding L0B_L_RRSD files between {} and {}".format(
            track_frame_start_time, track_frame_end_time))
        l0b_records = get_l0b_records(
            track_frame_start_time, track_frame_end_time)
        if len(l0b_records) == 0:
            # Don't think this could ever happen, but it's here just in case
            no_l0bs_msg = "No L0B_L_RRSD files found between {} and {}".format(track_frame_start_time,
                                                                               track_frame_end_time)
            logger.warning(no_l0bs_msg)
            missing_l0bs_for_track_frame.append(no_l0bs_msg)
            continue
        l0b_records.sort(key=lambda x: x.get("_source").get("metadata").get(pm.RANGE_START_DATE_TIME))
        best_fit_records = ancillary_es.select_best_fit(track_frame_start_time, track_frame_end_time, l0b_records)
        is_complete = False
        if len(best_fit_records) == 0:
            ids = list()
            for record in l0b_records:
                ids.append(record.get("_source", {}).get("id"))
            logger.info("L0B_L_RRSD records found, but they do not completely cover the "
                        "data date time range {} to {}: {}".format(track_frame_start_time,
                                                                   track_frame_end_time,
                                                                   ids))
        else:
            ids = list()
            for record in best_fit_records:
                ids.append(record.get("_source", {}).get("id"))
            logger.info("best fit records: {}".format(ids))
            l0b_records = best_fit_records
            is_complete = True

        # Find all beam modes that intersect the track frame time range.
        l0b_beams = get_beam_mode_names(l0b_records, track_frame_start_time, track_frame_end_time)

        # Feed the list of beam mode names to the l0b2l1list function to get back a list of partials and a common
        # full frame
        beam_names_list = list()
        cal_frames = list()
        individual_frames = list()
        full_frame = None
        for obs_id, beam_name_info in l0b_beams.items():
            science_beams = list()
            for beam_name in beam_name_info.get("beam_names"):
                # Need to separate science beams from calibration beams
                if beam_name == "cal":
                    cal_frames.append((obs_id, beam_name))
                else:
                    science_beams.append(beam_name)
            beam_names_list.extend(science_beams)

        is_individual = False
        if len(beam_names_list) == 1:
            if len(cal_frames) == 0:
                full_frame = beam_names_list[0]
                # If you only ever get back 1 beam mode, we consider it to be a full
                # frame using an individual data source vs a mixed source since we've
                # checked the track frame time range against the COP and found all
                # possible beam modes that intersect it.
                is_individual = True
            else:
                # If we've detected calibration frames in the track frame, we will
                # consider the beam name to be a partial frame
                individual_frames.append((beam_names_list[0], beam_names_list[0]))
        else:
            individual_frames, full_frame = l0blist2l1(beam_names_list)

        if len(cal_frames) != 0:
            # TODO: Need to determine if we need to process calibration frames at the RSLC level.
            # For now, we will just ignore these and log it.
            logger.warning("Detected calibration frames. Ignoring for now.: {}".format(cal_frames))
            detected_cal_frames.extend(cal_frames)
            # To ignore calibration frames, we'll need to remove them from our dictionary as well that keeps
            # track of observations to its beam mode name and associated L0B ES record.
            for cal_frame in cal_frames:
                logger.info("Removing {} from l0b_beams dictionary since it is associated with a "
                            "calibration frame.".format(cal_frame[0]))
                l0b_beams.pop(cal_frame[0])

        if full_frame:
            logger.info("Full Frame Beam: {}. is_individual={}".format(full_frame, is_individual))
            frame_coverage = "full"
            data_source = "mixed"
            if is_individual is True:
                data_source = "individual"
            track_frame_id = create_state_config_id(row.cycle, row.track, row.frame,
                                                    frame_coverage, data_source, full_frame)
            found_rrsds = OrderedDict()
            for dr in ancillary_es.get_datastore_refs_from_es_records(l0b_records):
                found_rrsds[os.path.basename(dr)] = os.path.dirname(dr)
            is_already_complete = is_track_frame_complete(track_frame_id)
            if is_already_complete:
                logger.info("Will not create a state config for {} as it is already declared as complete in "
                            "ES".format(track_frame_id))
                already_completed_state_configs.append(track_frame_id)
            else:
                existing_state_config = find_state_config(track_frame_id)
                submitted_by_timer = existing_state_config.get(pm.SUBMITTED_BY_TIMER, None)
                track_frame_info, geojson = get_track_frame_info(row)

                # Determine the processing start and end time by taking the RangeStartDateTime
                # of the earliest L0B and the RangeStopDateTime of the latest L0B
                earliest_l0b_met = l0b_records[0].get("_source", {}).get("metadata", {})
                latest_l0b_met = l0b_records[-1].get("_source", {}).get("metadata", {})
                processing_start_time, processing_end_time = get_processing_start_and_stop_times(
                    track_frame_start_time,
                    track_frame_end_time,
                    earliest_l0b_met.get(pm.RANGE_START_DATE_TIME),
                    latest_l0b_met.get(pm.RANGE_STOP_DATE_TIME))
                if processing_start_time is None or processing_end_time is None:
                    raise RuntimeError(
                        "Could not determine proper start and/or end times given the following information: "
                        "track_frame_start_time={}, track_frame_end_time={}, "
                        "l0b_start_time={}, l0b_end_time={} -- "
                        "calculated_start_time={}, calculated_end_time={}".format(
                            track_frame_start_time,
                            track_frame_end_time,
                            earliest_l0b_met.get(pm.RANGE_START_DATE_TIME),
                            latest_l0b_met.get(pm.RANGE_STOP_DATE_TIME),
                            processing_start_time,
                            processing_end_time)
                    )

                create_state_config(track_frame_id, track_frame_info, found_rrsds, is_complete,
                                    track_frame_start_time, track_frame_end_time, geojson,
                                    processing_start_time, processing_end_time, full_frame,
                                    frame_coverage=frame_coverage,
                                    data_source=data_source,
                                    submitted_by_timer=submitted_by_timer)

        observation_ids = list(l0b_beams.keys())
        index_counters = dict()
        for index in range(0, len(individual_frames)):
            logger.info("Processing individual frame: {}".format(individual_frames[index]))
            frame_coverage = "partial"
            l0b_beam_id = individual_frames[index][0]
            individual_frame_id = individual_frames[index][1]
            if individual_frame_id:
                # Find the L0B associated with the l0b_beam_id
                obs_id = observation_ids[index]
                beam_name_info = l0b_beams.get(obs_id, None)
                if beam_name_info is None:
                    raise RuntimeError("Could not find beam name information for observation {}: {}".format(
                        obs_id, json.dumps(l0b_beams, indent=2)))
                else:
                    if l0b_beam_id in beam_name_info.get("beam_names"):
                        l0b_record = beam_name_info.get("l0b_record")
                    else:
                        raise RuntimeError("Beam name {} is not a part of the beam_names list: {}".format(
                            l0b_beam_id, beam_name_info.get("beam_names")))
                if l0b_record is None:
                    logger.info("Cannot find any L0B record at this point that is associated "
                                "with observation {} and individual beam name {}. "
                                "Will not create state config.".format(obs_id, l0b_beam_id))
                    continue
                l0b_met = l0b_record.get("_source", {}).get("metadata", {})
                # Determine the proper start and stop times
                processing_start_time, processing_end_time = get_processing_start_and_stop_times(
                    track_frame_start_time,
                    track_frame_end_time,
                    l0b_met.get(pm.RANGE_START_DATE_TIME),
                    l0b_met.get(pm.RANGE_STOP_DATE_TIME))
                if processing_start_time is None or processing_end_time is None:
                    raise RuntimeError(
                        "Could not determine proper start and/or end times given the following information: "
                        "track_frame_start_time={}, track_frame_end_time={}, "
                        "l0b_start_time={}, l0b_end_time={} -- "
                        "calculated_start_time={}, calculated_end_time={}".format(
                            track_frame_start_time,
                            track_frame_end_time,
                            l0b_met.get(pm.RANGE_START_DATE_TIME),
                            l0b_met.get(pm.RANGE_STOP_DATE_TIME),
                            processing_start_time,
                            processing_end_time)
                    )
                if individual_frame_id in index_counters:
                    counter = index_counters[individual_frame_id]
                    index_counters[individual_frame_id] = counter + 1
                else:
                    index_counters[individual_frame_id] = 0
                track_frame_id = create_state_config_id(row.cycle, row.track, row.frame,
                                                        frame_coverage, "individual",
                                                        individual_frame_id,
                                                        index_counters[individual_frame_id])
                found_rrsds = OrderedDict()
                for dr in ancillary_es.get_datastore_ref_from_es_record(l0b_record):
                    found_rrsds[os.path.basename(dr)] = os.path.dirname(dr)
                is_already_complete = is_track_frame_complete(track_frame_id)
                if is_already_complete:
                    logger.info("Will not create a state config for {} as it is already declared as complete in "
                                "ES".format(track_frame_id))
                    already_completed_state_configs.append(track_frame_id)
                else:
                    existing_state_config = find_state_config(track_frame_id)
                    submitted_by_timer = existing_state_config.get(pm.SUBMITTED_BY_TIMER, None)
                    track_frame_info, geojson = get_track_frame_info(row)
                    create_state_config(track_frame_id, track_frame_info, found_rrsds, True,
                                        track_frame_start_time, track_frame_end_time, geojson,
                                        processing_start_time, processing_end_time, individual_frame_id,
                                        frame_coverage=frame_coverage,
                                        data_source="individual",
                                        submitted_by_timer=submitted_by_timer)
            else:
                logger.info("No individual frame found for {}".format(l0b_beam_id))

    # Aggregate the info messages
    msgs = list()
    msg_details = ""
    if len(already_completed_state_configs):
        msgs.append(short_msg.STATE_CONFIGS_ALREADY_COMPLETE)
        msg_details += "\n\nWill not create a state config for the following as they are already declared " \
                       "as complete in ES:\n\n"
        for sc in already_completed_state_configs:
            msg_details += "{}\n".format(sc)

    if len(missing_track_frames):
        msgs.append(short_msg.MISSING_TRACK_FRAME_INFO_IN_DB)
        msg_details += "\n\nMissing cycle, track, or frame in the following returned track frame records:\n\n"
        for mtf in missing_track_frames:
            msg_details += "{}\n".format(mtf)

    if len(detected_cal_frames):
        msgs.append(short_msg.DETECTED_CALIBRATION_FRAMES)
        msg_details += "\n\nDetected calibration frames. Ignoring for now:\n\n"
        for cf in detected_cal_frames:
            msg_details += "{}\n".format(cf)

    if len(missing_l0bs_for_track_frame):
        msgs.append(short_msg.MISSING_L0B_FOR_TRACK_FRAME)
        msg_details += "\n\nMissing L0B_L_RRSD files for these track frame time ranges:\n\n"
        for l0b_msg in missing_l0bs_for_track_frame:
            msg_details += "{}\n".format(l0b_msg)

    if len(msgs) != 0:
        create_info_message_files(msg=msgs, msg_details=msg_details)


if __name__ == "__main__":
    evaluate()
