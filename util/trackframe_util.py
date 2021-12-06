"""
NISAR PCM Track frame utilities
"""
import os

from collections import namedtuple
from datetime import datetime, timedelta

import pandas
import json

from util.common_util import to_datetime

DEFAULT_TRACK_FRAME_DB_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "TFdb",
    "NISAR_TFdb_2021_04_01.gpkg"
)

DEFAULT_BEAM_NAMES_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "nisar-mixed-modes",
    "modes.json"
)

_LOADED_TF_DATA = None
_LOADED_TF_FILENAME = None
_LOADED_BEAM_NAMES_DATA = None
_LOADED_BEAM_NAMES_FILENAME = None

CycleInfo = namedtuple(
    'CycleInfo', [
        'id',                     # Cycle ID
        'ctz_utc',                # Cycle Time Zero (CTZ) in UTC (datetime object or string)
        'time_corrections_table'  # Timing correction table
    ]
)


def load_track_frame_database(tfdb_filename: str):
    """
    Loads the track frame database if it is not already loaded
    """
    global _LOADED_TF_DATA
    global _LOADED_TF_FILENAME
    if _LOADED_TF_DATA is None \
            or _LOADED_TF_FILENAME is None \
            or _LOADED_TF_FILENAME != tfdb_filename:
        # TODO: geopandas affects SSL verification for ES connection, importing here is a workaround
        from geopandas import read_file

        _LOADED_TF_DATA = _LOADED_TF_FILENAME = None
        _LOADED_TF_DATA = read_file(tfdb_filename)
        _LOADED_TF_FILENAME = tfdb_filename
    return _LOADED_TF_DATA


def get_track_frames_for_one_cycle(
        cycle_info: CycleInfo,
        start_time_utc: datetime,
        end_time_utc: datetime,
        tfdb_filename: str = DEFAULT_TRACK_FRAME_DB_FILENAME):
    """
    Queries the track frame database for records that partially or fully overlap
    with the given time range. Returns all records that overlap.
    Returns:
        A GeoDataFrame object containing the track frame records from
        the Track Fame Database that partially or fully overlap with the given
        start and end time, with the following additional columns:
            cycle           Cycle ID
            start_time_utc  Start time in UTC of the track frame
            end_time_utc    End time in UTC of the track frame

    :param cycle_info: Data for the cycle
    :param start_time_utc: Start of the time range in UTC
    :param end_time_utc: End of the time range in UTC
    :param tfdb_filename: Track frame database filename
    :return: GeoDataFrame object
    """
    ctz_utc = to_datetime(cycle_info.ctz_utc)
    start_seconds_since_ctz = (start_time_utc - ctz_utc).total_seconds()
    end_seconds_since_ctz = (end_time_utc - ctz_utc).total_seconds()
    tfdb = load_track_frame_database(tfdb_filename)
    results = tfdb[(end_seconds_since_ctz >= tfdb.startCY)
                   & (tfdb.endCY >= start_seconds_since_ctz)]

    # add column with the cycle ID
    results = results.assign(cycle=cycle_info.id)

    # add columns with the start and end times (UTC) of each track frame
    tf_start_times = results.startCY.apply(
        lambda x: pandas.Timestamp(ctz_utc + timedelta(seconds=x)))
    tf_end_times = results.endCY.apply(
        lambda x: pandas.Timestamp(ctz_utc + timedelta(seconds=x)))
    results = results.assign(start_time_utc=tf_start_times)
    results = results.assign(end_time_utc=tf_end_times)
    return results


def get_track_frames(cycles: [CycleInfo],
                     start_time_utc,
                     end_time_utc,
                     tfdb_filename: str = DEFAULT_TRACK_FRAME_DB_FILENAME):
    """
    Queries the track frame database for records that partially or fully overlap
    with the given time range. Returns all track frame records that overlap.
    Similar to get_track_frames_for_one_cycle except that this function takes
    a list of cycles.

    Args:
        cycles              List of information about each cycle to check.
                            See the CycleInfo structure.
        start_time_utc:     Start of the time range in UTC (datetime object or string)
        end_time_utc:       End of the time range in UTC (datetime object or string)
        tfdb_filename:      Track frame database filename

    Returns:
        A GeoDataFrame object containing the track frame records from
        the Track Fame Database that partially or fully overlap with the given
        start and end time, with some added columns. See the
        get_track_frames_for_one_cycle() function for a description of the
        added columns.
    """
    # TODO: geopandas affects SSL verification for ES connection, importing in this scope is a workaround
    from geopandas.geodataframe import GeoDataFrame

    start_time_utc = to_datetime(start_time_utc)
    end_time_utc = to_datetime(end_time_utc)
    overall_results = GeoDataFrame()  # empty initially

    for cycle in cycles:
        overlapping_track_frames = \
            get_track_frames_for_one_cycle(cycle,
                                           start_time_utc,
                                           end_time_utc,
                                           tfdb_filename)

        if len(overlapping_track_frames) > 0:
            if len(overall_results) > 0:
                overall_results = overall_results.append(overlapping_track_frames, ignore_index=True)
            else:
                overall_results = overlapping_track_frames
    sort_list = []
    if "cycle" in overall_results.columns:
        sort_list.append("cycle")
    if "track" in overall_results.columns:
        sort_list.append("track")
    if "frame" in overall_results.columns:
        sort_list.append("frame")

    if len(sort_list) != 0:
        overall_results.sort_values(by=sort_list, ignore_index=True)

    if not validate_temporal_coverage(overall_results, start_time_utc, end_time_utc):
        raise ValueError("Error getting track frames for time range {} to {}: "
                         "the provided cycle data does not fully cover "
                         "the specified time range".format(start_time_utc, end_time_utc))

    return overall_results


def validate_temporal_coverage(sorted_track_frames,
                               start_time_utc: datetime,
                               end_time_utc: datetime) -> bool:
    """
    Checks that the specified track frame records completely cover the
    specified date/time range
    """
    end_of_coverage_utc = start_time_utc
    for _, track_frame in sorted_track_frames.iterrows():
        if track_frame.start_time_utc > end_of_coverage_utc:
            return False

        end_of_coverage_utc = max(end_of_coverage_utc, track_frame.end_time_utc)

    if end_time_utc > end_of_coverage_utc:
        return False

    return True


def load_beam_names(beam_names_filename):
    """
    Loads the radar modes to beam names dictionary if it is not already loaded
    """
    global _LOADED_BEAM_NAMES_DATA
    global _LOADED_BEAM_NAMES_FILENAME
    if _LOADED_BEAM_NAMES_DATA is None \
            or _LOADED_BEAM_NAMES_FILENAME is None \
            or _LOADED_BEAM_NAMES_FILENAME != beam_names_filename:
        _LOADED_BEAM_NAMES_DATA = _LOADED_BEAM_NAMES_FILENAME = None
        with open(beam_names_filename, "r") as f:
            _LOADED_BEAM_NAMES_DATA = json.load(f)
        _LOADED_BEAM_NAMES_FILENAME = beam_names_filename
    return _LOADED_BEAM_NAMES_DATA


def get_beam_mode_name(radar_mode, beam_names_filename=DEFAULT_BEAM_NAMES_FILENAME):
    """
    Returns the beam mode name of the given radar mode

    :param radar_mode:
    :param beam_names_filename:
    :return:
    """
    beam_names_data = load_beam_names(beam_names_filename)
    beam_name = beam_names_data.get(str(radar_mode), None)
    return beam_name
