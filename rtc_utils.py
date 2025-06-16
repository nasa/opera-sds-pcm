import re
from datetime import timedelta

from dateutil.parser import isoparse

_EPOCH_S1A = "20140101T000000Z"

rtc_granule_regex = (
    r'(?P<id>'
    r'(?P<project>OPERA)_'
    r'(?P<level>L2)_'
    r'(?P<product_type>RTC)-'
    r'(?P<source>S1)_'
    r'(?P<burst_id>\w{4}-\w{6}-\w{3})_'
    r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    r'(?P<creation_ts>(?P<cre_year>\d{4})(?P<cre_month>\d{2})(?P<cre_day>\d{2})T(?P<cre_hour>\d{2})(?P<cre_minute>\d{2})(?P<cre_second>\d{2})Z)_'
    r'(?P<sensor>S1A|S1B)_'
    r'(?P<spacing>30)_'
    r'(?P<product_version>v\d+[.]\d+)'
    r')'
)
"""
Example: "OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0
"""

rtc_product_file_regex = (
    rtc_granule_regex + ''
    r'(_'
    r'(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)|_BROWSE|_mask)?'
    r'[.]'
    r'(?P<ext>tif|tiff|h5|png|iso\.xml)$'
)
"""
Example: "OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0.h5"
"""

rtc_product_file_revision_regex = rtc_product_file_regex[:-1] + r'-(?P<revision>r\d+)$'

rtc_relative_orbit_number_regex = r"t(?P<relative_orbit_number>\d+)"


def determine_acquisition_cycle_for_rtc_product_file(rtc_product_filename=None, match_rtc_product_filename=None):
    if not match_rtc_product_filename:
        match_rtc_product_filename = re.match(rtc_product_file_regex, rtc_product_filename)
    granule_id = match_rtc_product_filename.group("id")

    return determine_acquisition_cycle_for_rtc_granule(granule_id)


def determine_acquisition_cycle_for_rtc_granule(granule_id=None, match_granule_id=None):
    if not match_granule_id:
        match_granule_id = re.match(rtc_granule_regex, granule_id)
    else:
        granule_id = match_granule_id.group("id")

    acquisition_dts = match_granule_id.group("acquisition_ts")  # e.g. 20210705T183117Z
    burst_id = match_granule_id.group("burst_id")  # e.g. T074-157286-IW3

    return determine_acquisition_cycle(burst_id, acquisition_dts, granule_id)


def determine_acquisition_cycle(burst_id, acquisition_dts, granule_id, epoch = None):
    """RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
    The cycle restarts periodically with some miniscule drift over time and the life of the mission."""
    # RTC: Calculating the Collection Cycle Index (Part 1):
    #  required constants
    cycle_days = 12

    if epoch is not None:
        instrument_epoch = isoparse(epoch)  # We use whatever was passed in
    else:
        MISSION_EPOCH_S1A = isoparse(_EPOCH_S1A)  # set approximate mission start date
        MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
        instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B

    MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
    ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=cycle_days).total_seconds()

    # RTC: Calculating the Collection Cycle Index (Part 2):
    #  RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
    #  The cycle restarts periodically with some miniscule drift over time and the life of the mission.
    burst_identification_number = int(burst_id.split(sep="-")[1])
    seconds_after_mission_epoch = (isoparse(acquisition_dts) - instrument_epoch).total_seconds()
    acquisition_index = (
                                seconds_after_mission_epoch - (ACQUISITION_CYCLE_DURATION_SECS * (
                                    burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
                        ) / ACQUISITION_CYCLE_DURATION_SECS


    acquisition_cycle = round(acquisition_index)
    assert acquisition_cycle >= 0, f"Acquisition cycle is negative: {acquisition_cycle=}"
    return acquisition_cycle
