#!/usr/bin/env python3

import pytest
import conftest
from pathlib import Path
from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_blackout import process_disp_blackout_dates, DispS1BlackoutDates
from data_subscriber.parser import create_parser
from data_subscriber.cslc import cslc_query
from datetime import datetime
from data_subscriber.cmr import DateTimeRange

forward_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward", "--start-date=2021-01-24T23:00:00Z",\
                     "--end-date=2021-01-25T00:00:00Z", "--grace-mins=60", "--k=4", "--m=4"]

BURST_MAP = Path(__file__).parent / "opera-disp-s1-consistent-burst-ids-2024-10-14-2016-07-01_to_2024-09-04.json"
frame_to_bursts, burst_to_frames, datetime_to_frames = cslc_utils.process_disp_frame_burst_hist(BURST_MAP)

def test_extend_additional_records():
    """Given a list of granules, test that we are extending additional granules for bursts that belong to two frames"""

    p = Path(__file__).parent / "empty_disp_s1_blackout.json"
    frame_blackout_dates = process_disp_blackout_dates(file = p)
    blackout_obj = DispS1BlackoutDates(frame_blackout_dates, frame_to_bursts, burst_to_frames)

    granules = []
    granules.append({"granule_id": "OPERA_L2_CSLC-S1_T042-088921-IW1_20160705T140755Z_20240425T204418Z_S1A_VV_v1.1"}) # frame 11115, 11116
    granules.append({"granule_id": "OPERA_L2_CSLC-S1_T042-088919-IW1_20160705T140750Z_20240425T204418Z_S1A_VV_v1.1"}) # frames 11115

    blackout_obj.extend_additional_records(granules, proc_mode = "forward")

    assert len(granules) == 3


def test_reprocessing_by_native_id(caplog):
    ''' Tests reprocessing query commands and high-level processing when specifying a native_id'''
    reprocessing_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=reprocessing", "--k=4", "--m=4",
                              "--native-id=OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0", "--no-schedule-download"]
    reproc_args = create_parser().parse_args(reprocessing_arguments)
    c_query = cslc_query.CslcCmrQuery(reproc_args, None, None, None, None,
                                      {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60},BURST_MAP)
    c_query.query_cmr(reproc_args, None, None, None, None, datetime.utcnow())
    assert ("native_id='OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0' is not found in the DISP-S1 Burst ID Database JSON. Nothing to process"
            in caplog.text)


def test_historical_query(caplog):
    ''' Tests historical query commands and high-level processing when specifying a frame range'''
    hist_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=historical", "--k=4", "--m=4",
                      "--start-date=2022-07-18T13:02:00Z", "--end-date=2022-07-19T13:01:53Z", "--frame-id=44044åå"]
    args = create_parser().parse_args(hist_arguments)
    query_timerange = DateTimeRange(args.start_date, args.end_date)
    c_query = cslc_query.CslcCmrQuery(args, None, None, None, None,
                                      {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60}, BURST_MAP)

    # TODO: figure out how to test the query_cmr method
    #await c_query.query_cmr(args, None, None, None, query_timerange, datetime.utcnow())å
    #assert ("native_id='OPERA_L2_CSLC-S1_T027-056778-IW1_20231008T133102Z_20231009T204457Z_S1A_VV_v1.0å' is not found in the DISP-S1 Burst ID Database JSON. Nothing to process"
    #        in caplog.text)

@pytest.mark.skip
def test_reprocessing_by_dates():
    ''' Tests reprocessing query commands and high-level processing'''
    reprocessing_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=reprocessing",
                              "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-25T00:00:00Z",
                              "--k=4", "--no-schedule-download"]
    reproc_args = create_parser().parse_args(reprocessing_arguments)
    query_timerange = DateTimeRange(reproc_args.start_date, reproc_args.end_date)
    c_query = cslc_query.CslcCmrQuery(reproc_args, None, None, None, None,
                                      {"DEFAULT_DISP_S1_QUERY_GRACE_PERIOD_MINUTES": 60},BURST_MAP)
    cr = c_query.query_cmr(reproc_args, None, None, None, query_timerange, datetime.utcnow())
    args = cr.cr_frame.f_locals["args"]
    assert args.collection == 'OPERA_L2_CSLC-S1_V1'
    assert args.start_date == '2021-01-24T23:00:00Z'
    assert args.end_date == '2021-01-25T00:00:00Z'
    assert args.proc_mode == 'reprocessing'