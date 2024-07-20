#!/usr/bin/env python3

import sys
from data_subscriber import cslc_utils
import tools.run_disp_s1_historical_processing
from tools.run_disp_s1_historical_processing import generate_initial_frame_states, form_job_params, convert_datetime

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()
from mock import MagicMock

disp_burst_map, burst_to_frames, day_indices_to_frames = cslc_utils.localize_disp_frame_burst_hist(cslc_utils.DISP_FRAME_BURST_MAP_HIST)

START_DATE = '2016-07-01T00:00:00Z'
END_DATE = '2024-07-01T00:00:00Z'
PROCESSING_MODE = "historical"
JOB_TYPE = "cslc_query"
INCLUDE_REGIONS = "north_america_opera"
EXCLUDE_REGIONS = "california"

tools.run_disp_s1_historical_processing.JOB_RELEASE="abc"
tools.run_disp_s1_historical_processing.ENDPOINT = "abc"

class P(object):
    pass

def generate_p():
    p = P()
    p.label = "historical1"
    p.processing_mode = PROCESSING_MODE
    p.job_type = JOB_TYPE
    p.download_job_queue = "some_queue"
    p.chunk_size = 1
    p.include_regions = INCLUDE_REGIONS
    p.exclude_regions = EXCLUDE_REGIONS
    p.data_start_date = START_DATE[:-1]
    p.data_end_date = END_DATE[:-1]
    p.frames =  [[831, 833], 8882]
    p.k = 4
    p.m = 2
    p.job_queue = "opera-job_worker-cslc_data_query"
    p.collection_short_name = "OPERA_L2_CSLC-S1_V1"
    p.frames_per_query = 100

    return p

def test_generate_initial_frame_states():
    result = generate_initial_frame_states([[831, 833], 8882])
    print(result)
    assert result == {831:0, 832:0, 833 :0, 8882:0}

def test_form_job_params_basic():
    '''Basic test for form_job_params in DISP-S1 historical processing'''

    p = generate_p()
    p.frame_states = generate_initial_frame_states(p.frames)
    do_submit, job_name, job_spec, job_params, job_tags, next_frame_sensing_position, finished = \
        form_job_params(p, 831, 0, None, None)

    assert do_submit == True
    assert job_name == "data-subscriber-query-timer-historical1_f831-2017-02-15T22:35:24-2017-03-23T23:35:24"
    assert JOB_TYPE in job_spec
    assert job_tags == ['data-subscriber-query-timer', 'historical_processing']
    assert job_params["start_datetime"] == f"--start-date=2017-02-15T22:35:24Z"
    assert job_params["end_datetime"] == f"--end-date=2017-03-23T23:35:24Z"
    assert job_params["processing_mode"] == f'--processing-mode={PROCESSING_MODE}'
    assert job_params["use_temporal"] == f'--use-temporal'
    assert job_params["include_regions"] == f'--include-regions={INCLUDE_REGIONS}'
    assert job_params["exclude_regions"] == f'--exclude-regions={EXCLUDE_REGIONS}'
    assert job_params["frame_id"] == f'--frame-id=831'
    assert job_params["k"] == f'--k=4'
    assert job_params["m"] == f'--m=1'

    assert next_frame_sensing_position == 4
    assert finished == False

def test_form_job_params_early():
    '''If the frame position sensing time is before data_start_date, don't process this round'''

    p = generate_p()
    p.frame_states = generate_initial_frame_states(p.frames)
    p.data_start_date = '2018-07-01T00:00:00'
    do_submit, job_name, job_spec, job_params, job_tags, next_frame_sensing_position, finished = \
        form_job_params(p, 831, 0, None, None)

    assert next_frame_sensing_position == 4
    assert do_submit == False
    assert finished == False

def test_form_job_params_late():
    '''If the frame position sensing time is after the data_end_date, don't process this round'''

    p = generate_p()
    p.frame_states = generate_initial_frame_states(p.frames)
    p.data_end_date = '2015-07-01T00:00:00'
    do_submit, job_name, job_spec, job_params, job_tags, next_frame_sensing_position, finished = \
        form_job_params(p, 831, 0, None, None)

    assert do_submit == False
    assert finished == True

def test_form_job_params_no_ccslc(monkeypatch):
    '''If compressed cslcs are not found, don't process this round and don't increment the position'''

    mock_ccslc = MagicMock(return_value=False)
    monkeypatch.setattr(cslc_utils,
                        cslc_utils.compressed_cslc_satisfied.__name__, mock_ccslc)

    p = generate_p()
    p.frame_states = generate_initial_frame_states(p.frames)
    p.data_end_date = '2015-07-01T00:00:00'
    do_submit, job_name, job_spec, job_params, job_tags, next_frame_sensing_position, finished = \
        form_job_params(p, 831, 0, None, None)

    assert do_submit == False
    assert next_frame_sensing_position == 0