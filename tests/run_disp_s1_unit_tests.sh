#!/bin/bash
pytest -s data_subscriber/test_cslc_util.py && pytest -s data_subscriber/test_cslc_query.py &&  pytest -s data_subscriber/test_query.py && pytest -s tools/test_run_disp_s1_historical_processing.py
