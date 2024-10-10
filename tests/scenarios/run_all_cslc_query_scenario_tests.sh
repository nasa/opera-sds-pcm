#!/bin/bash

# Runs all known scenario tests for DISP-S1 triggering

python cslc_query_test.py cslc_query_hist_k2_test.json --clear true &&
python cslc_query_test.py cslc_query_reproc_k4_test.json --clear true &&
python cslc_query_test.py cslc_query_reproc_dates_frameid_k4_test.json --clear true &&
python cslc_query_test.py cslc_query_fwd_k2_test.json --clear true &&
python cslc_query_test.py cslc_query_reproc_blackout.json --clear true &&
python cslc_query_test.py cslc_query_reproc_blackout_empty.json --clear true