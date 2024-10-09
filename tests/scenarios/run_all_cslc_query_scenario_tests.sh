#!/bin/bash
python cslc_query_test.py cslc_query_hist_k2_test.json --clear true
python cslc_query_test.py cslc_query_reproc_k4_test.json --clear true
python cslc_query_test.py cslc_query_reproc_blackout.json --clear true
python cslc_query_test.py cslc_query_fwd_k2_test.json --clear true