import random
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber import daac_data_subscriber, query, cslc_utils

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-24T23:00:00Z", "--frame-range=100,101"]

@pytest.mark.skip
def test_frame_range():

    args = daac_data_subscriber.create_parser().parse_args(arguments)

    assert args.native_id == "*iw1*"

def test_arg_expansion():
    '''Test that the native_id field is expanded correctly for a given frame range'''
    disp_burst_map, metadata, version = cslc_utils.process_disp_frame_burst_json(cslc_utils.DISP_FRAME_BURST_MAP_JSON)

    args = daac_data_subscriber.create_parser().parse_args(arguments)
    cslc_utils.expand_clsc_frames(args, disp_burst_map)
    print("----------------------------------")
    print(args.native_id)
    print("----------------------------------")
    assert args.native_id == "OPERA_L2_CSLC-S1_T001-000793-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000793-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000793-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000794-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000795-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000796-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000797-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000798-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000799-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000800-IW3*&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW1*&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW2*&native-id[]=OPERA_L2_CSLC-S1_T001-000801-IW3*"