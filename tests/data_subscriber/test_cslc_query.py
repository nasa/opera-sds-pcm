import random
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber import daac_data_subscriber, query

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-24T23:00:00Z", "--frame-range=1,100"]

@pytest.mark.skip
def test_frame_range():

    args = daac_data_subscriber.create_parser().parse_args(arguments)

    assert args.native_id == "*iw1*"

def test_arg_expansion():

    disp_burst_map, metadata, version = query.process_disp_frame_burst_json(query.DISP_FRAME_BURST_MAP_JSON)

    args = daac_data_subscriber.create_parser().parse_args(arguments)
    query.expand_clsc_frames(args, disp_burst_map)
    #print(args.native_id)
    assert args.native_id == "*T001-000753-IW1*&native_id[]=*T001-000753-IW2*&native_id[]=*T001-000753-IW3*&native_id[]=*T001-000754-IW1*&native_id[]=*T001-000754-IW2*&native_id[]=*T001-000754-IW3*&native_id[]=*T001-000755-IW1*&native_id[]=*T001-000755-IW2*&native_id[]=*T001-000755-IW3*&native_id[]=*T001-000756-IW1*&native_id[]=*T001-000756-IW2*&native_id[]=*T001-000756-IW3*&native_id[]=*T001-000757-IW1*&native_id[]=*T001-000757-IW2*&native_id[]=*T001-000757-IW3*&native_id[]=*T001-000758-IW1*&native_id[]=*T001-000758-IW2*&native_id[]=*T001-000758-IW3*&native_id[]=*T001-000759-IW1*&native_id[]=*T001-000759-IW2*&native_id[]=*T001-000759-IW3*&native_id[]=*T001-000760-IW1*&native_id[]=*T001-000760-IW2*&native_id[]=*T001-000760-IW3*&native_id[]=*T001-000761-IW1*&native_id[]=*T001-000761-IW2*&native_id[]=*T001-000761-IW3*&native_id[]=*T001-000761-IW1*&native_id[]=*T001-000761-IW2*&native_id[]=*T001-000761-IW3*&native_id[]=*T001-000762-IW1*&native_id[]=*T001-000762-IW2*&native_id[]=*T001-000762-IW3*&native_id[]=*T001-000763-IW1*&native_id[]=*T001-000763-IW2*&native_id[]=*T001-000763-IW3*&native_id[]=*T001-000764-IW1*&native_id[]=*T001-000764-IW2*&native_id[]=*T001-000764-IW3*&native_id[]=*T001-000765-IW1*&native_id[]=*T001-000765-IW2*&native_id[]=*T001-000765-IW3*&native_id[]=*T001-000766-IW1*&native_id[]=*T001-000766-IW2*&native_id[]=*T001-000766-IW3*&native_id[]=*T001-000767-IW1*&native_id[]=*T001-000767-IW2*&native_id[]=*T001-000767-IW3*&native_id[]=*T001-000768-IW1*&native_id[]=*T001-000768-IW2*&native_id[]=*T001-000768-IW3*&native_id[]=*T001-000769-IW1*&native_id[]=*T001-000769-IW2*&native_id[]=*T001-000769-IW3*"