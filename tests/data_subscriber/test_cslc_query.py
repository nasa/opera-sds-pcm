import random
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber import daac_data_subscriber, query

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--start-date=2021-01-24T23:00:00Z", "--end-date=2021-01-24T23:00:00Z", "--frame-range=1,100"]

@pytest.mark.skip
def test_frame_range():

    args = daac_data_subscriber.create_parser().parse_args(arguments)

    assert args.native_ids == "*iw1*"

def test_arg_expansion():

    disp_burst_map, metadata, version = query.process_disp_frame_burst_json(query.DISP_FRAME_BURST_MAP_JSON)

    args = daac_data_subscriber.create_parser().parse_args(arguments)
    query.expand_clsc_frames(args, disp_burst_map)
    assert args.native_ids == "*t001_000753_iw1*,*t001_000753_iw2*,*t001_000753_iw3*,*t001_000754_iw1*,*t001_000754_iw2*,*t001_000754_iw3*,*t001_000755_iw1*,*t001_000755_iw2*,*t001_000755_iw3*,*t001_000756_iw1*,*t001_000756_iw2*,*t001_000756_iw3*,*t001_000757_iw1*,*t001_000757_iw2*,*t001_000757_iw3*,*t001_000758_iw1*,*t001_000758_iw2*,*t001_000758_iw3*,*t001_000759_iw1*,*t001_000759_iw2*,*t001_000759_iw3*,*t001_000760_iw1*,*t001_000760_iw2*,*t001_000760_iw3*,*t001_000761_iw1*,*t001_000761_iw2*,*t001_000761_iw3*,*t001_000761_iw1*,*t001_000761_iw2*,*t001_000761_iw3*,*t001_000762_iw1*,*t001_000762_iw2*,*t001_000762_iw3*,*t001_000763_iw1*,*t001_000763_iw2*,*t001_000763_iw3*,*t001_000764_iw1*,*t001_000764_iw2*,*t001_000764_iw3*,*t001_000765_iw1*,*t001_000765_iw2*,*t001_000765_iw3*,*t001_000766_iw1*,*t001_000766_iw2*,*t001_000766_iw3*,*t001_000767_iw1*,*t001_000767_iw2*,*t001_000767_iw3*,*t001_000768_iw1*,*t001_000768_iw2*,*t001_000768_iw3*,*t001_000769_iw1*,*t001_000769_iw2*,*t001_000769_iw3*"