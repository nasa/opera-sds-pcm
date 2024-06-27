#!/usr/bin/env python3

import pytest
from data_subscriber import cslc_utils
from data_subscriber.parser import create_parser
import dateutil
from datetime import datetime
from util.conf_util import SettingsConf
from tools.run_disp_s1_historical_processing import generate_initial_frame_states

def test_generate_initial_frame_states():
    result = generate_initial_frame_states([[831, 833], 8882])
    print(result)
    assert result == [{831:0}, {832:0}, {833 :0}, {8882:0}]