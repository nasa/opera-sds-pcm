#!/usr/bin/env python3

import pytest
from product2dataset import product2dataset

def test_decorate_compressed_cslc():
    dataset_met_json = {
        "id": "OPERA_L2_CSLC-S1_T093-197858-IW3_20231118T013640Z_20231119T073552Z_S1A_VV_v1.0",
        "frame_id": 24733,
        "acquisition_cycle": 2748,
        "Files": [
            {
                "burst_id": "T093-197858-IW3",
                "last_date_time": "2023-11-18T00:00:00.000000Z"
            }
        ]
    }

    product2dataset.decorate_compressed_cslc(dataset_met_json)

    ccslc_file = dataset_met_json["Files"][0]

    assert dataset_met_json["burst_id"] == ccslc_file["burst_id"]
    assert dataset_met_json["acquisition_cycle"] == 2748
    assert dataset_met_json["ccslc_m_index"] == "t093_197858_iw3_2748"