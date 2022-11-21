from pathlib import Path

import os
import glob
import pytest
import yaml

from os.path import abspath, dirname, join
from util import pge_util

TEST_DIR = dirname(abspath(__file__))
REPO_DIR = abspath(join(TEST_DIR, os.pardir, os.pardir))


def test_simulate_cslc_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L2_CSLC_S1A_IW_*_VV_*Z_v0.1_*Z.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_CSLC_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_output_base_name = "OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220501T015035Z_v0.1_20220501T015102Z"

    try:
        assert Path(f'/tmp/{expected_output_base_name}.tiff').exists()
        assert Path(f'/tmp/{expected_output_base_name}.json').exists()
        assert Path(f'/tmp/{expected_output_base_name}.catalog.json').exists()
        assert Path(f'/tmp/{expected_output_base_name}.iso.xml').exists()
        assert Path(f'/tmp/{expected_output_base_name}.log').exists()
        assert Path(f'/tmp/{expected_output_base_name}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_CSLC_S1A_IW_*_VV_*Z_v0.1_*Z.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_rtc_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L2_RTC_S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_RTC_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_output_basename = 'OPERA_L2_RTC_S1_{burst_id}_20180504T104507Z_20180504T104535Z_S1B_30_v0.1'

    for burst_id in pge_util.RTC_BURST_IDS:
        assert Path(f'/tmp/{expected_output_basename.format(burst_id=burst_id)}.nc').exists()

    expected_ancillary_basename = 'OPERA_L2_RTC_S1_20180504T104535Z_S1B_30_v0.1'

    try:
        assert Path(f'/tmp/{expected_ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.iso.xml').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.log').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_RTC_S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_dswx_hls_pge_with_l30():
    expected_output_base_name = "OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0"

    # before
    for path in glob.iglob(f'/tmp/{expected_output_base_name}*.tiff'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file)

        # ACT
        pge_util.simulate_run_pge(
            pge_config['runconfig'],
            pge_config,
            context={
                "job_specification": {
                    "params": [
                        {
                            "name": "input_dataset_id",
                            "value": "HLS.L30.T22VEQ.2021248T143156.v2.0"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )

    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_BAND_NAMES, start=1):
        assert Path(f'/tmp/{expected_output_base_name}_B{band_idx:02}_{band_name}.tiff').exists()

    assert Path(f'/tmp/{expected_output_base_name}.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.png').exists()
    assert Path(f'/tmp/{expected_output_base_name}.qa.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.catalog.json').exists()
    assert Path(f'/tmp/{expected_output_base_name}.iso.xml').exists()

    # after
    for path in glob.iglob(f'/tmp/{expected_output_base_name}*.tiff'):
        Path(path).unlink(missing_ok=True)

    Path(f'/tmp/{expected_output_base_name}.log').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.png').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.qa.log').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.catalog.json').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.iso.xml').unlink(missing_ok=False)


def test_simulate_dswx_hls_pge_with_s30():
    expected_output_base_name = "OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0"

    # before
    for path in glob.iglob(f'/tmp/{expected_output_base_name}*.tiff'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file)

        # ACT
        pge_util.simulate_run_pge(
            pge_config['runconfig'],
            pge_config,
            context={
                "job_specification": {
                    "params": [
                        {
                            "name": "input_dataset_id",
                            "value": "HLS.S30.T15SXR.2021250T163901.v2.0"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )

    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_BAND_NAMES, start=1):
        assert Path(f'/tmp/{expected_output_base_name}_B{band_idx:02}_{band_name}.tiff').exists()

    assert Path(f'/tmp/{expected_output_base_name}.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.png').exists()
    assert Path(f'/tmp/{expected_output_base_name}.qa.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.catalog.json').exists()
    assert Path(f'/tmp/{expected_output_base_name}.iso.xml').exists()

    # after
    for path in glob.iglob(f'/tmp/{expected_output_base_name}*.tiff'):
        Path(path).unlink(missing_ok=False)

    Path(f'/tmp/{expected_output_base_name}.log').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.png').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.qa.log').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.catalog.json').unlink(missing_ok=False)
    Path(f'/tmp/{expected_output_base_name}.iso.xml').unlink(missing_ok=False)


def test_simulate_dswx_hls_pge_with_unsupported():
    # before_each
    pge_config_file_path = Path('opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with pytest.raises(Exception) as exc_info:
        with open(pge_config_file_path) as pge_config_file:
            pge_config = yaml.load(pge_config_file)

            # ACT
            pge_util.simulate_run_pge(
                pge_config['runconfig'],
                pge_config,
                context={
                    "job_specification": {
                        "params": [
                            {
                                "name": "input_dataset_id",
                                "value": "HLS.X30.T15SXR.2021250T163901.v2.0"
                            }
                        ]
                    }
                },
                output_dir='/tmp'
            )
