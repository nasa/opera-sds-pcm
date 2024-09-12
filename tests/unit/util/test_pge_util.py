from pathlib import Path

import os
import glob
import pytest
import yaml

from os.path import abspath, dirname, join
from util import pge_util

TEST_DIR = dirname(abspath(__file__))
REPO_DIR = abspath(join(TEST_DIR, os.pardir, os.pardir, os.pardir))


def test_simulate_cslc_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L2_CSLC-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_CSLC_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC-r1"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_output_basename = 'OPERA_L2_CSLC-S1_{burst_id}_20220501T015035Z_{creation_ts}Z_S1A_VV_v0.1'
    expected_ancillary_basename = 'OPERA_L2_CSLC-S1_{creation_ts}Z_S1A_VV_v0.1'
    creation_ts = pge_util.get_time_for_filename()

    try:
        for burst_id in pge_util.CSLC_BURST_IDS:
            output_basename = expected_output_basename.format(burst_id=burst_id, creation_ts=creation_ts)

            assert Path(f'/tmp/{output_basename}.h5').exists()
            assert Path(f'/tmp/{output_basename}_BROWSE.png').exists()
            assert Path(f'/tmp/{output_basename}.iso.xml').exists()

        ancillary_basename = expected_ancillary_basename.format(creation_ts=creation_ts)
        assert Path(f'/tmp/{ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{ancillary_basename}.log').exists()
        assert Path(f'/tmp/{ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_CSLC-S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_cslc_s1_static_pge():
    for path in glob.iglob('/tmp/OPERA_L2_CSLC-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_CSLC_S1_STATIC.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC-r1"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_static_output_basename = 'OPERA_L2_CSLC-S1-STATIC_{burst_id}_20140403_S1A_v0.1'
    expected_ancillary_basename = 'OPERA_L2_CSLC-S1_{creation_ts}Z_S1A_VV_v0.1'
    creation_ts = pge_util.get_time_for_filename()

    try:
        for burst_id in pge_util.CSLC_BURST_IDS:
            static_output_basename = expected_static_output_basename.format(burst_id=burst_id)

            assert Path(f'/tmp/{static_output_basename}.h5').exists()
            assert Path(f'/tmp/{static_output_basename}.iso.xml').exists()

        ancillary_basename = expected_ancillary_basename.format(creation_ts=creation_ts)
        assert Path(f'/tmp/{ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{ancillary_basename}.log').exists()
        assert Path(f'/tmp/{ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_CSLC-S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_rtc_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L2_RTC-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_RTC_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F-r1"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_output_basename = 'OPERA_L2_RTC-S1_{burst_id}_20180504T104507Z_{creation_ts}Z_S1B_30_v0.1'
    expected_ancillary_basename = 'OPERA_L2_RTC-S1_{creation_ts}Z_S1B_30_v0.1'
    creation_ts = pge_util.get_time_for_filename()

    try:
        for burst_id in pge_util.RTC_BURST_IDS:
            output_basename = expected_output_basename.format(burst_id=burst_id, creation_ts=creation_ts)

            assert Path(f'/tmp/{output_basename}.h5').exists()
            assert Path(f'/tmp/{output_basename}_VV.tif').exists()
            assert Path(f'/tmp/{output_basename}_VH.tif').exists()
            assert Path(f'/tmp/{output_basename}_mask.tif').exists()
            assert Path(f'/tmp/{output_basename}_BROWSE.png').exists()
            assert Path(f'/tmp/{output_basename}.iso.xml').exists()

        ancillary_basename = expected_ancillary_basename.format(creation_ts=creation_ts)
        assert Path(f'/tmp/{ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{ancillary_basename}.log').exists()
        assert Path(f'/tmp/{ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_RTC-S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_rtc_s1_static_pge():
    for path in glob.iglob('/tmp/OPERA_L2_RTC-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L2_RTC_S1_STATIC.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": [
                    {
                        "name": "input_dataset_id",
                        "value": "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F-r1"
                    }
                ]
            }
        },
        output_dir='/tmp'
    )

    expected_static_output_basename = 'OPERA_L2_RTC-S1-STATIC_{burst_id}_20140403_S1B_30_v0.1'
    expected_ancillary_basename = 'OPERA_L2_RTC-S1_{creation_ts}Z_S1B_30_v0.1'
    creation_ts = pge_util.get_time_for_filename()

    try:
        for burst_id in pge_util.RTC_BURST_IDS:
            static_output_basename = expected_static_output_basename.format(burst_id=burst_id)

            assert Path(f'/tmp/{static_output_basename}.h5').exists()
            assert Path(f'/tmp/{static_output_basename}_BROWSE.png').exists()
            assert Path(f'/tmp/{static_output_basename}_incidence_angle.tif').exists()
            assert Path(f'/tmp/{static_output_basename}_mask.tif').exists()
            assert Path(f'/tmp/{static_output_basename}_local_incidence_angle.tif').exists()
            assert Path(f'/tmp/{static_output_basename}_number_of_looks.tif').exists()
            assert Path(f'/tmp/{static_output_basename}_rtc_anf_gamma0_to_beta0.tif').exists()
            assert Path(f'/tmp/{static_output_basename}_rtc_anf_gamma0_to_sigma0.tif').exists()
            assert Path(f'/tmp/{static_output_basename}.iso.xml').exists()

        ancillary_basename = expected_ancillary_basename.format(creation_ts=creation_ts)
        assert Path(f'/tmp/{ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{ancillary_basename}.log').exists()
        assert Path(f'/tmp/{ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L2_RTC-S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_dswx_hls_pge_with_l30():
    # before
    for path in glob.iglob(f'/tmp/OPERA_L3_DSWx-HLS*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

        # ACT
        pge_util.simulate_run_pge(
            pge_config['runconfig'],
            pge_config,
            context={
                "job_specification": {
                    "params": [
                        {
                            "name": "input_dataset_id",
                            "value": "HLS.L30.T22VEQ.2021248T143156.v2.0-r1"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )

    creation_ts = pge_util.get_time_for_filename()
    expected_output_base_name = f"OPERA_L3_DSWx-HLS_T22VEQ_20210905T143156Z_{creation_ts}Z_L8_30_v2.0"

    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_HLS_BAND_NAMES, start=1):
        assert Path(f'/tmp/{expected_output_base_name}_B{band_idx:02}_{band_name}.tif').exists()

    assert Path(f'/tmp/{expected_output_base_name}.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}_BROWSE.png').exists()
    assert Path(f'/tmp/{expected_output_base_name}_BROWSE.tif').exists()
    assert Path(f'/tmp/{expected_output_base_name}.qa.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.catalog.json').exists()
    assert Path(f'/tmp/{expected_output_base_name}.iso.xml').exists()

    # after
    for path in glob.iglob(f'/tmp/OPERA_L3_DSWx-HLS*'):
        Path(path).unlink(missing_ok=True)


def test_simulate_dswx_hls_pge_with_s30():
    # before
    for path in glob.iglob(f'/tmp/OPERA_L3_DSWx-HLS*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

        # ACT
        pge_util.simulate_run_pge(
            pge_config['runconfig'],
            pge_config,
            context={
                "job_specification": {
                    "params": [
                        {
                            "name": "input_dataset_id",
                            "value": "HLS.S30.T15SXR.2021250T163901.v2.0-r1"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )

    creation_ts = pge_util.get_time_for_filename()
    expected_output_base_name = f"OPERA_L3_DSWx-HLS_T15SXR_20210907T163901Z_{creation_ts}Z_S2A_30_v2.0"

    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_HLS_BAND_NAMES, start=1):
        assert Path(f'/tmp/{expected_output_base_name}_B{band_idx:02}_{band_name}.tif').exists()

    assert Path(f'/tmp/{expected_output_base_name}.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}_BROWSE.png').exists()
    assert Path(f'/tmp/{expected_output_base_name}_BROWSE.tif').exists()
    assert Path(f'/tmp/{expected_output_base_name}.qa.log').exists()
    assert Path(f'/tmp/{expected_output_base_name}.catalog.json').exists()
    assert Path(f'/tmp/{expected_output_base_name}.iso.xml').exists()

    # after
    for path in glob.iglob(f'/tmp/OPERA_L3_DSWx-HLS*'):
        Path(path).unlink(missing_ok=False)


def test_simulate_dswx_hls_pge_with_unsupported():
    # before_each
    pge_config_file_path = Path('opera_chimera/configs/pge_configs/PGE_L3_DSWx_HLS.yaml')

    # ARRANGE
    with pytest.raises(Exception) as exc_info:
        with open(pge_config_file_path) as pge_config_file:
            pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

            # ACT
            pge_util.simulate_run_pge(
                pge_config['runconfig'],
                pge_config,
                context={
                    "job_specification": {
                        "params": [
                            {
                                "name": "input_dataset_id",
                                "value": "HLS.X30.T15SXR.2021250T163901.v2.0-r1"
                            }
                        ]
                    }
                },
                output_dir='/tmp'
            )


def test_simulate_dswx_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L3_DSWx-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": []
            }
        },
        output_dir='/tmp'
    )

    creation_ts = pge_util.get_time_for_filename()
    expected_output_basename = 'OPERA_L3_DSWx-S1_{tile_id}_20200702T231843Z_{creation_ts}Z_S1A_30_v0.1'
    expected_ancillary_basename = f'OPERA_L3_DSWx-S1_{creation_ts}Z_S1A_30_v0.1'

    try:
        for tile_id in pge_util.DSWX_TILES:
            for band_idx, band_name in enumerate(pge_util.DSWX_S1_BAND_NAMES, start=1):
                assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_B{band_idx:02}_{band_name}.tif').exists()

            assert Path(
                f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_BROWSE.tif').exists()
            assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_BROWSE.png').exists()
            assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}.iso.xml').exists()

        assert Path(f'/tmp/{expected_ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.log').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L3_DSWx-S1*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_dswx_ni_pge():
    for path in glob.iglob('/tmp/OPERA_L3_DSWx-NI*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DSWx_NI.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": []
            }
        },
        output_dir='/tmp'
    )

    creation_ts = pge_util.get_time_for_filename()
    expected_output_basename = 'OPERA_L3_DSWx-NI_{tile_id}_{creation_ts}Z_{creation_ts}Z_LSAR_30_v0.1'
    expected_ancillary_basename = f'OPERA_L3_DSWx-NI_{creation_ts}Z_LSAR_30_v0.1'

    try:
        for tile_id in pge_util.DSWX_TILES:
            for band_idx, band_name in enumerate(pge_util.DSWX_S1_BAND_NAMES, start=1):
                assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_B{band_idx:02}_{band_name}.tif').exists()

            assert Path(
                f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_BROWSE.tif').exists()
            assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}_BROWSE.png').exists()
            assert Path(f'/tmp/{expected_output_basename.format(tile_id=tile_id, creation_ts=creation_ts)}.iso.xml').exists()

        assert Path(f'/tmp/{expected_ancillary_basename}.catalog.json').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.log').exists()
        assert Path(f'/tmp/{expected_ancillary_basename}.qa.log').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L3_DSWx-NI*.*'):
            Path(path).unlink(missing_ok=True)


def test_simulate_disp_s1_pge():
    for path in glob.iglob('/tmp/OPERA_L3_DISP-S1*.*'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_DISP_S1.yaml')

    with open(pge_config_file_path) as pge_config_file:
        pge_config = yaml.load(pge_config_file, Loader=yaml.SafeLoader)

    pge_util.simulate_run_pge(
        pge_config['runconfig'],
        pge_config,
        context={
            "job_specification": {
                "params": []
            }
        },
        output_dir='/tmp'
    )

    creation_ts = pge_util.get_time_for_filename()
    expected_output_basename = 'OPERA_L3_DISP-S1_IW_F10859_VV_20160705T000000Z_20160822T000000Z_v0.1_{creation_ts}Z'
    expected_ancillary_basename = 'OPERA_L3_DISP-S1_IW_F10859_v0.1_{creation_ts}Z'
    expected_compressed_cslc_basename = 'OPERA_L2_COMPRESSED-CSLC-S1_{burst_id}_20160705T000000Z_20160822T000000Z_20160915T000000Z_{creation_ts}Z_VV_v0.1'

    try:
        assert Path(f'/tmp/{expected_output_basename.format(creation_ts=creation_ts)}.nc').exists()
        assert Path(f'/tmp/{expected_output_basename.format(creation_ts=creation_ts)}_BROWSE.png').exists()
        assert Path(f'/tmp/{expected_output_basename.format(creation_ts=creation_ts)}.iso.xml').exists()
        assert Path(f'/tmp/{expected_ancillary_basename.format(creation_ts=creation_ts)}.catalog.json').exists()
        assert Path(f'/tmp/{expected_ancillary_basename.format(creation_ts=creation_ts)}.log').exists()
        assert Path(f'/tmp/{expected_ancillary_basename.format(creation_ts=creation_ts)}.qa.log').exists()

        for burst_id in pge_util.CCSLC_BURST_IDS:
            assert Path(f'/tmp/{expected_compressed_cslc_basename.format(burst_id=burst_id, creation_ts=creation_ts)}.h5').exists()
    finally:
        for path in glob.iglob('/tmp/OPERA_L3_DISP-S1*.*'):
            Path(path).unlink(missing_ok=True)

        for path in glob.iglob('/tmp/OPERA_L2_COMPRESSED-CSLC-S1*.*'):
            Path(path).unlink(missing_ok=True)

