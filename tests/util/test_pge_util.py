from pathlib import Path

import os
import glob
import pytest
import yaml

from os.path import abspath, dirname, join
from util import pge_util

TEST_DIR = dirname(abspath(__file__))
REPO_DIR = abspath(join(TEST_DIR, os.pardir, os.pardir))


def test_simulate_run_pge__when_product_shortname_is_l30():
    # before
    for path in glob.iglob('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0*.tiff'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_HLS.yaml')

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
                            "value": "HLS.L30.T22VEQ.2021248T143156.v2.0_state_config"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )
    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_BAND_NAMES, start=1):
        assert Path(f'/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0_B{band_idx:02}_{band_name}.tiff').exists()

    assert Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.qa.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.catalog.json').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.iso.xml').exists()

    # after
    for path in glob.iglob('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0*.tiff'):
        Path(path).unlink(missing_ok=True)

    Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.qa.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.catalog.json').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0.iso.xml').unlink(missing_ok=False)


def test_simulate_run_pge__when_product_shortname_is_s30():
    # before
    for path in glob.iglob('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0*.tiff'):
        Path(path).unlink(missing_ok=True)

    pge_config_file_path = join(REPO_DIR, 'opera_chimera/configs/pge_configs/PGE_L3_HLS.yaml')

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
                            "value": "HLS.S30.T15SXR.2021250T163901.v2.0_state_config"
                        }
                    ]
                }
            },
            output_dir='/tmp'
        )

    # ASSERT
    for band_idx, band_name in enumerate(pge_util.DSWX_BAND_NAMES, start=1):
        assert Path(f'/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0_B{band_idx:02}_{band_name}.tiff').exists()

    assert Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.qa.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.catalog.json').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.iso.xml').exists()

    # after
    for path in glob.iglob('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0*.tiff'):
        Path(path).unlink(missing_ok=False)

    Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.qa.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.catalog.json').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0.iso.xml').unlink(missing_ok=False)


def test_simulate_run_pge__when_product_shortname_is_unsupported():
    # before_each
    pge_config_file_path = Path('opera_chimera/configs/pge_configs/PGE_L3_HLS.yaml')

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
                                "value": "HLS.X30.T15SXR.2021250T163901.v2.0_state_config"
                            }
                        ]
                    }
                },
                output_dir='/tmp'
            )
