from pathlib import Path

import pytest
import yaml

from util import pge_util


def test_simulate_run_pge__when_product_shortname_is_l30():
    # before
    Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.tif').unlink(missing_ok=True)
    pge_config_file_path = Path('opera_chimera/configs/pge_configs/PGE_L3_HLS.yaml')

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
    assert Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.tif').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.met').exists()

    # after
    Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.tif').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0.met').unlink(missing_ok=False)


def test_simulate_run_pge__when_product_shortname_is_s30():
    # before
    Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.tif').unlink(missing_ok=True)
    pge_config_file_path = Path('opera_chimera/configs/pge_configs/PGE_L3_HLS.yaml')

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
    assert Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.tif').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.log').exists()
    assert Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.met').exists()

    # after
    Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.tif').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.log').unlink(missing_ok=False)
    Path('/tmp/OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0.met').unlink(missing_ok=False)


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
