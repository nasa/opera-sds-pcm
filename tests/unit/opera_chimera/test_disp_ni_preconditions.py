"""DISP-NI preconditions on the product metadata the gslc_download job writes.

They replace the canned golden inputs, so every RunConfig field the SAS needs must now come from the
k-set's own metadata or from settings.yaml: a GSLC or compressed GSLC left out, or the wrong
frequency/polarization, silently produces the wrong ministack.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

# opera_chimera needs chimera and mgrs, which ship on the cluster images but not in every local environment
try:
    from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as oc_const
    from opera_chimera.precondition_functions import OperaPreConditionFunctions
except ImportError as e:
    pytest.skip(f"precondition_functions is not importable here: {e}", allow_module_level=True)

GSLCS = [f"s3://asf-nisar/g{i}/NISAR_L2_PR_GSLC_00{i}_151_A_011_4005_DHDH.h5" for i in range(4, 7)]
COMPRESSED = ["s3://lts/products/GSLC_COMPRESSED/c1/OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_c1.h5"]
GUNWS = [f"s3://asf-nisar-gunw/u{i}/NISAR_L2_PR_GUNW_00{i}_151_A_011_00{i + 1}_4000_SH.h5" for i in range(4, 6)]
BBOX = [-155.0, 19.0, -154.0, 20.0]
SETTINGS = {"DISP_NI": {
    "ANCILLARY_MARGIN": 25,
    "THREADS_PER_WORKER": 2, "N_PARALLEL_BURSTS": 6, "AZIMUTH_BLOCKS": 6,
    "ALGORITHM_PARAMETERS": {
        "HISTORICAL": "s3://opera-ancillaries/algorithm_parameters/disp_ni/historical.yaml",
        "IONOSPHERE_HISTORICAL": "s3://opera-ancillaries/algorithm_parameters/disp_ni/ionosphere_historical.yaml",
    },
}}
PGE_CONFIG = {oc_const.GET_DISP_NI_MASK_FILE: {"s3_bucket": "opera-water-mask", "s3_key": "v0.3/EPSG4326.vrt"}}


@pytest.fixture
def working_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "workunit.json").write_text(json.dumps({"args": [str(tmp_path) + "/"]}))
    return tmp_path


def make_preconditions(**metadata_overrides):
    metadata = {
        "frame_id": 26410, "frequency": "frequencyA", "polarization": "HH", "save_compressed_slc": True,
        "processing_mode": "historical", "bounding_box": BBOX,
        "product_paths": {"L2_GSLC_NI": GSLCS, "L2_GSLC_COMPRESSED": COMPRESSED, "L2_GUNW_NI": GUNWS},
    }
    metadata.update(metadata_overrides)
    return OperaPreConditionFunctions({"product_metadata": {"metadata": metadata}}, PGE_CONFIG, SETTINGS, None)


def test_input_files_are_the_k_sets_gslcs_compressed_gslcs_and_gunws():
    assert make_preconditions().get_disp_ni_input_files() == {
        "input_file_paths": GSLCS, "compressed_gslc_paths": COMPRESSED, "last_processed": None,
        "gunw_files": GUNWS}


def test_the_first_k_set_has_no_compressed_gslcs():
    rc_params = make_preconditions(product_paths={"L2_GSLC_NI": GSLCS}).get_disp_ni_input_files()

    assert rc_params["compressed_gslc_paths"] == []


@pytest.mark.parametrize("product_paths", [
    {"L2_GSLC_NI": GSLCS, "L2_GUNW_NI": []},
    {"L2_GSLC_NI": GSLCS},
], ids=["no_chain", "metadata_before_gunws"])
def test_a_k_set_without_gunws_passes_null_so_the_sas_estimates_the_ionosphere(product_paths):
    # The RunConfig schema takes a non-empty list or null for gunw_files
    assert make_preconditions(product_paths=product_paths).get_disp_ni_input_files()["gunw_files"] is None


def test_processing_parameters_come_from_the_metadata_and_settings():
    rc_params = make_preconditions().get_disp_ni_processing_parameters()

    assert rc_params == {
        "frame_id": 26410, "frequency": "frequencyA", "polarization": "HH",
        "product_type": "DISP_NISAR_HISTORICAL", "save_compressed_slc": True,
        "threads_per_worker": "2", "n_parallel_bursts": "6", "azimuth_blocks": "6", "halo_rows": None,
        "troposphere_files": [],
    }


def test_both_algorithm_parameters_files_of_the_processing_mode_are_downloaded(working_dir):
    with patch("opera_chimera.precondition_functions.download_object_from_s3") as download:
        rc_params = make_preconditions().get_disp_ni_algorithm_parameters()

    assert rc_params == {
        "algorithm_parameters_file": str(working_dir / "historical.yaml"),
        "ionosphere_algorithm_parameters_file": str(working_dir / "ionosphere_historical.yaml"),
    }
    assert [c.args[:2] for c in download.call_args_list] == [
        ("opera-ancillaries", "algorithm_parameters/disp_ni/historical.yaml"),
        ("opera-ancillaries", "algorithm_parameters/disp_ni/ionosphere_historical.yaml"),
    ]


@pytest.mark.parametrize("precondition, rc_key, filename", [
    ("get_disp_ni_mask_file", "mask_file", "water_mask.vrt"),
    ("get_disp_ni_dem", "dem_file", "dem.vrt"),
], ids=["mask", "dem"])
def test_mask_and_dem_are_staged_over_the_frames_bounding_box(working_dir, precondition, rc_key, filename):
    preconditions = make_preconditions()
    preconditions.get_opera_ancillary = MagicMock(return_value={})

    with patch("opera_chimera.precondition_functions.write_pge_metrics"):
        rc_params = getattr(preconditions, precondition)()

    assert rc_params == {rc_key: str(working_dir / filename)}
    staging_args = preconditions.get_opera_ancillary.call_args.kwargs["staging_func_args"]
    assert staging_args.bbox == BBOX
    assert staging_args.margin == 25
    if rc_key == "mask_file":
        assert (staging_args.s3_bucket, staging_args.s3_key) == ("opera-water-mask", "v0.3/EPSG4326.vrt")
