"""DISP-NI wrapper functions on localized real inputs.

Chimera localizes every S3 input to the working directory under its basename. The lineage must list
exactly those files so they are copied into the PGE's input directory, and the RunConfig must point
at them inside the container, with the compressed GSLCs in the same list as the GSLCs because the
SAS reads both from gslc_file_list.
"""

from wrapper.pge_functions import disp_ni_lineage_metadata, update_disp_ni_runconfig

GSLCS = ["s3://asf-nisar/g4/G4.h5", "s3://asf-nisar/g5/G5.h5"]
COMPRESSED = ["s3://lts/products/GSLC_COMPRESSED/c1/C1.h5"]
GUNWS = ["s3://asf-nisar-gunw/u4/U4.h5"]


def make_run_config(work_dir, gunw_files=None):
    return {
        "input_file_group": {"input_file_paths": list(GSLCS), "compressed_gslc_paths": list(COMPRESSED),
                             "last_processed": None},
        "dynamic_ancillary_file_group": {
            "algorithm_parameters_file": f"{work_dir}/historical.yaml",
            "ionosphere_algorithm_parameters_file": f"{work_dir}/ionosphere_historical.yaml",
            "mask_file": f"{work_dir}/water_mask.vrt",
            "dem_file": f"{work_dir}/dem.vrt",
            "gunw_files": gunw_files,
            "troposphere_files": [],
        },
        "static_ancillary_file_group": {
            "algorithm_parameters_overrides_json": f"{work_dir}/overrides.json",
            "frame_to_bounds_json": "s3://opera-ancillaries/disp_frames/disp_ni/frame-to-bounds.json",
            "reference_date_database_json": "s3://opera-ancillaries/disp_frames/disp_ni/reference-dates.json",
        },
    }


def test_lineage_lists_every_localized_input(tmp_path):
    for name in ("dem.vrt", "dem_0.tif", "water_mask.vrt"):
        (tmp_path / name).write_text("")

    lineage = disp_ni_lineage_metadata({"run_config": make_run_config(tmp_path)}, str(tmp_path))

    assert sorted(lineage) == sorted(str(tmp_path / name) for name in (
        "G4.h5", "G5.h5", "C1.h5", "historical.yaml", "ionosphere_historical.yaml", "dem.vrt", "dem_0.tif",
        "water_mask.vrt", "overrides.json", "frame-to-bounds.json", "reference-dates.json"))


def test_runconfig_points_at_the_inputs_in_the_container_with_compressed_gslcs_in_the_gslc_list(tmp_path):
    context = {"run_config": make_run_config(tmp_path),
               "job_specification": {"params": [{"name": "container_home", "value": "/home/mamba"}]}}

    run_config = update_disp_ni_runconfig(context, str(tmp_path))

    prefix = "/home/mamba/input_dir"
    assert run_config["input_file_group"]["input_file_paths"] == [f"{prefix}/G4.h5", f"{prefix}/G5.h5",
                                                                  f"{prefix}/C1.h5"]
    dynamic = run_config["dynamic_ancillary_file_group"]
    assert dynamic["algorithm_parameters_file"] == f"{prefix}/historical.yaml"
    assert dynamic["ionosphere_algorithm_parameters_file"] == f"{prefix}/ionosphere_historical.yaml"
    assert (dynamic["dem_file"], dynamic["mask_file"]) == (f"{prefix}/dem.vrt", f"{prefix}/water_mask.vrt")
    assert dynamic["gunw_files"] is None
    static = run_config["static_ancillary_file_group"]
    assert static == {"algorithm_parameters_overrides_json": f"{prefix}/overrides.json",
                      "frame_to_bounds_json": f"{prefix}/frame-to-bounds.json",
                      "reference_date_database_json": f"{prefix}/reference-dates.json"}


def test_gunws_are_in_the_lineage_and_point_into_the_container(tmp_path):
    lineage = disp_ni_lineage_metadata({"run_config": make_run_config(tmp_path, list(GUNWS))}, str(tmp_path))
    assert str(tmp_path / "U4.h5") in lineage

    context = {"run_config": make_run_config(tmp_path, list(GUNWS)),
               "job_specification": {"params": [{"name": "container_home", "value": "/home/mamba"}]}}
    run_config = update_disp_ni_runconfig(context, str(tmp_path))
    assert run_config["dynamic_ancillary_file_group"]["gunw_files"] == ["/home/mamba/input_dir/U4.h5"]
