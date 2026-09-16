"""CSLC filename metadata is promoted to the top level of the dataset metadata.

The DISP-S1 cycle evaluator and the other consumers that select CSLC datasets by burst
and sensing time read the burst id at the top level of the dataset metadata and the
acquisition time from the dataset starttime. The component tests below publish
PGE-shaped datasets offline through the real extractor, settings.yaml and
pge_outputs.yaml, which is the path a CSLC-S1 PGE job takes on a cluster.
"""
import json
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    import hysds.utils  # noqa: F401
    _HYSDS_STUBS = {}
except ImportError:
    # util.checksum_util imports hysds, which is only installed on cluster images.
    _HYSDS_STUBS = {"hysds": MagicMock(), "hysds.utils": MagicMock()}

with patch.dict(sys.modules, _HYSDS_STUBS):
    from product2dataset import product2dataset

REPO_ROOT = Path(__file__).resolve().parents[3]

CSLC_ID = "OPERA_L2_CSLC-S1_T091-193607-IW1_20260909T222012Z_20260910T212729Z_S1D_VV_v1.1"
CSLC_STATIC_ID = "OPERA_L2_CSLC-S1-STATIC_T091-193607-IW1_20140101_S1D_v1.0"
AUX_STEM = "OPERA_L2_CSLC-S1_20260910T212729Z_S1D_VV_v1.1"
ACQUISITION_TS = "2026-09-09T22:20:12.000000Z"


class TestPromoteFileMetadata:

    def test_lifts_keys_constant_across_files(self):
        files = [{"FileName": f"f{i}", "burst_id": "T091-193607-IW1", "pol": "VV"} for i in range(3)]
        met = {"Files": files}

        promoted = product2dataset.promote_file_metadata(met, ("burst_id", "pol", "sensor"))

        assert promoted == ["burst_id", "pol"]
        assert met["burst_id"] == "T091-193607-IW1"
        assert met["pol"] == "VV"
        assert "sensor" not in met
        assert met["Files"] == files

    def test_keeps_an_existing_top_level_value(self):
        met = {"burst_id": "T001-000001-IW1", "Files": [{"burst_id": "T091-193607-IW1"}]}

        promoted = product2dataset.promote_file_metadata(met, ("burst_id",))

        assert promoted == []
        assert met["burst_id"] == "T001-000001-IW1"

    def test_does_not_guess_when_files_disagree(self):
        met = {"Files": [{"burst_id": "T091-193607-IW1"}, {"burst_id": "T091-193607-IW2"}]}

        promoted = product2dataset.promote_file_metadata(met, ("burst_id",))

        assert promoted == []
        assert "burst_id" not in met

    def test_tolerates_a_dataset_without_files(self):
        met = {}

        assert product2dataset.promote_file_metadata(met, ("burst_id",)) == []
        assert met == {}


def _job_json(wf_name):
    return {
        "params": {"wf_name": wf_name},
        "context": {
            "container_specification": {"version": "v9.9.9"},
            "job_specification": {
                "dependency_images": [{"container_image_name": "opera_pge/cslc_s1:2.1.4"}]
            },
        },
    }


def _publish(work_dir: Path, pge_name, primary_files, mocker):
    """Run product2dataset.convert over a fake PGE output directory, as the wrapper does."""
    product_dir = work_dir / "pge_output_dir"
    product_dir.mkdir()
    for name in primary_files:
        (product_dir / name).write_bytes(b"not a real product")
    (product_dir / f"{AUX_STEM}.log").write_text("log")
    (product_dir / f"{AUX_STEM}.catalog.json").write_text(
        json.dumps({"PGE_Version": "2.1.4", "SAS_Version": "0.5.5"}))

    (work_dir / "_job.json").write_text(json.dumps(_job_json(pge_name)))
    shutil.copy(REPO_ROOT / "conf" / "sds" / "files" / "datasets.json", work_dir / "datasets.json")

    # Checksums call into hysds; they are not what is under test.
    mocker.patch.object(product2dataset, "create_dataset_checksums")

    extra_met = {
        "lineage": ["/work/input/S1D_IW_SLC.zip", "/work/input/S1D_OPER_AUX_RESORB.EOF"],
        "runconfig": {
            "localize": ["s3://bucket/inputs/S1D_OPER_AUX_RESORB.EOF"],
            "input_file_group": {"input_file_paths": ["/work/input/S1D_IW_SLC.zip"]},
        },
    }
    created = product2dataset.convert(
        str(work_dir), str(product_dir), pge_name, extra_met=extra_met,
        product_metadata={"id": "S1D_IW_SLC__1SDV_20260909T221948_20260909T222015_004507_0085FB_16EE"},
    )

    assert len(created) == 1
    dataset_dir = Path(created[0])
    dataset_id = dataset_dir.name
    met = json.loads((dataset_dir / f"{dataset_id}.met.json").read_text())
    dataset = json.loads((dataset_dir / f"{dataset_id}.dataset.json").read_text())
    return dataset_id, met, dataset


class TestConvertPromotesCslcMetadata:

    def test_l2_cslc_s1_dataset_carries_burst_metadata_and_starttime(self, tmp_path, mocker):
        dataset_id, met, dataset = _publish(
            tmp_path, "L2_CSLC_S1",
            [f"{CSLC_ID}.h5", f"{CSLC_ID}_BROWSE.png", f"{CSLC_ID}.iso.xml"], mocker)

        assert dataset_id == CSLC_ID
        assert met["burst_id"] == "T091-193607-IW1"
        assert met["acquisition_ts"] == ACQUISITION_TS
        assert met["sensor"] == "S1D"
        assert met["pol"] == "VV"
        assert met["dataset_version"] == "v1.1"
        # The per-file entries are untouched and still carry the same fields.
        assert len(met["Files"]) == 3
        assert {f["burst_id"] for f in met["Files"]} == {"T091-193607-IW1"}
        assert all(path.startswith("s3://") for path in met["product_s3_paths"])
        assert dataset["starttime"] == ACQUISITION_TS
        assert dataset["endtime"] == ACQUISITION_TS

    def test_l2_cslc_s1_static_dataset_carries_burst_metadata(self, tmp_path, mocker):
        dataset_id, met, dataset = _publish(
            tmp_path, "L2_CSLC_S1_STATIC",
            [f"{CSLC_STATIC_ID}.h5", f"{CSLC_STATIC_ID}.iso.xml"], mocker)

        assert dataset_id == CSLC_STATIC_ID
        assert met["burst_id"] == "T091-193607-IW1"
        assert met["validity_ts"] == "2014-01-01T00:00:00.000000Z"
        assert met["sensor"] == "S1D"
        # Static layers are selected by burst only; they get no dataset time range.
        assert "starttime" not in dataset
        assert "acquisition_ts" not in met
