"""A DISP-S1 SCIFLO that produces compressed CSLCs also publishes one CCSLC-set marker.

The component tests below publish PGE-shaped DISP-S1 outputs offline through the real
extractor, settings.yaml and pge_outputs.yaml, which is the path a SCIFLO_L3_DISP_S1
job takes on a cluster.
"""
import json
import os
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

try:
    import hysds.utils  # noqa: F401
    _HYSDS_STUBS = {}
except ImportError:
    # util.checksum_util imports hysds, which is only installed on cluster images.
    _HYSDS_STUBS = {"hysds": MagicMock(), "hysds.utils": MagicMock()}

with patch.dict(sys.modules, _HYSDS_STUBS):
    from product2dataset import product2dataset

REPO_ROOT = Path(__file__).resolve().parents[3]

KSC_ID = "disp_s1-kcycle-k15-m6-f36541-20260627-state-config"
L3_ID = "OPERA_L3_DISP-S1_IW_F36541_VV_20250703T140200Z_20260627T140210Z_v1.0_20261006T101400Z"
CCSLC_IDS = [
    f"OPERA_L2_COMPRESSED-CSLC-S1_F36541_T064-{burst}_20250703T000000Z_20250703T000000Z_"
    f"20260627T000000Z_20261006T101400Z_VV_v1.0"
    for burst in ("135520-IW1", "135520-IW2")
]
AUX_STEM = "OPERA_L3_DISP-S1_IW_F36541_v1.0_20261006T101400Z"


def _job_json(wf_name):
    return {
        "params": {"wf_name": wf_name},
        "context": {
            "container_specification": {"version": "v9.9.9"},
            "job_specification": {
                "dependency_images": [{"container_image_name": "opera_pge/disp_s1:3.0.11"}]
            },
        },
    }


def _publish(work_dir: Path, mocker, ccslc_ids=CCSLC_IDS, wf_name="L3_DISP_S1"):
    """Run product2dataset.convert over a fake DISP-S1 PGE output directory."""
    product_dir = work_dir / "pge_output_dir"
    product_dir.mkdir()
    for name in [f"{L3_ID}.nc", f"{L3_ID}_BROWSE.png"] + [f"{i}.h5" for i in ccslc_ids]:
        (product_dir / name).write_bytes(b"not a real product")
    (product_dir / f"{AUX_STEM}.log").write_text("log")
    (product_dir / f"{AUX_STEM}.catalog.json").write_text(
        json.dumps({"PGE_Version": "3.0.11", "SAS_Version": "0.24"}))

    (work_dir / "_job.json").write_text(json.dumps(_job_json(wf_name)))
    shutil.copy(REPO_ROOT / "conf" / "sds" / "files" / "datasets.json", work_dir / "datasets.json")

    # Checksums call into hysds; they are not what is under test.
    mocker.patch.object(product2dataset, "create_dataset_checksums")

    extra_met = {
        "lineage": ["/work/input/a.h5", "/work/input/b.h5"],
        "runconfig": {
            "localize": ["s3://bucket/inputs/a.h5"],
            "input_file_group": {"input_file_paths": ["/work/input/a.h5"]},
        },
    }
    created = product2dataset.convert(
        str(work_dir), str(product_dir), "L3_DISP_S1", extra_met=extra_met,
        product_metadata={"id": KSC_ID, "frame_id": 36541, "acquisition_cycle": 3972,
                          "sensing_date": "20260627"},
    )
    return product_dir / "datasets", created


def _markers(datasets_dir):
    return [d for d in os.listdir(datasets_dir) if d.startswith("disp_s1-ccslc-set-")]


class TestCcslcSetMarker:

    def test_marker_lists_every_ccslc_and_is_published_last(self, tmp_path, mocker):
        datasets_dir, created = _publish(tmp_path, mocker)

        markers = _markers(datasets_dir)
        assert len(markers) == 1
        marker = markers[0]
        assert marker.startswith("disp_s1-ccslc-set-f36541-20250703-20250703-20260627-")

        met = json.loads((datasets_dir / marker / f"{marker}.met.json").read_text())
        assert met["ccslc_ids"] == sorted(CCSLC_IDS)
        assert met["ccslc_count"] == 2
        assert met["frame_id"] == 36541
        assert met["ksc_id"] == KSC_ID
        dataset = json.loads((datasets_dir / marker / f"{marker}.dataset.json").read_text())
        assert dataset["index"]["suffix"].startswith("1_disp_s1-ccslc-set-")

        # HySDS walks dataset directories in sorted order and publishes in that order.
        assert sorted(os.listdir(datasets_dir))[-1] == marker
        # The marker is not a product.
        assert all(Path(d).name != marker for d in created)
        assert {Path(d).name for d in created} == set(CCSLC_IDS) | {L3_ID}

    def test_no_marker_without_ccslcs(self, tmp_path, mocker):
        datasets_dir, created = _publish(tmp_path, mocker, ccslc_ids=[])
        assert _markers(datasets_dir) == []
        assert [Path(d).name for d in created] == [L3_ID]

    def test_no_marker_for_a_product_update(self, tmp_path, mocker):
        # A product-update job republishes existing products; it is not a new set.
        datasets_dir, _ = _publish(tmp_path, mocker, wf_name="Product_Update")
        assert _markers(datasets_dir) == []
