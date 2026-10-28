"""DISP-NI products carry the metadata the next k-set is found by, at the top level.

The historical walker holds a frame's next k-set until GSLCDependency finds the previous k-set's
compressed GSLC in GRQ by metadata.frame_id and metadata.last_date_time, and the smoke test counts
L3_DISP_NI products by metadata.frame_id. The extractor writes those fields only into each file's
entry under Files, so without promotion the walker never sees the compressed GSLC and the frame
stays blocked. The test publishes PGE-shaped outputs offline through the real extractor,
settings.yaml and pge_outputs.yaml.
"""
import json
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

CREATION = "20261028T153405Z"
COMPRESSED_ID = (f"OPERA_L2_COMPRESSED-GSLC-NI_4000_26410_SH_20251226T000000Z_20251108T000000Z_"
                 f"20251226T000000Z_{CREATION}_v1.0")
L3_ID = f"OPERA_L3_DISP-NI_4000_26410_SH_20251108T155041Z_20251120T155041Z_v0.1_{CREATION}"
AUX_STEM = f"OPERA_L3_DISP-NI_4000_26410_SH_v0.1_{CREATION}"


def publish(work_dir: Path, monkeypatch):
    """Run product2dataset.convert over a fake DISP-NI PGE output directory, as the wrapper does."""
    product_dir = work_dir / "pge_output_dir"
    product_dir.mkdir()
    for name in (f"{L3_ID}.nc", f"{L3_ID}.iso.xml", f"{L3_ID}_BROWSE.png", f"{COMPRESSED_ID}.h5"):
        (product_dir / name).write_bytes(b"not a real product")
    (product_dir / f"{AUX_STEM}.log").write_text("log")
    (product_dir / f"{AUX_STEM}.catalog.json").write_text(json.dumps({"PGE_Version": "6.0.0", "SAS_Version": "0.3"}))

    (work_dir / "_job.json").write_text(json.dumps({
        "params": {"wf_name": "L3_DISP_NI"},
        "context": {"container_specification": {"version": "v9.9.9"},
                    "job_specification": {"dependency_images": [{"container_image_name": "opera_pge/disp_ni:6.0.0"}]}},
    }))
    shutil.copy(REPO_ROOT / "conf" / "sds" / "files" / "datasets.json", work_dir / "datasets.json")

    # Checksums call into hysds; they are not what is under test.
    monkeypatch.setattr(product2dataset, "create_dataset_checksums", lambda *args, **kwargs: None)
    # The iso.xml is not real; the bounding polygon is not what is under test.
    monkeypatch.setattr(product2dataset, "search_for_iso_xml_file", lambda dataset_dir: iter(()))

    created = product2dataset.convert(
        str(work_dir), str(product_dir), "L3_DISP_NI",
        extra_met={"lineage": [], "runconfig": {"input_file_group": {"input_file_paths": []}}},
        product_metadata={"id": "f26410_a4_f26410_a5", "frame_id": 26410},
    )

    mets = {}
    for dataset_dir in map(Path, created):
        mets[dataset_dir.name] = json.loads((dataset_dir / f"{dataset_dir.name}.met.json").read_text())
    return mets


def test_a_compressed_gslc_is_found_by_its_frame_and_last_date(tmp_path, monkeypatch):
    met = publish(tmp_path, monkeypatch)[COMPRESSED_ID]

    assert met["frame_id"] == 26410
    assert met["last_date_time"] == "2025-12-26T00:00:00.000000Z"
    assert met["first_date_time"] == "2025-11-08T00:00:00.000000Z"
    assert met["ref_date_time"] == met["Files"][0]["ref_date_time"]
    assert all(path.startswith("s3://") and "/products/GSLC_COMPRESSED/" in path for path in met["product_s3_paths"])


def test_a_displacement_product_is_found_by_its_frame(tmp_path, monkeypatch):
    met = publish(tmp_path, monkeypatch)[L3_ID]

    assert met["frame_id"] == 26410
