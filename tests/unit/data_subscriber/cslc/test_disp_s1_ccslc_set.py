"""Tests for the DISP-S1 compressed CSLC set marker (data_subscriber.cslc.disp_s1_ccslc_set)."""

import fnmatch
import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_ccslc_set import (
    create_ccslc_set_dataset,
    is_ccslc_id,
    make_ccslc_set_id,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
NOW = datetime(2026, 10, 6, 10, 15, 0)

CCSLC_IDS = [
    f"OPERA_L2_COMPRESSED-CSLC-S1_F36541_T{burst}_20250703T000000Z_20250703T000000Z_"
    f"20260627T000000Z_20261006T101400Z_VV_v1.0"
    for burst in ("064-135520-IW1", "064-135520-IW2", "064-135521-IW3")
]
L3_IDS = [
    "OPERA_L3_DISP-S1_IW_F36541_VV_20250703T140200Z_20260615T140210Z_v1.0_20261006T101400Z",
    "OPERA_L3_DISP-S1_IW_F36541_VV_20250703T140200Z_20260627T140210Z_v1.0_20261006T101400Z",
]
SET_ID = "disp_s1-ccslc-set-f36541-20250703-20250703-20260627-20261006T101500Z"


def _datasets():
    # The publish locations are Jinja-templated; the match patterns are not.
    return json.loads((REPO_ROOT / "conf" / "sds" / "files" / "datasets.json").read_text())["datasets"]


def _recognize(path):
    """HySDS recognizer semantics: the first datasets.json entry whose pattern matches wins."""
    for ds in _datasets():
        if re.search(ds["match_pattern"], path):
            return ds["type"]
    return None


def test_set_id_format():
    assert make_ccslc_set_id(36541, "20250703", "20250703", "20260627", NOW) == SET_ID


def test_set_id_sorts_after_every_product_id():
    # find_dataset_json walks dataset directories in sorted order, and the bulk
    # publisher queues dataset_processed events in that order, so this is what
    # makes the marker the last event a DISP-S1 SCIFLO emits.
    assert sorted(CCSLC_IDS + L3_IDS + [SET_ID])[-1] == SET_ID


def test_is_ccslc_id():
    assert all(is_ccslc_id(i) for i in CCSLC_IDS)
    assert not any(is_ccslc_id(i) for i in L3_IDS + [SET_ID])


def test_marker_dataset_contents(tmp_path):
    set_dir = Path(create_ccslc_set_dataset(
        str(tmp_path), frame_id=36541, ccslc_ids=list(reversed(CCSLC_IDS)),
        ksc_id="disp_s1-kcycle-k15-m6-f36541-20260627-state-config",
        sensing_date="20260627", now=NOW))

    assert set_dir == tmp_path / SET_ID
    met = json.loads((set_dir / f"{SET_ID}.met.json").read_text())
    dataset = json.loads((set_dir / f"{SET_ID}.dataset.json").read_text())

    assert met["id"] == SET_ID
    assert met[c.FRAME_ID] == 36541
    assert met[c.CCSLC_IDS] == sorted(CCSLC_IDS)
    assert met[c.CCSLC_COUNT] == 3
    assert (met[c.REF_DATE], met[c.FIRST_DATE], met[c.LAST_DATE]) == ("20250703", "20250703", "20260627")
    assert met[c.KSC_ID] == "disp_s1-kcycle-k15-m6-f36541-20260627-state-config"
    assert met[c.SENSING_DATE] == "20260627"

    assert dataset["index"]["suffix"] == "1_disp_s1-ccslc-set-2026.10"
    assert dataset["starttime"].startswith("2025-07-03T00:00:00")
    assert dataset["endtime"].startswith("2026-06-27T00:00:00")


def test_marker_requires_members(tmp_path):
    with pytest.raises(ValueError):
        create_ccslc_set_dataset(str(tmp_path), frame_id=36541, ccslc_ids=[], now=NOW)


def test_marker_rejects_an_unparseable_member(tmp_path):
    with pytest.raises(ValueError):
        create_ccslc_set_dataset(str(tmp_path), frame_id=36541, ccslc_ids=["not-a-ccslc"], now=NOW)


def test_datasets_json_recognizes_the_marker_and_only_the_marker():
    assert _recognize(f"/data/work/pge_output_dir/datasets/{SET_ID}") == c.DISP_S1_CCSLC_SET
    for ccslc_id in CCSLC_IDS:
        assert _recognize(f"/data/work/pge_output_dir/datasets/{ccslc_id}") == c.CCSLC_DATASET_TYPE
    for l3_id in L3_IDS:
        assert _recognize(f"/data/work/pge_output_dir/datasets/{l3_id}") == "L3_DISP_S1"


def test_marker_is_metadata_only():
    # Like the state configs, the marker has no files to upload.
    entry = next(ds for ds in _datasets() if ds["type"] == c.DISP_S1_CCSLC_SET)
    assert "publish" not in entry


@pytest.mark.parametrize("pattern", [
    # CCSLC counts in tools, ops scripts and the k-cycle evaluator use these wildcards;
    # a marker index they matched would inflate every one of them.
    "grq_*_l2_cslc_s1_compressed*",
    "grq_1_l2_cslc_s1_compressed*",
    "grq_*_l2_cslc_s1_compressed-*",
    # KSC queries
    "grq_*_disp_s1-kcycle*",
    "grq_*_disp_s1-kcycle-state-config*",
])
def test_marker_index_is_invisible_to_ccslc_and_ksc_wildcards(pattern):
    assert not fnmatch.fnmatchcase("grq_1_disp_s1-ccslc-set-2026.10", pattern)


@pytest.mark.parametrize("engine,path", [
    ("opensearch", "opensearch/grq_os_templates/os_template_disp_s1_ccslc_set.json"),
    ("elasticsearch", "elasticsearch/grq_es_templates/es_template_disp_s1_ccslc_set.json"),
])
def test_marker_index_template(engine, path):
    files = REPO_ROOT / "conf" / "sds" / "files"
    template = json.loads((files / path).read_text())
    assert any(fnmatch.fnmatchcase("grq_1_disp_s1-ccslc-set-2026.10", p) for p in template["index_patterns"])

    # Index template priorities must be unique among the GRQ templates of one engine,
    # or a fresh deploy fails with "multiple index templates may not match".
    # raw_decode reads the leading object only, as OpenSearch does: one template
    # carries a stray trailing comma.
    priorities = [
        json.JSONDecoder().raw_decode(f.read_text())[0].get("priority")
        for f in (files / path).parent.glob("*.json")
    ]
    priorities = [p for p in priorities if p is not None]
    assert len(priorities) == len(set(priorities)), f"{engine} template priorities collide: {priorities}"

    cluster_py = (REPO_ROOT / "conf" / "sds" / "cluster.py").read_text()
    assert Path(path).name in cluster_py, f"{Path(path).name} is not installed by cluster.py"
