import logging

import pytest

from data_subscriber.cmr import Collection
from data_subscriber.grq_query import _grq_doc_to_granule

_MISSING = object()

_GRANULE_ID = "OPERA_L2_RTC-S1_T064-135518-IW1_20260101T000000Z_20260102T000000Z_S1A_30_v1.0"
_DUAL_POL_PATHS = [
    f"s3://bucket/{_GRANULE_ID}/{_GRANULE_ID}_VV.tif",
    f"s3://bucket/{_GRANULE_ID}/{_GRANULE_ID}_VH.tif",
    f"s3://bucket/{_GRANULE_ID}/{_GRANULE_ID}_mask.tif",
    f"s3://bucket/{_GRANULE_ID}/{_GRANULE_ID}.iso.xml",
]


def _doc(polarizations=_MISSING, product_s3_paths=None):
    metadata = {
        "product_s3_paths": _DUAL_POL_PATHS if product_s3_paths is None else product_s3_paths,
        "acquisition_ts": "2026-01-01T00:00:00Z",
        "CollectionName": "OPERA_L2_RTC-S1_V1",
    }
    if polarizations is not _MISSING:
        metadata["polarizations"] = polarizations
    return {"_source": {"id": _GRANULE_ID, "creation_timestamp": "2026-01-02T00:00:00Z", "metadata": metadata}}


@pytest.mark.parametrize("polarizations, expected", [
    pytest.param(["VV", "VH"], ["VV", "VH"], id="list"),
    pytest.param("VV", ["VV"], id="bare-string-single-pol"),
    pytest.param("VV+VH", ["VV", "VH"], id="plus-joined-string"),
    pytest.param("vv, vh", ["VV", "VH"], id="comma-joined-lowercase-string"),
    pytest.param(_MISSING, ["VV", "VH"], id="missing-key-derived-from-file-names"),
])
def test_grq_doc_to_granule_normalizes_polarization(polarizations, expected):
    granule = _grq_doc_to_granule(_doc(polarizations), Collection.RTC_S1_V1)

    assert granule["polarization"] == expected


def test_grq_doc_to_granule_missing_polarization_without_pol_files_warns(caplog):
    paths = [f"s3://bucket/{_GRANULE_ID}/{_GRANULE_ID}.iso.xml"]

    with caplog.at_level(logging.WARNING):
        granule = _grq_doc_to_granule(_doc(product_s3_paths=paths), Collection.RTC_S1_V1)

    assert granule["polarization"] == []
    assert _GRANULE_ID in caplog.text
