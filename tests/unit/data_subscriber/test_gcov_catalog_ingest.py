import json
import os
import re

import pytest

from data_subscriber.gcov.gcov_catalog_ingest import GcovCatalogIngest
from opera_commons.constants import product_metadata as pm
from util.common_util import convert_datetime

"""
Local only unit tests for gcov_catalog_ingest's HySDS dataset descriptors.

These guard the field name in the emitted .dataset.json. GCOV granules are
catalogued by this module rather than by extractor/extract.py, so the
`creation_timestamp` that every other OPERA product type carries has to be
written here explicitly -- and was previously written as `creation_time`,
which is the state-config field name (pm.STATE_CONFIG_CREATION_TIME).
"""


@pytest.fixture
def example_cmr_response():
    """Load the example CMR response from file."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "test_data", "example_cmr_query_response_gcov.json")
    with open(path) as f:
        return json.load(f)


@pytest.fixture
def ingester():
    """
    A GcovCatalogIngest without its __init__.

    __init__ loads the MGRS track/frame sqlite DB, which _create_datasets never
    touches -- it uses only self.dataset_pattern and the static polygon helper.
    Bypassing it keeps this test free of that fixture.

    The pattern is deliberately permissive: granule-ID parsing is covered by
    test_gcov_granule_util.py, and what is under test here is the descriptor.
    """
    obj = object.__new__(GcovCatalogIngest)
    obj.dataset_pattern = re.compile(r"NISAR_L2_.+")
    obj.settings = {}
    obj.es_conn = None
    return obj


def _dataset_jsons(root):
    """Every <dir>/<dir>.dataset.json under root, keyed by directory name."""
    out = {}
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name, f"{name}.dataset.json")
        if os.path.isfile(path):
            with open(path) as f:
                out[name] = json.load(f)
    return out


def test_granule_dataset_json_carries_creation_timestamp(
        ingester, example_cmr_response, tmp_path, monkeypatch):
    """Each GCOV granule descriptor must use creation_timestamp, not creation_time."""
    monkeypatch.chdir(tmp_path)

    created = ingester._create_datasets(example_cmr_response["items"], batch_publish=False)
    assert created > 0, "fixture produced no datasets; the rest of this test proves nothing"

    descriptors = _dataset_jsons(tmp_path)
    assert len(descriptors) == created

    for name, ds in descriptors.items():
        assert "creation_timestamp" in ds, f"{name} has no creation_timestamp"
        assert "creation_time" not in ds, (
            f"{name} still carries the state-config field name creation_time"
        )
        # round-trips through the same helper that wrote it
        convert_datetime(ds["creation_timestamp"])


def test_batch_dataset_json_carries_creation_timestamp(
        ingester, example_cmr_response, tmp_path, monkeypatch):
    """The GCOV batch manifest descriptor must use the same field name."""
    monkeypatch.chdir(tmp_path)

    ingester._create_datasets(example_cmr_response["items"], batch_publish=True)

    batches = {n: d for n, d in _dataset_jsons(tmp_path).items()
               if n.startswith("NISAR_GCOV_BATCH_")}
    assert len(batches) == 1, f"expected exactly one batch dataset, got {sorted(batches)}"

    ds = next(iter(batches.values()))
    assert "creation_timestamp" in ds
    assert "creation_time" not in ds
    convert_datetime(ds["creation_timestamp"])


def test_field_name_is_not_the_state_config_one(
        ingester, example_cmr_response, tmp_path, monkeypatch):
    """
    Pins the intent rather than the spelling: GCOV granules are products, so
    their descriptor must not use the constant reserved for state configs.
    """
    monkeypatch.chdir(tmp_path)

    ingester._create_datasets(example_cmr_response["items"], batch_publish=True)

    for name, ds in _dataset_jsons(tmp_path).items():
        assert pm.STATE_CONFIG_CREATION_TIME not in ds, (
            f"{name} uses {pm.STATE_CONFIG_CREATION_TIME!r}, which is the "
            f"state-config creation field, not the product one"
        )
