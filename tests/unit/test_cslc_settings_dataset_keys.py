"""Pin the L2_CSLC_S1 dataset time range to the burst acquisition time.

The DISP-S1 cycle evaluator selects CSLCs of a sensing date through the dataset
starttime. A dataset created from a CSLC-S1 PGE product only has one if the product
type's Dataset_Keys names it, so a silent revert of this setting would leave locally
produced CSLCs selectable only through the per-file fallback.
"""
import os

import pytest
import yaml

import util.conf_util  # noqa: F401  registers the !!python/regexp YAML constructor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@pytest.fixture(scope="module")
def product_types():
    with open(os.path.join(REPO_ROOT, "conf", "settings.yaml")) as fh:
        return yaml.safe_load(fh)["PRODUCT_TYPES"]


def test_l2_cslc_s1_dataset_time_range_is_the_acquisition_time(product_types):
    assert product_types["L2_CSLC_S1"]["Dataset_Keys"] == {
        "starttime": "acquisition_ts",
        "endtime": "acquisition_ts",
    }


def test_l2_cslc_s1_pattern_captures_the_acquisition_time(product_types):
    groups = product_types["L2_CSLC_S1"]["Pattern"].groupindex
    assert "acquisition_ts" in groups
    assert "acquisition_ts" in product_types["L2_CSLC_S1"]["Configuration"]["Date_Time_Keys"]
