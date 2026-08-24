"""Sentinel-1D product identifiers against the shipped filename patterns.

An S1-derived product carries its sensor in the filename. If any of the three
independent pattern sets -- the PCM settings, the HySDS dataset definitions, or
a PGE's input regexes -- stops at S1C, S1D products fail to extract metadata,
publish, or localize, depending on which one lags.
"""
import json
import os
import re

import pytest
import yaml

import util.conf_util  # registers the !!python/regexp YAML constructor

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

CSLC = "OPERA_L2_CSLC-S1_T064-135518-IW1_20260501T015035Z_20260807T212944Z_S1D_VV_v1.1"
CSLC_STATIC = "OPERA_L2_CSLC-S1-STATIC_T064-135524-IW2_20140101_S1D_v1.0"
RTC = "OPERA_L2_RTC-S1_T069-147174-IW3_20260504T104521Z_20260804T203850Z_S1D_30_v1.0"
DISP_STATIC = "OPERA_L3_DISP-S1-STATIC_F11115_20140403_S1D_v1.0"


@pytest.fixture(scope="module")
def settings():
    with open(os.path.join(REPO_ROOT, "conf", "settings.yaml")) as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def datasets():
    with open(os.path.join(REPO_ROOT, "conf", "sds", "files", "datasets.json")) as fh:
        return {entry["type"]: entry for entry in json.load(fh)["datasets"]}


@pytest.mark.parametrize("product_type, product_id", [
    ("L2_CSLC_S1", f"{CSLC}.h5"),
    ("L2_CSLC_S1_STATIC", f"{CSLC_STATIC}.h5"),
    ("L2_RTC_S1", f"{RTC}.h5"),
    ("L3_DISP_S1_STATIC", f"{DISP_STATIC}_dem.tif"),
])
def test_settings_patterns_match_s1d_products(settings, product_type, product_id):
    pattern = settings["PRODUCT_TYPES"][product_type]["Pattern"]
    match = pattern.match(product_id)
    assert match, f"{product_type} pattern rejected {product_id}"
    assert match.groupdict()["sensor"] == "S1D"


@pytest.mark.parametrize("dataset_type, product_id", [
    ("L2_CSLC_S1", CSLC),
    ("L2_CSLC_S1_STATIC", CSLC_STATIC),
    ("L2_RTC_S1", RTC),
    ("L3_DISP_S1_STATIC", DISP_STATIC),
])
def test_dataset_match_patterns_accept_s1d_products(datasets, dataset_type, product_id):
    match = re.match(datasets[dataset_type]["match_pattern"], product_id)
    assert match, f"{dataset_type} match_pattern rejected {product_id}"
    assert match.groupdict()["sensor"] == "S1D"


@pytest.mark.parametrize("pge_config, product_id", [
    ("PGE_L3_DISP_S1.yaml", CSLC),
    ("PGE_L3_DISP_S1_STATIC.yaml", CSLC_STATIC),
])
def test_disp_s1_pge_accepts_s1d_inputs(pge_config, product_id):
    """DISP-S1 localizes its input CSLCs by matching these regexes, so an S1D
    CSLC that these reject cannot be staged for a DISP-S1 job."""
    path = os.path.join(REPO_ROOT, "opera_chimera", "configs", "pge_configs", pge_config)
    with open(path) as fh:
        config = yaml.safe_load(fh)

    regexes = config["input_file_base_name_regexes"]
    assert any(re.match(regex, product_id) for regex in regexes), \
        f"{pge_config} input regexes rejected {product_id}"
