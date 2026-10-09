"""Every GRQ user rule searches only the indices its dataset can land in.

HySDS evaluates each enabled rule against every newly ingested dataset. A rule without
an index_pattern is searched against the whole grq alias -- every shard of every product
index -- so each ingest costs one alias-wide search per enabled rule. A pattern that
misses the dataset's index is worse: the rule quietly never matches and nothing is
triggered, with no error anywhere.

Index names follow grq_<version>_<dataset>-<YYYY.MM> (lowercased; the DIST-S1 state
configs have no month suffix). The version varies by producer and release, so patterns
wildcard it. The "-" before the month keeps a pattern from also matching a longer
dataset name that shares its prefix, e.g. l2_cslc_s1 vs l2_cslc_s1_static.
"""
import fnmatch
import json
import os
import re

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RULE_FILES = [
    os.path.join(REPO_ROOT, "conf", "sds", "rules", "user_rules.json"),
    os.path.join(REPO_ROOT, "conf", "sds", "rules", "user_rules-cnm.json.tmpl"),
]

# Index names each rule dataset is written to. Names seen on OPS FWD/POP1 where the
# dataset exists there; otherwise as the producer builds them.
INDEX_NAMES = {
    "L1_S1_SLC": ["grq_1_l1_s1_slc-2026.09"],
    "L2_HLS_L30": ["grq_v2.0_l2_hls_l30-2026.09"],
    "L2_HLS_S30": ["grq_v2.0_l2_hls_s30-2026.09"],
    # PGE output and catalog-ingested CSLCs
    "L2_CSLC_S1": ["grq_v1.1_l2_cslc_s1-2026.09", "grq_1_l2_cslc_s1-2026.09"],
    "L2_CSLC_S1_STATIC": ["grq_v1.0_l2_cslc_s1_static-2026.09"],
    "L2_CSLC_S1_COMPRESSED": ["grq_1_l2_cslc_s1_compressed-2026.09"],
    "L2_RTC_S1": ["grq_v1.0_l2_rtc_s1-2026.09"],
    "L2_RTC_S1_STATIC": ["grq_v1.0_l2_rtc_s1_static-2026.09"],
    "L2_GCOV_NI": ["grq_1_l2_gcov_ni-2026.09"],
    "L2_GCOV_NI_BATCH": ["grq_1_l2_gcov_ni_batch-2026.09"],
    "L2_GSLC_NI": ["grq_1_l2_gslc_ni-2026.09"],
    "L2_GSLC_COMPRESSED": ["grq_1_l2_gslc_compressed-2026.09"],
    "L3_DSWx_HLS": ["grq_v1.1_l3_dswx_hls-2026.09"],
    "L3_DSWx_S1": ["grq_v1.0_l3_dswx_s1-2026.09"],
    "L3_DSWx_NI": ["grq_v1.0_l3_dswx_ni-2026.09"],
    "L3_DISP_S1": ["grq_v1.0_l3_disp_s1-2026.09"],
    "L3_DISP_S1_STATIC": ["grq_v1.0_l3_disp_s1_static-2026.07"],
    "L3_DISP_NI": ["grq_v1.0_l3_disp_ni-2026.09"],
    "L3_DIST_S1": ["grq_v1.0_l3_dist_s1-2026.08"],
    "L4_TROPO": ["grq_v1.0_l4_tropo-2026.09"],
    "L4_CAL_DISP": ["grq_v1.0_l4_cal_disp-2026.09"],
    "cslc_s1-cycle-state-config": ["grq_1_cslc_s1-cycle-state-config-2026.08"],
    "disp_s1-kcycle-state-config": ["grq_1_disp_s1-kcycle-state-config-2026.09"],
    # one marker per DISP-S1 compressed CSLC set
    "disp_s1-ccslc-set": ["grq_1_disp_s1-ccslc-set-2026.10"],
    "DIST_S1-STATE-CONFIG": ["grq_1.0_dist_s1-state-config"],
    "DIST_S1-FWD-STATE-CONFIG": ["grq_1.0_dist_s1-fwd-state-config"],
    # regular and expired MGRS-set state configs share one dataset type
    "DSWX-NI-STATE-CONFIG": [
        "grq_1_dswx_ni-state-config-2026.09",
        "grq_1_dswx_ni-expired-state-config-2026.09",
    ],
    "area_of_interest": ["grq_2.0_area_of_interest"],
    "triaged_job": ["grq_v3.1.1_triaged_job"],
}


def _grq_rules():
    rules = []
    for path in RULE_FILES:
        with open(path) as f:
            rules.extend(json.load(f)["grq"])
    return rules


def _dataset_filter(rule):
    values = set(re.findall(r'"dataset(?:_type)?\.keyword"\s*:\s*"([^"]+)"', rule["query_string"]))
    assert len(values) == 1, f"{rule['rule_name']} should filter on exactly one dataset: {values}"
    return values.pop()


def _matches(index_pattern, index):
    return any(fnmatch.fnmatchcase(index, p.strip()) for p in index_pattern.split(","))


GRQ_RULES = _grq_rules()


@pytest.mark.parametrize("rule", GRQ_RULES, ids=lambda r: r["rule_name"])
def test_index_pattern_is_set_and_wildcarded(rule):
    index_pattern = rule.get("index_pattern", "")
    assert index_pattern, f"{rule['rule_name']} has no index_pattern, so it searches the whole grq alias"
    for p in index_pattern.split(","):
        # A concrete name that doesn't exist yet fails the rule's search outright;
        # a wildcard that matches nothing just returns no hits.
        assert "*" in p and set(p) != {"*"}, f"{rule['rule_name']}: {p!r}"


@pytest.mark.parametrize("rule", GRQ_RULES, ids=lambda r: r["rule_name"])
def test_index_pattern_covers_its_dataset_and_nothing_else(rule):
    dataset = _dataset_filter(rule)
    assert dataset in INDEX_NAMES, f"add the index names for {dataset} to INDEX_NAMES"

    for index in INDEX_NAMES[dataset]:
        assert _matches(rule["index_pattern"], index), (
            f"{rule['rule_name']} would never see {dataset} datasets in {index}")

    for other, indices in INDEX_NAMES.items():
        if other == dataset:
            continue
        for index in indices:
            assert not _matches(rule["index_pattern"], index), (
                f"{rule['rule_name']} ({dataset}) also searches {other} index {index}")
