#!/usr/bin/env python
"""Assert on the result files run_smoke_test.sh leaves, one test per k-set and check.

The number of k-sets is a smoke test setting, so the tests are generated from the result files the
run wrote: /tmp/disp_ni_hist_k_set_<i>.txt (the walk reached k-set i) and
/tmp/datasets_disp_ni_k_set_<i>.txt (k-set i's products were published).
"""
import glob
import re

import pytest

SUCCESS_RE = re.compile(r"^SUCCESS", re.MULTILINE)
ERROR_RE = re.compile(r"^ERROR", re.MULTILINE)


def k_set_of(path):
    return int(re.search(r"_k_set_(\d+)\.txt$", path).group(1))


RESULT_FILES = sorted(glob.glob("/tmp/disp_ni_hist_k_set_*.txt") + glob.glob("/tmp/datasets_disp_ni_k_set_*.txt"),
                      key=lambda path: (k_set_of(path), "datasets" in path))


def test_the_run_left_results_for_at_least_two_k_sets():
    assert {k_set_of(path) for path in RESULT_FILES} >= {1, 2}


@pytest.mark.parametrize("check_file", RESULT_FILES, ids=lambda path: path.rsplit("/", 1)[-1].removesuffix(".txt"))
def test_check_succeeded(check_file):
    with open(check_file) as f:
        res = f.read()
    assert SUCCESS_RE.search(res) is not None, res
    assert ERROR_RE.search(res) is None, res
