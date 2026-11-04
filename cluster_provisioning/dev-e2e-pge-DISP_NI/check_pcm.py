#!/usr/bin/env python
"""Assert on the result files run_smoke_test.sh leaves, one test per frame, k-set and check.

The frames and the number of k-sets are smoke test settings, so the tests are generated from the
result files the run wrote:
  /tmp/disp_ni_hist_f<frame>_k_set_<i>.txt      the frame's walk reached k-set i
  /tmp/datasets_disp_ni_f<frame>_k_set_<i>.txt  the frame's k-set i products were published
  /tmp/disp_ni_hist_f<frame>_finished.txt       the batch proc finished with every frame done
"""
import glob
import re
from collections import defaultdict

import pytest

SUCCESS_RE = re.compile(r"^SUCCESS", re.MULTILINE)
ERROR_RE = re.compile(r"^ERROR", re.MULTILINE)
RESULT_RE = re.compile(r"_f(?P<frame>\d+)_(k_set_(?P<k_set>\d+)|finished)\.txt$")


def sort_key(path):
    match = RESULT_RE.search(path)
    k_set = int(match.group("k_set")) if match.group("k_set") else float("inf")
    return int(match.group("frame")), k_set, "datasets" in path


RESULT_FILES = sorted(glob.glob("/tmp/disp_ni_hist_f*.txt") + glob.glob("/tmp/datasets_disp_ni_f*.txt"),
                      key=sort_key)


def test_the_run_left_results_for_at_least_two_k_sets_of_each_frame_and_its_finish():
    k_sets = defaultdict(set)
    finished = set()
    for path in RESULT_FILES:
        match = RESULT_RE.search(path)
        if match.group("k_set"):
            k_sets[match.group("frame")].add(int(match.group("k_set")))
        else:
            finished.add(match.group("frame"))

    assert k_sets, "no result files"
    assert all(found >= {1, 2} for found in k_sets.values()), dict(k_sets)
    assert finished == set(k_sets), (sorted(finished), sorted(k_sets))


# The ids are listed rather than computed by a function, which pytest would also call on the placeholder
# it uses when there are no result files at all
@pytest.mark.parametrize("check_file", RESULT_FILES,
                         ids=[path.rsplit("/", 1)[-1].removesuffix(".txt") for path in RESULT_FILES])
def test_check_succeeded(check_file):
    with open(check_file) as f:
        res = f.read()
    assert SUCCESS_RE.search(res) is not None, res
    assert ERROR_RE.search(res) is None, res
