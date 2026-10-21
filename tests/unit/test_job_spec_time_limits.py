"""Every job spec states a hard time limit that comes after its soft time limit.

celery raises SoftTimeLimitExceeded in the job worker at soft_time_limit and kills the
worker's pool child at time_limit. The time between the two is what the worker has to stop
the job's containers, triage the work dir and record the failure. A spec whose time_limit
is at or below its soft_time_limit asks for no time at all; HySDS then moves the hard limit
to soft_time_limit + HARD_TIME_LIMIT_GAP on its own, so the job runs with limits the spec
does not show.
"""
import glob
import json
import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _job_specs():
    return sorted(glob.glob(os.path.join(REPO_ROOT, "docker", "job-spec.json.*")))


@pytest.mark.parametrize("job_spec", _job_specs(), ids=os.path.basename)
def test_hard_limit_comes_after_the_soft_limit(job_spec):
    with open(job_spec) as f:
        spec = json.load(f)
    soft, hard = spec.get("soft_time_limit"), spec.get("time_limit")

    assert isinstance(soft, int) and soft > 0, f"soft_time_limit is {soft!r}"
    assert isinstance(hard, int) and hard > soft, (
        f"time_limit {hard!r} does not leave any time after soft_time_limit {soft}")
