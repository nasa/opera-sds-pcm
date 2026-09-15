"""Every job spec recommends a queue that the cluster terraform deploys workers for.

A job submitted with default arguments lands on the first recommended queue. If no
worker group listens there, the job is accepted and then waits forever with nothing in
any log -- which is how cslc_catalog_ingest jobs behaved until the job spec was pointed
at the CSLC download workers.
"""
import glob
import json
import os
import re

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
QUEUES_TF = os.path.join(REPO_ROOT, "cluster_provisioning", "modules", "common", "variables.tf")

# Recommended queues the common module deliberately does not deploy.
NOT_DEPLOYED_BY_COMMON = {
    # Development-only DIST-S1 state-config test job; no venue runs workers for it.
    "opera-job_worker-dist_s1_state_config",
}


def _deployed_queues():
    with open(QUEUES_TF) as f:
        return set(re.findall(r'"name"\s*=\s*"([^"]+)"', f.read()))


def _job_specs():
    return sorted(glob.glob(os.path.join(REPO_ROOT, "docker", "job-spec.json.*")))


@pytest.mark.parametrize("job_spec", _job_specs(), ids=os.path.basename)
def test_recommended_queues_are_deployed(job_spec):
    with open(job_spec) as f:
        recommended = json.load(f).get("recommended-queues", [])
    deployed = _deployed_queues()

    for queue in recommended:
        if queue in NOT_DEPLOYED_BY_COMMON:
            continue
        assert queue in deployed, (
            f"{os.path.basename(job_spec)} recommends {queue}, "
            f"which no worker group in modules/common/variables.tf deploys")


def test_cslc_catalog_ingest_runs_on_the_cslc_download_workers():
    with open(os.path.join(REPO_ROOT, "docker", "job-spec.json.cslc_catalog_ingest")) as f:
        assert json.load(f)["recommended-queues"] == ["opera-job_worker-cslc_data_download"]
