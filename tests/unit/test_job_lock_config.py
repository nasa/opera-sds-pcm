"""Job locking is enabled for every job worker, and its lock outlasts every job.

HySDS job locking takes a redis lock keyed on the job's payload id and renews it from a
heartbeat thread. Two things silently defeat it:

- a verdi celeryconfig template without the ENABLE_JOB_LOCKING setting, because the
  worker then falls back to the hysds default of False whatever ~/.sds/config says;
- a lock whose extension budget (JOB_LOCK_MAX_EXTENSIONS x JOB_LOCK_HEARTBEAT_INTERVAL)
  is shorter than a job's time_limit, because the heartbeat stops once the extensions
  run out and the lock lapses while the job is still running.
"""
import glob
import json
import os
import re

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FILES_DIR = os.path.join(REPO_ROOT, "conf", "sds", "files")
MOZART_TF = os.path.join(REPO_ROOT, "cluster_provisioning", "modules", "common", "mozart.tf")

# The OPERA-owned verdi celeryconfig overrides. mozart and factotum render from the
# upstream hysds template, which carries the same settings.
VERDI_TEMPLATES = ["celeryconfig.py.tmpl.asg", "celeryconfig.py.tmpl.private_verdi"]


def _read(path):
    with open(path) as f:
        return f.read()


def _int_setting(text, name):
    m = re.search(rf"^{name}\s*=\s*(\d+)\s*$", text, re.M)
    assert m, f"{name} is not set to an integer literal"
    return int(m.group(1))


def _longest_time_limit():
    longest, where = 0, None
    for path in glob.glob(os.path.join(REPO_ROOT, "docker", "job-spec.json.*")):
        with open(path) as f:
            spec = json.load(f)
        for key in ("time_limit", "soft_time_limit"):
            value = spec.get(key)
            if isinstance(value, int) and value > longest:
                longest, where = value, f"{os.path.basename(path)} {key}"
    return longest, where


@pytest.mark.parametrize("template", VERDI_TEMPLATES)
def test_enable_job_locking_comes_from_sds_config(template):
    text = _read(os.path.join(FILES_DIR, template))
    assert re.search(
        r"^ENABLE_JOB_LOCKING\s*=\s*\{\{\s*ENABLE_JOB_LOCKING\s*\|\s*default\(false\)\s*\}\}\s*$",
        text, re.M), f"{template} does not render ENABLE_JOB_LOCKING from ~/.sds/config"


@pytest.mark.parametrize("template", VERDI_TEMPLATES)
def test_enable_job_locking_renders_as_a_python_bool(template):
    jinja2 = pytest.importorskip("jinja2")
    line = next(l for l in _read(os.path.join(FILES_DIR, template)).splitlines()
                if l.startswith("ENABLE_JOB_LOCKING"))
    render = jinja2.Environment().from_string(line).render
    assert render(ENABLE_JOB_LOCKING=True) == "ENABLE_JOB_LOCKING = True"
    assert render() == "ENABLE_JOB_LOCKING = False"


@pytest.mark.parametrize("template", VERDI_TEMPLATES)
def test_lock_extensions_outlast_the_longest_job(template):
    text = _read(os.path.join(FILES_DIR, template))
    budget = (_int_setting(text, "JOB_LOCK_MAX_EXTENSIONS")
              * _int_setting(text, "JOB_LOCK_HEARTBEAT_INTERVAL"))
    longest, where = _longest_time_limit()
    assert longest > 0
    assert budget > longest, (
        f"{template}: the lock can be extended for {budget} s but {where} allows "
        f"{longest} s; raise JOB_LOCK_MAX_EXTENSIONS")


def test_cluster_provisioning_enables_job_locking():
    assert re.search(r"echo ENABLE_JOB_LOCKING: true >> ~/.sds/config", _read(MOZART_TF)), \
        "mozart.tf no longer writes ENABLE_JOB_LOCKING: true to ~/.sds/config"
