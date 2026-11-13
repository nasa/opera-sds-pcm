"""Dataset user-rule evaluations are pinned to the index the ingest wrote.

With USER_RULES_SEND_INDEX on, mozart's orchestrator (hysds >= 3.3.6) passes each
dataset evaluation the concrete index its ingest wrote, so a dataset re-ingested under
an id that already exists in another monthly index is evaluated on the new copy. An
evaluator older than hysds 3.3.6 rejects that extra kwarg and the dataset's triggers are
lost, so the setting is only safe while every hysds_release default is v6.4.7 or later.
"""
import glob
import os
import re

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FILES_DIR = os.path.join(REPO_ROOT, "conf", "sds", "files")
PROVISIONING = os.path.join(REPO_ROOT, "cluster_provisioning")
MOZART_TF = os.path.join(PROVISIONING, "modules", "common", "mozart.tf")

# The OPERA-owned verdi celeryconfig overrides. mozart and factotum render from the
# upstream hysds template, which carries the same settings.
VERDI_TEMPLATES = ["celeryconfig.py.tmpl.asg", "celeryconfig.py.tmpl.private_verdi"]

FIRST_RELEASE_WITH_SEND_INDEX = (6, 4, 7)


def _read(path):
    with open(path) as f:
        return f.read()


def _render_line(template, name, **ctx):
    jinja2 = pytest.importorskip("jinja2")
    line = next(l for l in _read(os.path.join(FILES_DIR, template)).splitlines()
                if l.startswith(name))
    return jinja2.Environment().from_string(line).render(**ctx)


@pytest.mark.parametrize("template", VERDI_TEMPLATES)
def test_send_index_renders_from_sds_config_as_a_python_bool(template):
    assert _render_line(template, "USER_RULES_SEND_INDEX", USER_RULES_SEND_INDEX=True) == \
        "USER_RULES_SEND_INDEX = True"
    assert _render_line(template, "USER_RULES_SEND_INDEX") == "USER_RULES_SEND_INDEX = False"


@pytest.mark.parametrize("template", VERDI_TEMPLATES)
def test_dataset_index_prefix_defaults_to_grq(template):
    assert _render_line(template, "DATASET_INDEX_PREFIX") == 'DATASET_INDEX_PREFIX = "grq"'
    assert _render_line(template, "DATASET_INDEX_PREFIX", DATASET_INDEX_PREFIX="other") == \
        'DATASET_INDEX_PREFIX = "other"'


def test_cluster_provisioning_enables_send_index():
    assert re.search(r"echo USER_RULES_SEND_INDEX: true >> ~/.sds/config", _read(MOZART_TF)), \
        "mozart.tf no longer writes USER_RULES_SEND_INDEX: true to ~/.sds/config"


def _hysds_release_defaults():
    """Every hysds_release default and tfvars value under cluster_provisioning."""
    found = []
    for path in glob.glob(os.path.join(PROVISIONING, "**", "*.tf"), recursive=True):
        text = _read(path)
        for m in re.finditer(r'variable\s+"hysds_release"\s*\{[^}]*?default\s*=\s*"([^"]+)"',
                             text, re.S):
            found.append((os.path.relpath(path, REPO_ROOT), m.group(1)))
    for path in glob.glob(os.path.join(PROVISIONING, "**", "*.tfvars"), recursive=True):
        for m in re.finditer(r'^\s*hysds_release\s*=\s*"([^"]+)"', _read(path), re.M):
            found.append((os.path.relpath(path, REPO_ROOT), m.group(1)))
    return found


def test_every_hysds_release_default_supports_send_index():
    defaults = _hysds_release_defaults()
    assert defaults, "found no hysds_release defaults under cluster_provisioning"
    for where, release in defaults:
        m = re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)", release)
        assert m, f"{where}: unexpected hysds_release {release!r}"
        assert tuple(int(x) for x in m.groups()) >= FIRST_RELEASE_WITH_SEND_INDEX, (
            f"{where} deploys HySDS {release}, whose user-rule evaluators reject the index "
            "that USER_RULES_SEND_INDEX makes mozart send")
