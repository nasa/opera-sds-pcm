"""The PGE container carries the labels HySDS stamps on the job's own container.

The wrapper starts the PGE with `docker run` through the host's docker socket, so the PGE
container is a sibling of the job's container on the host daemon rather than a child of it.
When the job is revoked or times out, HySDS stops every container labelled for the job's
work dir. The labels reach the wrapper through _docker_params.json; a HySDS release that
does not label containers leaves them out, and the command must then be what it was before.
"""
import json
import os
import shlex

import pytest

# the wrapper imports chimera and hysds, which ship on the cluster images only
pytest.importorskip("chimera")
pytest.importorskip("hysds")

from wrapper import opera_pge_wrapper  # noqa: E402

PGE_IMAGE = "opera_pge/disp_s1:3.0.7"
JOB_DIR = "/data/work/jobs/2026/10/01/12/00/job-WF-SCIFLO_L3_DISP_S1-frame-31241-20261001T120000.000000Z"
HYSDS_LABELS = {
    "hysds.job_id": "job-WF-SCIFLO_L3_DISP_S1-frame-31241-20261001T120000.000000Z",
    "hysds.task_id": "9b8e18f9-15a2-4f98-82d0-79f360eaece5",
    "hysds.job_dir": JOB_DIR,
}


def _context():
    return {
        "job_specification": {
            "dependency_images": [{"container_image_name": PGE_IMAGE}],
            "params": [
                {"name": "container_home", "value": "/home/conda"},
                {"name": "container_working_dir", "value": "/home/conda"},
            ],
        }
    }


def _exec_pge_command(tmp_path, image_params):
    """Run exec_pge_command against a work dir holding these docker params."""
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    docker_params = {PGE_IMAGE: {"uid": 1000, "gid": 1000, **image_params}}
    (work_dir / "_docker_params.json").write_text(json.dumps(docker_params))

    opera_pge_wrapper.exec_pge_command(
        context=_context(),
        work_dir=str(work_dir),
        input_dir=str(work_dir / "pge_input_dir"),
        runconfig_dir=str(work_dir / "pge_runconfig_dir"),
        output_dir=str(work_dir / "pge_output_dir"),
        scratch_dir=str(work_dir / "pge_scratch_dir"),
    )


def _pge_command(tmp_path, monkeypatch, image_params):
    """The command line exec_pge_command would have executed for these docker params."""
    commands = []
    monkeypatch.setattr(opera_pge_wrapper, "call_noerr", lambda cmd, cwd: commands.append(cmd))

    _exec_pge_command(tmp_path, image_params)

    assert len(commands) == 1
    return commands[0]


def _labels(tokens):
    """The values of every --label option in a tokenized docker command."""
    return [tokens[i + 1] for i, token in enumerate(tokens) if token == "--label"]


def test_pge_container_gets_the_job_labels(tmp_path, monkeypatch):
    tokens = shlex.split(_pge_command(tmp_path, monkeypatch, {"labels": HYSDS_LABELS}))

    assert _labels(tokens) == [f"{k}={v}" for k, v in HYSDS_LABELS.items()]


def test_labels_are_docker_options_not_pge_arguments(tmp_path, monkeypatch):
    """Anything after the image name is handed to the PGE's entrypoint."""
    tokens = shlex.split(_pge_command(tmp_path, monkeypatch, {"labels": HYSDS_LABELS}))

    image_at = tokens.index(PGE_IMAGE)
    assert all(i < image_at for i, token in enumerate(tokens) if token == "--label")
    assert tokens[image_at + 1:] == ["--file", "/home/conda/runconfig/RunConfig.yaml"]


@pytest.mark.parametrize("image_params", [{}, {"labels": {}}, {"labels": None}])
def test_no_labels_without_hysds_support(tmp_path, monkeypatch, image_params):
    """_docker_params.json from a HySDS release that does not label containers, or one
    with nothing to pass on."""
    tokens = shlex.split(_pge_command(tmp_path, monkeypatch, image_params))

    assert "--label" not in tokens
    assert tokens[:6] == ["docker", "run", "--init", "--rm", "-u", "1000:1000"]
    assert PGE_IMAGE in tokens


def test_labels_do_not_displace_runtime_options(tmp_path, monkeypatch):
    image_params = {"runtime_options": {"gpus": "all"}, "labels": HYSDS_LABELS}
    tokens = shlex.split(_pge_command(tmp_path, monkeypatch, image_params))

    assert tokens[tokens.index("--gpus") + 1] == "all"
    assert len(_labels(tokens)) == len(HYSDS_LABELS)


def test_label_values_survive_the_shell(tmp_path, monkeypatch):
    """The command runs through a shell, so a value must not be able to split into words
    or start a command of its own. Run it for real, against a stand-in docker that records
    the arguments it was given."""
    argv_file = tmp_path / "docker.argv"
    injected = tmp_path / "injected"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text('#!/bin/sh\nfor arg in "$@"; do printf \'%s\\n\' "$arg"; done > "$DOCKER_ARGV_FILE"\n')
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("DOCKER_ARGV_FILE", str(argv_file))
    # anything a broken command line creates by accident lands in the test's own dir
    monkeypatch.chdir(tmp_path)

    labels = {
        "hysds.job_id": f"job with spaces; touch {injected} $(touch {injected})",
        "hysds.job_dir": JOB_DIR,
    }
    _exec_pge_command(tmp_path, {"labels": labels})

    argv = argv_file.read_text().splitlines()
    assert _labels(argv) == [f"{k}={v}" for k, v in labels.items()]
    assert argv[argv.index(PGE_IMAGE) + 1:] == ["--file", "/home/conda/runconfig/RunConfig.yaml"]
    assert not injected.exists()
