import logging
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_subscriber.parser import create_parser
from data_subscriber.submission_backpressure import SubmissionBackpressure

REPO = Path(__file__).parents[3]

logger = logging.getLogger(__name__)


def test_wait_returns_immediately_when_queue_is_shallow():
    bp = SubmissionBackpressure(max_pending=100, logger=logger, queue="jobs_processed")
    bp._read_depth = MagicMock(return_value=100)

    with patch("data_subscriber.submission_backpressure.time.sleep") as sleep:
        bp.wait()

    sleep.assert_not_called()
    assert bp.waited_s == 0


def test_wait_holds_until_queue_drains():
    bp = SubmissionBackpressure(max_pending=100, logger=logger, check_interval_s=0, queue="jobs_processed")
    bp._read_depth = MagicMock(side_effect=[5000, 900, 101, 100])

    with patch("data_subscriber.submission_backpressure.time.sleep") as sleep:
        bp.wait()

    assert bp._read_depth.call_count == 4
    assert sleep.call_count == 3


def test_depth_is_read_at_most_once_per_interval():
    bp = SubmissionBackpressure(max_pending=100, logger=logger, check_interval_s=3600, queue="jobs_processed")
    bp._read_depth = MagicMock(return_value=0)

    for _ in range(50):
        bp.wait()

    assert bp._read_depth.call_count == 1


def test_unreadable_depth_does_not_block():
    bp = SubmissionBackpressure(max_pending=100, logger=logger, queue="jobs_processed")
    bp._read_depth = MagicMock(side_effect=ConnectionError("broker down"))

    with patch("data_subscriber.submission_backpressure.time.sleep") as sleep:
        bp.wait()

    sleep.assert_not_called()


@pytest.mark.parametrize("subcommand", ["query", "full"])
def test_operator_runs_hold_submissions_by_default(subcommand):
    assert create_parser().parse_args([subcommand, "-c", "HLSL30"]).max_pending_submissions == 500
    assert create_parser().parse_args(
        [subcommand, "-c", "HLSL30", "--max-pending-submissions=0"]).max_pending_submissions == 0


def test_query_jobs_on_workers_keep_backpressure_off():
    """Query jobs run on workers have only been run without backpressure, and a held forward job
    could run into its time limit, so their wrappers pass 0. It comes before the job's own
    arguments, so a job can still ask for backpressure."""

    scripts = sorted(REPO.glob("data_subscriber/*/*_query*.sh"))
    invocations = [(script, line) for script in scripts for line in script.read_text().splitlines()
                   if "daac_data_subscriber.py query" in line and not line.lstrip().startswith("#")]

    assert len({script for script, _ in invocations}) >= 10
    for script, line in invocations:
        assert re.search(r"--max-pending-submissions=0 \$(\*|\{\*\})", line), f"{script.name}: {line}"

    pinned_then_job_args = ["query", "-c", "HLSL30", "--max-pending-submissions=0", "--max-pending-submissions=200"]
    assert create_parser().parse_args(pinned_then_job_args).max_pending_submissions == 200
