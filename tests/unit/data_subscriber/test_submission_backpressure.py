import logging
from unittest.mock import MagicMock, patch

from data_subscriber.submission_backpressure import SubmissionBackpressure

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
