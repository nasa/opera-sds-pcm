import time
from threading import Lock


def _celery_app():
    # imported lazily so importing the query module does not require a HySDS install
    from hysds.celery import app
    return app


class SubmissionBackpressure:
    """Holds job submissions while the orchestrator's intake queue is deeper than max_pending.

    Every submitted job waits in the jobs_processed queue until the orchestrator dedups it and
    writes its job-queued status, and it is invisible in Figaro until then. Submitting faster than
    the orchestrator drains only grows that queue, so this keeps it bounded instead."""

    def __init__(self, max_pending, logger, check_interval_s=2.0, queue=None):
        self._max_pending = max_pending
        self._logger = logger
        self._check_interval_s = check_interval_s
        self._queue = queue or _celery_app().conf.get("JOBS_PROCESSED_QUEUE", "jobs_processed")
        self._lock = Lock()
        self._depth = 0
        self._checked_at = None
        self.waited_s = 0.0

    def _read_depth(self):
        with _celery_app().connection_for_read() as conn:
            return conn.default_channel.queue_declare(queue=self._queue, passive=True).message_count

    def wait(self):
        """Blocks until the queue is at or below max_pending. Reads the depth at most once per interval."""
        with self._lock:
            stalled_since = None

            while True:
                now = time.monotonic()
                if self._checked_at is None or now - self._checked_at >= self._check_interval_s:
                    try:
                        self._depth = self._read_depth()
                    except Exception as e:
                        self._logger.warning(f"Could not read {self._queue} depth, not throttling: {e}")
                        self._depth = 0
                    self._checked_at = now

                if self._depth <= self._max_pending:
                    if stalled_since is not None:
                        self._logger.info(f"{self._queue} depth {self._depth:,}; resuming submissions after "
                                          f"{time.monotonic() - stalled_since:.0f}s")
                    return

                if stalled_since is None:
                    stalled_since = time.monotonic()
                    self._logger.info(f"{self._queue} depth {self._depth:,} > {self._max_pending:,}; "
                                      f"holding submissions")

                time.sleep(self._check_interval_s)
                self.waited_s += self._check_interval_s
