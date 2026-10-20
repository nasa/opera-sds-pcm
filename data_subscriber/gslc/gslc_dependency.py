"""Compressed-GSLC prerequisites of a DISP-NI historical k-set.

Each historical DISP-NI job writes one compressed GSLC per frame, summarizing its k dates, and
every later k-set of the frame consumes the compressed GSLCs of the k-sets before it, up to m - 1
of them (m counts the current k-set too, as in DISP-S1). A k-set can therefore only be submitted
once those compressed GSLCs are cataloged in GRQ.

A compressed GSLC is identified by its frame and the last sensing date of the k-set that produced
it (its reference date equals that date under the SAS's last_per_ministack plan), so one lookup
per prerequisite replaces DISP-S1's per-burst ccslc_m_index lookups.
"""

from datetime import datetime, timedelta

from opera_commons.logger import get_logger

COMPRESSED_GSLC_ES_INDEX_PATTERNS = "grq_*_l2_gslc_compressed*"


class GSLCDependency:
    def __init__(self, k: int, m: int, frame_db: dict, logger=None):
        self.k = k
        self.m = m
        self.frame_db = frame_db
        self.logger = logger or get_logger()

    def m_for_position(self, position: int) -> int:
        """m to process the k-set starting at this position with: 1 for the first k-set, ramping up to m."""

        # TODO(OPERA-2675): ICS 2.6.1 selects compressed GSLCs within the frame's current reference
        #  epoch. Positions count from the frame's first date until ADT says how the ramp and the
        #  ministack boundaries behave at an epoch change.
        return min(position // self.k, self.m - 1) + 1

    def required_compressed_gslc_last_dates(self, frame_id: int, position: int) -> list[datetime]:
        """Last sensing datetime of each preceding k-set whose compressed GSLC this k-set needs, newest first."""

        sensing_datetimes = self.frame_db[frame_id].sensing_datetimes
        return [sensing_datetimes[position - 1 - j * self.k] for j in range(self.m_for_position(position) - 1)]

    def compressed_gslc_satisfied(self, frame_id: int, position: int, eu) -> bool:
        """Whether every compressed GSLC the k-set starting at this position needs is cataloged."""

        return self.get_dependent_compressed_gslcs(frame_id, position, eu) is not None

    def get_dependent_compressed_gslcs(self, frame_id: int, position: int, eu) -> list[dict] | None:
        """The GRQ docs of the compressed GSLCs the k-set starting at this position needs, newest first.

        Returns None if any of them is not cataloged yet."""

        compressed_gslcs = []
        for last_datetime in self.required_compressed_gslc_last_dates(frame_id, position):
            # The extractor stores last_date_time as an ISO datetime at midnight, so match the whole day
            day = datetime.combine(last_datetime.date(), datetime.min.time())
            hits = eu.query(
                index=COMPRESSED_GSLC_ES_INDEX_PATTERNS,
                body={"query": {"bool": {"must": [
                    {"term": {"metadata.frame_id": frame_id}},
                    {"range": {"metadata.last_date_time": {"gte": day.isoformat(),
                                                           "lt": (day + timedelta(days=1)).isoformat()}}}
                ]}}})

            if not hits:
                self.logger.info("Compressed GSLC for frame %s with last date %s was not found in GRQ ES",
                                 frame_id, day.strftime("%Y%m%d"))
                return None

            compressed_gslcs.append(hits[0])

        return compressed_gslcs
