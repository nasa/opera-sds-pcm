from dataclasses import dataclass
from datetime import datetime


@dataclass
class TimeRange:
    acq_ts_start: datetime
    acq_ts_end: datetime

@dataclass
class SummaryRow:
    tile_id: str
    num_sc: int
    num_dist_s1: int
    num_sc_skipped: int
    num_dist_s1_missing: int

@dataclass
class DetailedRow:
    name: str
    status: str
    was_skipped: bool
