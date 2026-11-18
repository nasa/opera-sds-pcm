"""DISP-NI frame database and NISAR GSLC naming helpers.

ADT's consistent GSLC database (opera-nisar-disp-consistent-gslc-<date>.json) lists, for each DISP-NI
frame, the GSLC mode and coverage it is processed in and the sensing times on which a GSLC in that
mode and coverage exists. It drives DISP-NI historical processing the way the consistent burst
database drives DISP-S1:

    {"metadata": {...},
     "data": {"26410": {"common_mode": "4005", "common_coverage": "P",
                        "sensing_time_list": ["2025-11-08T15:50:41", ...]}}}

A DISP-NI frame is one NISAR track/frame, numbered (track - 1) * 176 + (frame - 1). The database has no
pass direction, and none is needed: a NISAR frame number is only ever ascending or descending, so the
NISAR frame identifier is <track>_?_<frame>, a pattern that matches the A or D in GSLC filenames.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from fnmatch import fnmatchcase
from functools import cache

from opera_commons.logger import get_logger
from data_subscriber.cslc.disp_s1_phases import parse_sensing_time_list
from data_subscriber.cslc_utils import (expand_batch_proc_frames, first_k_set_on_or_after, localize_anc_json,
                                        straddles_start_date)
from util import datasets_json_util

logger = get_logger()

DISP_NI_CBDB_SETTINGS_FIELD = "DISP_NI_CBDB_S3PATH"
DEFAULT_DISP_NI_CBDB_NAME = "opera-nisar-disp-consistent-gslc.json"
CBDB_DATA_KEY = "data"
CBDB_MODE_KEY = "common_mode"
CBDB_COVERAGE_KEY = "common_coverage"
CBDB_SENSING_TIME_LIST_KEY = "sensing_time_list"

NISAR_FRAMES_PER_TRACK = 176

GSLC_DATASET_TYPE = "L2_GSLC_NI"

BATCH_PROC_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Co-polarized channel the SAS processes for the frequencyA polarization mode of a GSLC, which is the first
# two characters of its polarization code (DHDH -> DH). The selection rule is TBD in ICS 2.6.1; this matches
# the ADT sample data (4005_DHDH processed as frequencyA, HH).
GSLC_FREQUENCY = "frequencyA"
GSLC_CO_POLARIZATION = {"SH": "HH", "DH": "HH", "QP": "HH", "SV": "VV", "DV": "VV"}


@dataclass
class NisarFrame:
    """One DISP-NI frame of the frame database."""
    frame_number: int
    nisar_frames: set[str] = field(default_factory=set)            # e.g. {"151_?_011"}, see nisar_frame_of
    sensing_datetimes: list[datetime] = field(default_factory=list)  # sorted
    mode: str | None = None                                          # GSLC bandwidth mode, e.g. "4005"
    coverage: str | None = None                                      # GSLC coverage, "F" (full) or "P" (partial)

    def covers(self, nisar_frame: str) -> bool:
        """Whether a GSLC's NISAR frame (e.g. "151_A_011") is one of this frame's."""

        return any(fnmatchcase(nisar_frame, pattern) for pattern in self.nisar_frames)


def nisar_frame_of(frame_id: int) -> str:
    """The NISAR track/frame of a DISP-NI frame, with its pass direction left as a wildcard, e.g. 26410 -> 151_?_011."""

    track, frame = divmod(frame_id, NISAR_FRAMES_PER_TRACK)
    return f"{track + 1:03d}_?_{frame + 1:03d}"


@dataclass(frozen=True)
class GslcGranuleId:
    """The fields of a NISAR GSLC granule id that DISP-NI processing keys on."""
    cycle: int
    track: int
    direction: str
    nisar_frame: str          # <track>_<A|D>_<frame>, e.g. "151_A_011"
    mode: str                 # bandwidth mode, e.g. "4005"
    polarization: str         # e.g. "DHDH"
    sensing_start: datetime
    coverage: str             # "F" (full) or "P" (partial)

    @property
    def mode_polarization(self) -> str:
        return f"{self.mode}_{self.polarization}"


@cache
def localize_disp_ni_frame_db(settings_yaml_path=None) -> dict[int, NisarFrame]:
    """Download the DISP-NI frame database named in settings.yaml and parse it.

    Falls back to a local copy in the working directory, like localize_disp_frame_burst_hist()."""

    try:
        file = localize_anc_json(DISP_NI_CBDB_SETTINGS_FIELD, settings_yaml_path)
    except Exception:
        logger.warning(f"Could not download the DISP-NI frame database from settings.yaml field "
                       f"{DISP_NI_CBDB_SETTINGS_FIELD}. Attempting to use local copy named "
                       f"{DEFAULT_DISP_NI_CBDB_NAME}.")
        file = DEFAULT_DISP_NI_CBDB_NAME

    return process_disp_ni_frame_db(file)


@cache
def process_disp_ni_frame_db(file) -> dict[int, NisarFrame]:
    """Parse a DISP-NI frame database file into {frame_id: NisarFrame}."""

    with open(file) as f:
        data = json.load(f)[CBDB_DATA_KEY]

    frame_db = {}
    for frame_id, entry in data.items():
        sensing_datetimes, _ = parse_sensing_time_list(entry[CBDB_SENSING_TIME_LIST_KEY])
        frame_db[int(frame_id)] = NisarFrame(frame_number=int(frame_id),
                                             nisar_frames={nisar_frame_of(int(frame_id))},
                                             sensing_datetimes=sensing_datetimes,
                                             mode=entry[CBDB_MODE_KEY],
                                             coverage=entry[CBDB_COVERAGE_KEY])

    return frame_db


@cache
def _gslc_granule_regex() -> re.Pattern:
    return re.compile(datasets_json_util.DatasetsJson().get(GSLC_DATASET_TYPE)["match_pattern"])


def parse_gslc_granule_id(granule_id: str) -> GslcGranuleId:
    """Parse a NISAR GSLC granule id (or filename) with the L2_GSLC_NI pattern in datasets.json.

    Raises ValueError if the id does not match."""

    match = _gslc_granule_regex().match(granule_id.removesuffix(".h5"))
    if not match:
        raise ValueError(f"{granule_id} is not a NISAR GSLC granule id")

    return GslcGranuleId(
        cycle=int(match.group("cycle")),
        track=int(match.group("track")),
        direction=match.group("orbit_dir"),
        nisar_frame=f'{match.group("track")}_{match.group("orbit_dir")}_{match.group("nisar_frame")}',
        mode=match.group("mode"),
        polarization=match.group("pol"),
        sensing_start=datetime.strptime(match.group("sensing_start_date_time"), "%Y%m%dT%H%M%S"),
        coverage=match.group("coverage"),
    )


def nisar_cmr_attributes(nisar_frame: str, coverage: str) -> list[str]:
    """CMR attribute[] filters for the production granules of one NISAR track/frame (e.g. "151_?_011" or
    "151_A_011") with this coverage ("F" full or "P" partial), whatever their pass direction.

    NISAR granules carry these as additional attributes, so CMR selects them without a granule name pattern."""

    track, _, frame = nisar_frame.split("_")
    return [f"int,TRACK_NUMBER,{int(track)}",
            f"int,FRAME_NUMBER,{int(frame)}",
            f"string,FULL_FRAME,{'TRUE' if coverage == 'F' else 'FALSE'}",
            "string,PRODUCTION_PIPELINE,PR"]


def build_gslc_cmr_attributes(frame: NisarFrame) -> list[str]:
    """CMR attribute[] filters for the frame's GSLCs in its mode and coverage, any cycle and polarization.

    A bandwidth mode is the range bandwidths of frequencyA and frequencyB, two digits each (4005: 40 and 05)."""

    (nisar_frame,) = frame.nisar_frames
    return nisar_cmr_attributes(nisar_frame, frame.coverage) + \
        [f"int,FREQUENCY_A_RANGE_BANDWIDTH,{int(frame.mode[:2])}",
         f"int,FREQUENCY_B_RANGE_BANDWIDTH,{int(frame.mode[2:])}"]


def gslc_download_batch_id(frame_id: int, cycle: int) -> str:
    """One DISP-NI frame on one NISAR cycle, e.g. f26410_a4.

    Same shape as the DISP-S1 batch id, so cslc_utils.split_download_batch_id() splits it."""

    return f"f{frame_id}_a{cycle}"


def gslc_frequency_polarization(polarization_code: str) -> tuple[str, str]:
    """The (frequency, polarization) the SAS processes for a GSLC polarization code, e.g. DHDH -> (frequencyA, HH).

    Raises ValueError for a frequencyA mode with no co-polarized channel to process."""

    co_polarization = GSLC_CO_POLARIZATION.get(polarization_code[:2])
    if co_polarization is None:
        raise ValueError(f"GSLC polarization {polarization_code} has no frequencyA channel DISP-NI can process")

    return GSLC_FREQUENCY, co_polarization


def validate_batch_proc_start_date(proc: dict, frame_db: dict[int, NisarFrame]) -> bool | str:
    """Check that a gslc_query_hist batch proc's data_start_date leaves every frame able to run.

    run_disp_ni_historical_processing.py skips every k-set that starts before data_start_date, so the start
    date must not fall inside a k-set, whose later dates would be skipped with it. And unless m is 1, every
    k-set after a frame's first needs the compressed GSLCs of the k-sets before it, so the walk must not
    start partway through the frame: it would wait forever on compressed GSLCs from work it skipped.
    Continuing an earlier run is done by presetting the frame's cursor in frame_states, which these checks
    start from. The same rules as cslc_utils.validate_unphased_batch_proc for DISP-S1.

    Returns True or a message.
    """

    try:
        data_start = datetime.strptime(proc["data_start_date"], BATCH_PROC_DATETIME_FORMAT)
        data_end = datetime.strptime(proc["data_end_date"], BATCH_PROC_DATETIME_FORMAT)
    except (KeyError, TypeError, ValueError):
        return True

    k, m = proc["k"], proc["m"]
    frame_states = proc.get("frame_states") or {}
    for frame_id in expand_batch_proc_frames(proc["frames"]):
        dates = frame_db[frame_id].sensing_datetimes
        cursor = int(frame_states.get(str(frame_id), frame_states.get(frame_id, 0)) or 0)

        for position in range(cursor, len(dates) - k + 1, k):
            window = dates[position:position + k]
            if window[-1] > data_end:
                break                      # the walk finishes here
            if straddles_start_date(window, data_start):
                return (f"Frame {frame_id}: data_start_date {data_start:%Y-%m-%dT%H:%M:%S} falls inside k-set "
                        f"{window[0]:%Y-%m-%d}..{window[-1]:%Y-%m-%d}. A k-set that starts before data_start_date "
                        f"is skipped whole, so its later dates would not be processed either. Set "
                        f"data_start_date on or before {window[0]:%Y-%m-%dT%H:%M:%S}.")

        position = first_k_set_on_or_after(dates, cursor, data_start, k)
        if m == 1 or position == cursor or position + k > len(dates) or dates[position + k - 1] > data_end:
            continue                       # nothing waits on skipped work, or the walk finishes there
        return (f"Frame {frame_id}: with data_start_date {data_start:%Y-%m-%dT%H:%M:%S} the walk starts at "
                f"{dates[position]:%Y-%m-%d}, after the frame's first k-set to process ({dates[cursor]:%Y-%m-%d}). "
                f"The k-sets it skips make the compressed GSLCs the later ones need, so the frame would wait "
                f"forever. Set data_start_date on or before {dates[cursor]:%Y-%m-%dT%H:%M:%S}, or preset the "
                f"frame's cursor in frame_states to continue an earlier run.")

    return True


def gslc_unique_id(download_batch_id: str, nisar_frame: str) -> str:
    """Catalog id of one GSLC: its batch and NISAR frame, so a reprocessed granule overwrites in place."""

    return f"{download_batch_id}_{nisar_frame}"
