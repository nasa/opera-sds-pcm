"""DISP-NI frame database and NISAR GSLC naming helpers.

The DISP-NI frame database lists, for each DISP-NI frame, the NISAR frames it is made of and the
sensing datetimes on which every one of them was acquired. It drives DISP-NI historical processing
the way the consistent burst database drives DISP-S1. ADT has not delivered it yet, so the layout
is assumed to match the DISP-S1 database with the burst list replaced by NISAR frame identifiers:

    {"metadata": {...},
     "data": {"26410": {"nisar_frame_list": ["151_A_011"],
                        "sensing_time_list": ["2025-11-08T15:50:41", ...]}}}

The keys are the constants below, so the loader is the only code to change when the real file
arrives. A NISAR frame identifier is <track>_<A|D>_<frame>, as it appears in GSLC filenames.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from functools import cache

from opera_commons.logger import get_logger
from data_subscriber.cslc.disp_s1_phases import parse_sensing_time_list
from data_subscriber.cslc_utils import localize_anc_json
from util import datasets_json_util

logger = get_logger()

DISP_NI_CBDB_SETTINGS_FIELD = "DISP_NI_CBDB_S3PATH"
DEFAULT_DISP_NI_CBDB_NAME = "opera-disp-ni-consistent-frame-db.json"
CBDB_DATA_KEY = "data"
CBDB_NISAR_FRAME_LIST_KEY = "nisar_frame_list"
CBDB_SENSING_TIME_LIST_KEY = "sensing_time_list"

GSLC_DATASET_TYPE = "L2_GSLC_NI"
GSLC_MODE_POLARIZATION_SETTINGS_FIELD = "GSLC_MODE_POLARIZATION_PREFERENCE"


@dataclass
class NisarFrame:
    """One DISP-NI frame of the frame database."""
    frame_number: int
    nisar_frames: set[str] = field(default_factory=set)            # e.g. {"151_A_011"}
    sensing_datetimes: list[datetime] = field(default_factory=list)  # sorted


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
                                             nisar_frames=set(entry[CBDB_NISAR_FRAME_LIST_KEY]),
                                             sensing_datetimes=sensing_datetimes)

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
    )


def build_gslc_native_id_patterns(frame: NisarFrame) -> list[str]:
    """CMR native-id patterns matching every GSLC of the frame's NISAR frames, any cycle and mode."""

    return [f"NISAR_L2_*_GSLC_*_{nisar_frame}_*" for nisar_frame in sorted(frame.nisar_frames)]


def gslc_download_batch_id(frame_id: int, cycle: int) -> str:
    """One DISP-NI frame on one NISAR cycle, e.g. f26410_a4.

    Same shape as the DISP-S1 batch id, so cslc_utils.split_download_batch_id() splits it."""

    return f"f{frame_id}_a{cycle}"


def gslc_unique_id(download_batch_id: str, nisar_frame: str) -> str:
    """Catalog id of one GSLC: its batch and NISAR frame, so a reprocessed granule overwrites in place."""

    return f"{download_batch_id}_{nisar_frame}"
