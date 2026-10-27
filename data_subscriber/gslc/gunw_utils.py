"""Selection of the NISAR GUNWs a DISP-NI k-set can use for its ionosphere correction.

A GUNW is an interferogram between two passes of a NISAR frame. With GUNWs the SAS takes the ionosphere
phase screen from them; without, it estimates it from frequencyB of the GSLCs, which takes longer. The
SAS needs consecutive GUNWs with no gap from the k-set's first date to its last (SAS User Guide, "GUNW
Files"): N-1 GUNWs for N dates, or more where the chain passes through a pass that is not in the k-set,
e.g. a date the DISP-NI frame database leaves out.

A k-set therefore gets GUNWs only when one GUNW <mode>_<polarization> has such a chain on every NISAR
frame of the DISP-NI frame. Otherwise it gets none.
"""

import re
from dataclasses import dataclass
from datetime import datetime

from opera_commons.logger import get_logger

logger = get_logger()

GUNW_DATASET_TYPE = "L2_GUNW_NI"
GUNW_MODE_POLARIZATION_SETTINGS_FIELD = "GUNW_MODE_POLARIZATION_PREFERENCE"

GUNW_GRANULE_REGEX = re.compile(
    r"(?P<id>NISAR_L2_(?P<processing_type>PR|UR|OD)_GUNW_(?P<reference_cycle>\d{3})_(?P<track>\d{3})_"
    r"(?P<orbit_dir>[AD])_(?P<nisar_frame>\d{3})_(?P<secondary_cycle>\d{3})_(?P<mode>\d{4})_"
    r"(?P<pol>SH|SV|DH|DV|CL|CR|QP|NA)_(?P<reference_start>\d{8}T\d{6})_(?P<reference_end>\d{8}T\d{6})_"
    r"(?P<secondary_start>\d{8}T\d{6})_(?P<secondary_end>\d{8}T\d{6})_(?P<crid>[A-Za-z0-9]{6})_"
    r"(?P<accuracy>[PMNF])_(?P<coverage>[FP])_(?P<sds_location>[JN])_(?P<counter>\d{3}))$")


@dataclass(frozen=True)
class GunwGranuleId:
    """The fields of a NISAR GUNW granule id that GUNW selection keys on."""
    granule_id: str
    reference_cycle: int
    secondary_cycle: int
    nisar_frame: str          # <track>_<A|D>_<frame>, e.g. "151_A_011"
    mode: str                 # bandwidth mode, e.g. "4000"
    polarization: str         # e.g. "SH"
    reference_start: datetime
    secondary_start: datetime

    @property
    def mode_polarization(self) -> str:
        return f"{self.mode}_{self.polarization}"


def parse_gunw_granule_id(granule_id: str) -> GunwGranuleId:
    """Parse a NISAR GUNW granule id (or filename). Raises ValueError if it does not match."""

    match = GUNW_GRANULE_REGEX.match(granule_id.removesuffix(".h5"))
    if not match:
        raise ValueError(f"{granule_id} is not a NISAR GUNW granule id")

    return GunwGranuleId(
        granule_id=match.group("id"),
        reference_cycle=int(match.group("reference_cycle")),
        secondary_cycle=int(match.group("secondary_cycle")),
        nisar_frame=f'{match.group("track")}_{match.group("orbit_dir")}_{match.group("nisar_frame")}',
        mode=match.group("mode"),
        polarization=match.group("pol"),
        reference_start=datetime.strptime(match.group("reference_start"), "%Y%m%dT%H%M%S"),
        secondary_start=datetime.strptime(match.group("secondary_start"), "%Y%m%dT%H%M%S"),
    )


def build_gunw_native_id_patterns(nisar_frames) -> list[str]:
    """CMR native-id patterns matching every GUNW of these NISAR frames, any cycle pair and mode."""

    return [f"NISAR_L2_*_GUNW_*_{nisar_frame}_*" for nisar_frame in sorted(nisar_frames)]


def select_gunws(gunws: list[GunwGranuleId], cycles_by_nisar_frame: dict[str, list[int]],
                 mode_polarizations: list[str]) -> tuple[str, list[GunwGranuleId]] | None:
    """The first preferred GUNW <mode>_<polarization> with a chain on every NISAR frame, and that chain.

    One <mode>_<polarization> is used for the whole k-set, as the PGE names its products after the
    first GUNW's. Returns None if no preferred <mode>_<polarization> has a complete chain."""

    for mode_polarization in mode_polarizations:
        chains = [gunw_chain(gunws, nisar_frame, cycles, mode_polarization)
                  for nisar_frame, cycles in sorted(cycles_by_nisar_frame.items())]
        if all(chain is not None for chain in chains):
            return mode_polarization, [gunw for chain in chains for gunw in chain]

    return None


def gunw_chain(gunws: list[GunwGranuleId], nisar_frame: str, cycles: list[int],
               mode_polarization: str) -> list[GunwGranuleId] | None:
    """The GUNWs of one NISAR frame and <mode>_<polarization> that chain through every cycle, in order.

    Between two consecutive cycles the chain may pass through cycles that are not in the list, and
    takes the shortest hop first, backtracking on a dead end. Reprocessed GUNWs of the same pair are
    reduced to the newest one, which has the highest granule id. Returns None if there is no chain."""

    hops = {}
    for gunw in gunws:
        if gunw.nisar_frame != nisar_frame or gunw.mode_polarization != mode_polarization:
            continue
        pair = (gunw.reference_cycle, gunw.secondary_cycle)
        if pair not in hops or gunw.granule_id > hops[pair].granule_id:
            hops[pair] = gunw

    cycles = sorted(cycles)
    chain = []
    for start, end in zip(cycles, cycles[1:]):
        link = _link(hops, start, end)
        if link is None:
            logger.info("No %s GUNW chain from cycle %d to %d on NISAR frame %s",
                        mode_polarization, start, end, nisar_frame)
            return None
        chain.extend(link)

    return chain


def _link(hops: dict, start: int, end: int) -> list[GunwGranuleId] | None:
    """GUNWs from cycle start to cycle end, every one of them starting and ending between the two."""

    if start == end:
        return []

    for (reference, secondary), gunw in sorted(hops.items()):
        if reference == start and start < secondary <= end:
            rest = _link(hops, secondary, end)
            if rest is not None:
                return [gunw] + rest

    return None
