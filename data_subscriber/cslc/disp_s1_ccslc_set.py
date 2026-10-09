"""Marker dataset for one DISP-S1 compressed CSLC (CCSLC) set.

A forward DISP-S1 SCIFLO at a k-boundary publishes one L2_CSLC_S1_COMPRESSED dataset
per burst. Triggering the k-cycle evaluator on each of them starts one re-evaluation
per burst, all at once, and they race each other to finalize the same KSCs. The
SCIFLO therefore also writes one marker per set, and the evaluator rule fires on the
marker instead.

The marker has to reach the rule engine after its members are searchable. HySDS
discovers a job's datasets in sorted directory order (hysds.utils.find_dataset_json),
the bulk publisher indexes all of them in one request, and only then queues their
dataset_processed events in that same order. The marker id starts with a lowercase
letter, which sorts after every OPERA_L2_... CCSLC id and OPERA_L3_... L3 id, so its
event is the last one the SCIFLO emits. The evaluator still checks that every member
is in GRQ before acting, in case a future publisher does not keep that order.
"""

import json
import logging
import os
import shutil
from datetime import datetime, timezone

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc_utils import parse_ccslc_doc_id_dates

logger = logging.getLogger(__name__)

CCSLC_ID_PREFIX = "OPERA_L2_COMPRESSED-CSLC-S1"


def make_ccslc_set_id(frame_id, ref_date, first_date, last_date, creation_dt):
    """Dataset id of a CCSLC-set marker.

    Format: disp_s1-ccslc-set-f{frame_id}-{ref}-{first}-{last}-{YYYYMMDDTHHMMSSZ}
    Example: disp_s1-ccslc-set-f36541-20250703-20250703-20260627-20261006T101500Z

    The creation timestamp keeps the markers of two SCIFLO runs for one boundary
    distinct, so a duplicate run shows up as two markers instead of an overwrite.
    """
    return (f"{c.DISP_S1_CCSLC_SET}-f{int(frame_id)}-{ref_date}-{first_date}-{last_date}-"
            f"{creation_dt.strftime('%Y%m%dT%H%M%SZ')}")


def is_ccslc_id(dataset_id):
    return dataset_id.startswith(CCSLC_ID_PREFIX)


def create_ccslc_set_dataset(datasets_dir, frame_id, ccslc_ids, ksc_id=None, sensing_date=None,
                             now=None):
    """Write the marker dataset directory for a set of CCSLC datasets.

    :param datasets_dir: directory the job's other datasets were written to.
    :param frame_id: DISP-S1 frame the set belongs to.
    :param ccslc_ids: dataset ids of the member CCSLCs.
    :param ksc_id: id of the KSC whose SCIFLO produced the set.
    :param sensing_date: that KSC's sensing date (YYYYMMDD), the k-boundary.
    :param now: creation time (UTC), defaults to the current time.
    :return: the marker directory path.
    """
    ccslc_ids = sorted(set(ccslc_ids))
    if not ccslc_ids:
        raise ValueError("a CCSLC-set marker needs at least one member")

    dates = {}
    for ccslc_id in ccslc_ids:
        parsed = parse_ccslc_doc_id_dates(ccslc_id)
        if parsed is None:
            raise ValueError(f"cannot parse CCSLC dates from {ccslc_id}")
        dates[ccslc_id] = parsed

    ref_date = min(d[0] for d in dates.values())
    first_date = min(d[1] for d in dates.values())
    last_date = max(d[2] for d in dates.values())

    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    set_id = make_ccslc_set_id(frame_id, ref_date, first_date, last_date, now)

    metadata = {
        "id": set_id,
        c.FRAME_ID: int(frame_id),
        c.KSC_ID: ksc_id,
        c.SENSING_DATE: sensing_date,
        c.REF_DATE: ref_date,
        c.FIRST_DATE: first_date,
        c.LAST_DATE: last_date,
        c.CCSLC_IDS: ccslc_ids,
        c.CCSLC_COUNT: len(ccslc_ids),
    }

    def iso(yyyymmdd):
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}T00:00:00.000000Z"

    dataset = {
        "version": "1",
        "creation_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "starttime": iso(first_date),
        "endtime": iso(last_date),
        "index": {"suffix": f"1_{c.DISP_S1_CCSLC_SET}-{now.strftime('%Y.%m')}"},
    }

    set_dir = os.path.join(datasets_dir, set_id)
    if os.path.isdir(set_dir):
        shutil.rmtree(set_dir)
    os.makedirs(set_dir)

    with open(os.path.join(set_dir, f"{set_id}.met.json"), "w") as f:
        json.dump(metadata, f, indent=2)
    with open(os.path.join(set_dir, f"{set_id}.dataset.json"), "w") as f:
        json.dump(dataset, f, indent=2)

    logger.info(f"Created CCSLC-set marker {set_id} for {len(ccslc_ids)} CCSLCs of frame {frame_id}")
    return set_dir
