"""Tier 2 (DISP-S1 only in v1): facts that need the product file itself or the DISP-S1 burst database.

* `disp_s1_cycles` derives `metadata.acquisition_cycle` and the k-set id (`metadata.input_granule_id`, for
  example `f16655_a984_f16655_a972_...`) from the frame number, the secondary date and the CSLC inputs, with the
  same `data_subscriber.cslc_utils` functions the DISP-S1 triggers use. It needs the consistent burst database.
* `read_disp_s1_runconfig` reads `metadata/pge_runconfig` from the `.nc` over S3 with ranged GETs, so only the
  HDF5 superblock, the object headers and the string itself are transferred.
"""
from __future__ import annotations

import io
import logging
import posixpath
import threading
from collections import OrderedDict
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import yaml

from tools.ops.daac_catalog.sources import normalize_ts

logger = logging.getLogger(__name__)

_CSLC_ACQ_PREFIX = "OPERA_L2_CSLC-S1_"


def _naive_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None)


def cslc_acquisition_time(name: str) -> datetime | None:
    """Acquisition time of a CSLC from its id or file name (`OPERA_L2_CSLC-S1_<burst>_<YYYYMMDDTHHMMSSZ>_...`)."""
    base = posixpath.basename(name)
    if not base.startswith(_CSLC_ACQ_PREFIX):
        return None
    parts = base[len(_CSLC_ACQ_PREFIX):].split("_")
    if len(parts) < 2:
        return None
    return normalize_ts(parts[1])


def disp_s1_cycles(frame_id: int, cslc_inputs: list[str], frame_to_bursts) -> dict:
    """The k-set id of a DISP-S1 product's CSLC inputs (newest cycle first) and its acquisition cycle.

    Every product of one DISP-S1 run carries the run's k-set id and, as `acquisition_cycle`, the newest cycle of
    that set (the `latest_acq_index` in the job name), not the cycle of the product's own secondary date.
    """
    from data_subscriber.cslc_utils import determine_acquisition_cycle_cslc

    if frame_id not in frame_to_bursts:
        return {}
    cycles = set()
    for name in cslc_inputs:
        acquired = cslc_acquisition_time(name)
        if acquired is not None:
            cycles.add(determine_acquisition_cycle_cslc(_naive_utc(acquired), frame_id, frame_to_bursts))
    if not cycles:
        return {}
    ordered = sorted(cycles, reverse=True)
    return {"acquisition_cycle": ordered[0],
            "ksc_id": "_".join(f"f{frame_id}_a{cycle}" for cycle in ordered)}


@lru_cache(maxsize=4)
def load_burst_db(path: str):
    """frame_to_bursts from a local consistent-burst-database JSON (`DISP_S1_BURST_DB_S3PATH` in settings.yaml)."""
    from data_subscriber.cslc_utils import process_disp_frame_burst_hist
    frame_to_bursts, _, _ = process_disp_frame_burst_hist(str(path), use_processing_modes=False)
    return frame_to_bursts


def fetch_burst_db(url_or_path: str, cache_dir: Path, s3_client=None) -> Path:
    """Local copy of the burst database; s3:// URLs are downloaded once into `cache_dir`."""
    if not url_or_path.startswith("s3://"):
        return Path(url_or_path)
    bucket, key = url_or_path[5:].split("/", 1)
    target = Path(cache_dir) / posixpath.basename(key)
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        if s3_client is None:
            import boto3
            s3_client = boto3.client("s3")
        s3_client.download_file(bucket, key, str(target))
    return target


class S3RangeFile(io.RawIOBase):
    """Read-only, seekable file object over one S3 object, fetched in cached fixed-size blocks.

    h5py accepts any binary file object with read/seek/tell, so this lets it walk an HDF5 file in S3 while
    transferring only the blocks it touches. `bytes_fetched` records the cost.
    """

    def __init__(self, s3_client, bucket: str, key: str, size: int | None = None, block_size: int = 1 << 20,
                 max_blocks: int = 64):
        super().__init__()
        self._s3 = s3_client
        self._bucket = bucket
        self._key = key
        self._block_size = block_size
        self._max_blocks = max_blocks
        self._blocks: OrderedDict[int, bytes] = OrderedDict()
        self._pos = 0
        self._lock = threading.Lock()
        self.bytes_fetched = 0
        self.requests = 0
        if size is None:
            size = s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]
        self._size = int(size)

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        else:
            raise ValueError(whence)
        return self._pos

    def _block(self, index: int) -> bytes:
        with self._lock:
            block = self._blocks.get(index)
            if block is not None:
                self._blocks.move_to_end(index)
                return block
        start = index * self._block_size
        end = min(start + self._block_size, self._size) - 1
        body = self._s3.get_object(Bucket=self._bucket, Key=self._key, Range=f"bytes={start}-{end}")["Body"].read()
        with self._lock:
            self.bytes_fetched += len(body)
            self.requests += 1
            self._blocks[index] = body
            while len(self._blocks) > self._max_blocks:
                self._blocks.popitem(last=False)
        return body

    def readinto(self, buffer) -> int:
        view = memoryview(buffer).cast("B")
        want = min(len(view), max(0, self._size - self._pos))
        done = 0
        while done < want:
            index, offset = divmod(self._pos, self._block_size)
            block = self._block(index)
            chunk = block[offset:offset + (want - done)]
            if not chunk:
                break
            view[done:done + len(chunk)] = chunk
            done += len(chunk)
            self._pos += len(chunk)
        return done


def read_disp_s1_runconfig(s3_client, bucket: str, key: str, size: int | None = None) -> tuple[str, dict]:
    """The PGE RunConfig YAML embedded in a DISP-S1 product and the transfer statistics."""
    import h5py

    with S3RangeFile(s3_client, bucket, key, size=size) as raw:
        with h5py.File(raw, "r") as h5:
            value = h5["metadata/pge_runconfig"][()]
        stats = {"bytes_fetched": raw.bytes_fetched, "requests": raw.requests}
    text = value.decode() if isinstance(value, bytes) else str(value)
    return text, stats


def runconfig_inputs(runconfig_text: str) -> tuple[list[str], list[str]]:
    """CSLC and compressed-CSLC input basenames from a DISP-S1 PGE RunConfig."""
    try:
        rc = yaml.safe_load(runconfig_text) or {}
    except yaml.YAMLError as err:
        logger.warning(f"unparseable DISP-S1 RunConfig: {err}")
        return [], []
    group = (rc.get("RunConfig", {}).get("Groups", {}).get("PGE", {}).get("InputFilesGroup")
             or rc.get("input_file_group") or {})
    files = group.get("cslc_file_list") or group.get("InputFilePaths") or []
    cslc, ccslc = [], []
    for path in files:
        name = posixpath.basename(str(path))
        (ccslc if "COMPRESSED-CSLC" in name else cslc).append(name)
    return cslc, ccslc
