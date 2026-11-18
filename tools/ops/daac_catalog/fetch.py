"""Read tier-1 and tier-2 sources for a parsed granule from the DAAC buckets."""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

from botocore.exceptions import ClientError

from tools.ops.daac_catalog.build import SourceRef, Tier1, Tier2
from tools.ops.daac_catalog.parse import ListedObject, ParsedGranule, Role
from tools.ops.daac_catalog.registry import Dataset, Source
from tools.ops.daac_catalog.sources import md5_from_twin, parse_iso, parse_ummg
from tools.ops.daac_catalog.tier2 import read_disp_s1_runconfig, runconfig_inputs

logger = logging.getLogger(__name__)


@dataclass
class FetchStats:
    gets: int = 0
    bytes: int = 0
    missing: int = 0
    errors: int = 0
    tier2_bytes: int = 0
    tier2_requests: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, **kw):
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, getattr(self, k) + v)

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("gets", "bytes", "missing", "errors", "tier2_bytes",
                                               "tier2_requests")}


@dataclass
class FetchOptions:
    ummg: bool = True
    iso: bool = True
    md5: bool = False
    tier2: bool = False


class SourceFetcher:
    def __init__(self, clients, options: FetchOptions | None = None):
        self.clients = clients
        self.options = options or FetchOptions()
        self.stats = FetchStats()

    def _get(self, spec, obj: ListedObject) -> bytes | None:
        client = self.clients.for_source(spec, Source(obj.source))
        try:
            body = client.get_object(Bucket=obj.bucket, Key=obj.key)["Body"].read()
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                self.stats.add(missing=1)
                return None
            self.stats.add(errors=1)
            raise
        self.stats.add(gets=1, bytes=len(body))
        return body

    def tier1(self, parsed: ParsedGranule) -> tuple[Tier1, list[str]]:
        spec = parsed.spec
        t1, problems = Tier1(), []
        if self.options.ummg:
            for obj in parsed.objects(Role.SIDECAR):
                body = self._get(spec, obj)
                if body is None:
                    problems.append(f"listed but missing at build time: {obj.key}")
                    continue
                t1.ummg = parse_ummg(body)
                t1.refs.append(SourceRef("ummg", obj.s3_url, obj.etag, obj.last_modified))
        if self.options.iso:
            for obj in parsed.objects(Role.METADATA):
                body = self._get(spec, obj)
                if body is None:
                    problems.append(f"listed but missing at build time: {obj.key}")
                    continue
                try:
                    t1.iso = parse_iso(body)
                except Exception as err:  # a malformed ISO file must not stop the granule
                    problems.append(f"unparseable ISO XML {obj.key}: {err}")
                    continue
                t1.refs.append(SourceRef("iso_xml", obj.s3_url, obj.etag, obj.last_modified))
                break
        if self.options.md5:
            for obj in parsed.objects(Role.CHECKSUM):
                body = self._get(spec, obj)
                value = md5_from_twin(body) if body else None
                if value:
                    t1.md5[obj.name.removesuffix(".md5")] = value
        return t1, problems

    def tier2(self, parsed: ParsedGranule) -> tuple[Tier2 | None, list[str]]:
        if not self.options.tier2 or parsed.spec.dataset != Dataset.L3_DISP_S1:
            return None, []
        products = [o for o in parsed.objects(Role.PRIMARY) if o.name.endswith(".nc")]
        if not products:
            return None, ["no .nc file for tier 2"]
        obj = products[0]
        client = self.clients.for_source(parsed.spec, Source(obj.source))
        try:
            text, stats = read_disp_s1_runconfig(client, obj.bucket, obj.key, obj.size)
        except Exception as err:  # tier 2 is best effort; the tier-1 document still stands
            self.stats.add(errors=1)
            return None, [f"tier 2 read of {obj.key} failed: {type(err).__name__}: {err}"]
        self.stats.add(tier2_bytes=stats["bytes_fetched"], tier2_requests=stats["requests"])
        cslc, ccslc = runconfig_inputs(text)
        t2 = Tier2(pge_runconfig=text, cslc_inputs=cslc, ccslc_inputs=ccslc,
                   refs=[SourceRef("product_file", obj.s3_url, obj.etag, obj.last_modified)])
        return t2, []
