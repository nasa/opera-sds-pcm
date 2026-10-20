"""Tier-1 metadata sources: the ASF UMM-G sidecar and the ISO XML both DAACs keep next to the data.

Both readers return plain dataclasses with the handful of facts the document builder needs, already normalized
(timestamps as aware UTC datetimes, geometry as a shapely geometry in lon/lat). Nothing here touches the network.
"""
from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from shapely import wkt as shapely_wkt
from shapely.geometry import MultiPolygon, Polygon, box, mapping
from shapely.geometry.base import BaseGeometry

# ---- timestamps -------------------------------------------------------------------------------------------------

_TS_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:[T ](?P<h>\d{2}):(?P<m>\d{2})(?::(?P<s>\d{2})(?:\.(?P<f>\d+))?)?)?"
    r"\s*(?P<tz>Z|[+-]\d{2}:?\d{2})?$")
_COMPACT_RE = re.compile(r"^(?P<d>\d{8})(?:T(?P<t>\d{6}))?Z?$")


def normalize_ts(value: str | datetime | None) -> datetime | None:
    """Parse every timestamp spelling seen in OPERA metadata into an aware UTC datetime.

    Accepts `YYYY-MM-DD`, `YYYY-MM-DD[T ]HH:MM[:SS[.f...]][Z|+HH:MM]` with any number of fractional digits
    (ISO XML carries up to 10), and compact `YYYYMMDD[THHMMSS][Z]`. Returns None for empty input.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    match = _TS_RE.match(text)
    if match:
        frac = (match.group("f") or "")[:6].ljust(6, "0")
        dt = datetime.strptime(
            f"{match.group('date')}T{match.group('h') or '00'}:{match.group('m') or '00'}:{match.group('s') or '00'}"
            f".{frac}", "%Y-%m-%dT%H:%M:%S.%f")
        tz = match.group("tz")
        if tz and tz != "Z":
            sign = 1 if tz[0] == "+" else -1
            digits = tz[1:].replace(":", "")
            dt = dt - sign * timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
        return dt.replace(tzinfo=timezone.utc)
    match = _COMPACT_RE.match(text)
    if match:
        fmt = "%Y%m%dT%H%M%S" if match.group("t") else "%Y%m%d"
        raw = match.group("d") + (f"T{match.group('t')}" if match.group("t") else "")
        return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
    raise ValueError(f"unrecognized timestamp {value!r}")


def fmt_creation(dt: datetime) -> str:
    """GRQ `creation_timestamp`: millisecond precision, no zone."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def fmt_micro_z(dt: datetime) -> str:
    """GRQ `@timestamp`, `ProductReceivedTime`, `daac_CNM_S_timestamp`."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def fmt_seconds_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


# ---- geometry ---------------------------------------------------------------------------------------------------

def _numbers(text: str) -> list[float]:
    return [float(t) for t in re.split(r"[\s,()]+", text) if t]


def polygon_from_text(text: str) -> BaseGeometry | None:
    """Polygon from the posList or attribute spellings seen in OPERA ISO XML.

    * WKT: `POLYGON ((lon lat, ...))`, `MULTIPOLYGON (((...)))` (RTC-S1 posList, DISP-S1 BoundingPolygon)
    * parenthesized coordinate list: `(lon lat lon lat ...)` (CSLC, DISP-S1-STATIC), `((lon lat ...))` (DISP-S1)
    """
    text = (text or "").strip()
    if not text:
        return None
    if text[0].isalpha():
        return shapely_wkt.loads(text)
    numbers = _numbers(text)
    if len(numbers) < 6 or len(numbers) % 2:
        return None
    ring = list(zip(numbers[0::2], numbers[1::2]))
    return Polygon(ring)


def bbox_polygon(west: float, east: float, south: float, north: float) -> Polygon:
    return box(west, south, east, north)


def valid_geometry(geom: BaseGeometry | None) -> BaseGeometry | None:
    if geom is None or geom.is_empty:
        return None
    if not geom.is_valid:
        geom = geom.buffer(0)
        if geom.is_empty:
            return None
    if isinstance(geom, MultiPolygon) and len(geom.geoms) == 1:
        geom = geom.geoms[0]
    return geom


def to_grq_geojson(geom: BaseGeometry) -> dict:
    """GeoJSON dict with the lower-cased type GRQ documents carry (grq2 lower-cases it)."""
    gj = mapping(geom)
    out = {"type": gj["type"].lower(), "coordinates": _lists(gj["coordinates"])}
    return out


def _lists(value):
    if isinstance(value, (list, tuple)):
        return [_lists(v) for v in value]
    return value


# ---- UMM-G ------------------------------------------------------------------------------------------------------

@dataclass
class UmmgInfo:
    granule_ur: str | None = None
    short_name: str | None = None
    insert_time: datetime | None = None
    production_time: datetime | None = None
    begin: datetime | None = None
    end: datetime | None = None
    geometry: BaseGeometry | None = None
    pge_name: str | None = None
    pge_version: str | None = None
    sas_version: str | None = None
    input_granules: list[str] = field(default_factory=list)
    attributes: dict[str, list[str]] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)   # file name -> md5
    sizes: dict[str, int] = field(default_factory=dict)       # file name -> bytes

    def attribute(self, name: str) -> str | None:
        values = self.attributes.get(name)
        return values[0] if values else None


def parse_ummg(raw: bytes | str | dict) -> UmmgInfo:
    umm = raw if isinstance(raw, dict) else json.loads(raw)
    umm = umm.get("umm", umm)  # tolerate a CMR {meta, umm} wrapper
    info = UmmgInfo()
    info.granule_ur = umm.get("GranuleUR")
    info.short_name = (umm.get("CollectionReference") or {}).get("ShortName")
    for pd in umm.get("ProviderDates") or []:
        if pd.get("Type") == "Insert":
            info.insert_time = normalize_ts(pd.get("Date"))
    data_granule = umm.get("DataGranule") or {}
    info.production_time = normalize_ts(data_granule.get("ProductionDateTime"))
    for ident in data_granule.get("Identifiers") or []:
        if ident.get("IdentifierName") == "SASVersionId":
            info.sas_version = ident.get("Identifier")
        elif ident.get("IdentifierName") == "PGEVersionId" and not info.pge_version:
            info.pge_version = ident.get("Identifier")
    for adi in data_granule.get("ArchiveAndDistributionInformation") or []:
        name = adi.get("Name")
        if not name:
            continue
        if adi.get("SizeInBytes") is not None:
            info.sizes[name] = int(adi["SizeInBytes"])
        checksum = adi.get("Checksum") or {}
        if (checksum.get("Algorithm") or "").upper().replace("-", "") == "MD5" and checksum.get("Value"):
            info.checksums[name] = checksum["Value"]
    pge = umm.get("PGEVersionClass") or {}
    info.pge_name = pge.get("PGEName")
    info.pge_version = pge.get("PGEVersion") or info.pge_version
    temporal = umm.get("TemporalExtent") or {}
    if "RangeDateTime" in temporal:
        info.begin = normalize_ts(temporal["RangeDateTime"].get("BeginningDateTime"))
        info.end = normalize_ts(temporal["RangeDateTime"].get("EndingDateTime")) or info.begin
    elif "SingleDateTime" in temporal:
        info.begin = info.end = normalize_ts(temporal["SingleDateTime"])
    info.geometry = _ummg_geometry(umm)
    info.input_granules = list(umm.get("InputGranules") or [])
    info.attributes = {a["Name"]: [str(v) for v in a.get("Values") or []]
                       for a in umm.get("AdditionalAttributes") or [] if a.get("Name")}
    return info


def _ummg_geometry(umm: dict) -> BaseGeometry | None:
    geometry = (((umm.get("SpatialExtent") or {}).get("HorizontalSpatialDomain") or {}).get("Geometry") or {})
    polygons = []
    for gpoly in geometry.get("GPolygons") or []:
        points = (gpoly.get("Boundary") or {}).get("Points") or []
        ring = [(p["Longitude"], p["Latitude"]) for p in points]
        if len(ring) >= 4:
            polygons.append(Polygon(ring))
    if polygons:
        return valid_geometry(polygons[0] if len(polygons) == 1 else MultiPolygon(polygons))
    for rect in geometry.get("BoundingRectangles") or []:
        return valid_geometry(bbox_polygon(rect["WestBoundingCoordinate"], rect["EastBoundingCoordinate"],
                                           rect["SouthBoundingCoordinate"], rect["NorthBoundingCoordinate"]))
    return None


# ---- ISO XML ----------------------------------------------------------------------------------------------------

NS = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gml": "http://www.opengis.net/gml/3.2",
    "eos": "http://earthdata.nasa.gov/schema/eos",
}
_GML_ALTERNATES = ("http://www.opengis.net/gml/3.2", "http://www.opengis.net/gml")


@dataclass
class IsoInfo:
    attributes: dict[str, str] = field(default_factory=dict)
    identifiers: dict[str, str] = field(default_factory=dict)   # MD_Identifier description -> code
    begin: datetime | None = None
    end: datetime | None = None
    poslist_geometry: BaseGeometry | None = None
    bbox_geometry: BaseGeometry | None = None

    def attribute(self, *names: str) -> str | None:
        for name in names:
            value = self.attributes.get(name)
            if value not in (None, ""):
                return value
        return None

    @property
    def pge_version(self) -> str | None:
        return self.identifiers.get("OtherId: PGEVersionId")

    @property
    def sas_version(self) -> str | None:
        return self.identifiers.get("OtherId: SASVersionId")

    @property
    def processing_time(self) -> datetime | None:
        value = self.attribute("ProcessingDateTime", "ProcessingDatetime")
        return normalize_ts(value) if value else None

    def geometry(self) -> BaseGeometry | None:
        """posList polygon, else the DISP-S1 BoundingPolygon attribute, else the geographic bounding box."""
        for candidate in (self.poslist_geometry, _safe_polygon(self.attribute("BoundingPolygon")),
                          self.bbox_geometry):
            geom = valid_geometry(candidate)
            if geom is not None:
                return geom
        return None


def _safe_polygon(text: str | None) -> BaseGeometry | None:
    try:
        return polygon_from_text(text) if text else None
    except Exception:  # malformed text falls through to the next geometry source
        return None


def _text(element) -> str | None:
    if element is None:
        return None
    return (element.text or "").strip() or None


def parse_iso(raw: bytes | str) -> IsoInfo:
    root = ET.fromstring(raw)
    info = IsoInfo()
    eos, gco, gmd = NS["eos"], NS["gco"], NS["gmd"]

    for attr in root.iter(f"{{{eos}}}AdditionalAttribute"):
        name = _text(attr.find(f"{{{eos}}}reference/{{{eos}}}EOS_AdditionalAttributeDescription/"
                               f"{{{eos}}}name/{{{gco}}}CharacterString"))
        if not name:
            continue
        values = [_text(v) for v in attr.findall(f"{{{eos}}}value/{{{gco}}}CharacterString")]
        values = [v for v in values if v is not None]
        if values and name not in info.attributes:
            info.attributes[name] = values[0] if len(values) == 1 else json.dumps(values)

    for ident in root.iter(f"{{{gmd}}}MD_Identifier"):
        code = _text(ident.find(f"{{{gmd}}}code/{{{gco}}}CharacterString"))
        desc = _text(ident.find(f"{{{gmd}}}description/{{{gco}}}CharacterString"))
        if code and desc and desc not in info.identifiers:
            info.identifiers[desc] = code

    for gml in _GML_ALTERNATES:
        begin = root.find(f".//{{{gml}}}beginPosition")
        if begin is not None:
            info.begin = normalize_ts(_text(begin))
            info.end = normalize_ts(_text(root.find(f".//{{{gml}}}endPosition"))) or info.begin
            break
        position = root.find(f".//{{{gml}}}timePosition")
        if position is not None:
            info.begin = info.end = normalize_ts(_text(position))
            break

    for gml in _GML_ALTERNATES:
        poslist = root.find(f".//{{{gml}}}posList")
        if poslist is not None and _text(poslist):
            info.poslist_geometry = _safe_polygon(_text(poslist))
            break

    for bbox in root.iter(f"{{{gmd}}}EX_GeographicBoundingBox"):
        values = {}
        for side in ("westBoundLongitude", "eastBoundLongitude", "southBoundLatitude", "northBoundLatitude"):
            values[side] = _text(bbox.find(f"{{{gmd}}}{side}/{{{gco}}}Decimal"))
        if all(values.values()):
            info.bbox_geometry = bbox_polygon(float(values["westBoundLongitude"]),
                                              float(values["eastBoundLongitude"]),
                                              float(values["southBoundLatitude"]),
                                              float(values["northBoundLatitude"]))
            break
    return info


def parse_list_attribute(value: str | None) -> list[str]:
    """ISO list attributes come as JSON (`["a", "b"]`), Python repr (`['a', 'b']`) or comma-separated text."""
    if not value:
        return []
    text = value.strip()
    if text.startswith("["):
        try:
            return [str(v) for v in json.loads(text)]
        except json.JSONDecodeError:
            try:
                return [str(v) for v in json.loads(text.replace("'", '"'))]
            except json.JSONDecodeError:
                text = text.strip("[]")
    return [t.strip().strip("'\"") for t in text.split(",") if t.strip()]


def md5_from_twin(body: bytes) -> str | None:
    """The DAACs' `.md5` twins hold the 32 hex digits, sometimes followed by a file name."""
    text = body.decode("ascii", errors="replace").strip()
    match = re.match(r"^([0-9a-fA-F]{32})\b", text)
    return match.group(1).lower() if match else None


def as_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return fmt_seconds_z(value)
    return value
