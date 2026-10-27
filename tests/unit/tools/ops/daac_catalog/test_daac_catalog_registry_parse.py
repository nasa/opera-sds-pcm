import pytest

from daac_catalog_testlib import gid_for, granule_ids, grq_doc, listing, spec_of
from tools.ops.daac_catalog.parse import (
    GranuleListing, ListedObject, RejectedGranule, Role, classify, parse_granule)
from tools.ops.daac_catalog.registry import (
    DATASETS, Daac, Dataset, Layout, Source, get_spec, granule_id_of, id_pattern, is_granule_id,
    named_group_source, primary_patterns, product_type_config, resolve_datasets)


def test_registry_covers_the_ten_delivered_collections():
    assert len(DATASETS) == 10
    assert {s.daac for s in DATASETS.values()} == {Daac.ASF, Daac.PODAAC}
    assert [s.dataset for s in DATASETS.values() if s.daac == Daac.PODAAC] == [Dataset.L3_DSWX_HLS,
                                                                             Dataset.L3_DSWX_S1]
    for spec in DATASETS.values():
        assert spec.product_prefix.endswith("/") and spec.browse_prefix.endswith("/")
        assert (spec.ummg_prefix is not None) == (spec.daac == Daac.ASF)
        assert (spec.layout == Layout.FOLDER) == (spec.daac == Daac.ASF)
        assert product_type_config(spec.dataset)["Pattern"]
        assert primary_patterns(spec.dataset)


def test_resolve_datasets():
    assert len(resolve_datasets("all")) == 10
    assert [s.dataset for s in resolve_datasets("L4_TROPO,L3_DSWx_S1")] == [Dataset.L4_TROPO, Dataset.L3_DSWX_S1]
    with pytest.raises(ValueError):
        get_spec("L9_NOPE")


@pytest.mark.parametrize("gid", granule_ids())
def test_id_pattern_and_primary_pattern_agree_on_sample(gid):
    spec = spec_of(gid)
    assert is_granule_id(spec.dataset, gid)
    assert gid.startswith(spec.id_stem)
    assert any(gid.startswith(shard) for shard in spec.shards())
    for obj in listing(gid).files:
        assert granule_id_of(spec.dataset, obj.name) == gid, obj.name


def test_shards_are_disjoint_prefixes():
    for spec in DATASETS.values():
        shards = spec.shards()
        assert len(shards) == len(set(shards))
        for a in shards:
            assert not any(b != a and b.startswith(a) for b in shards), (spec.dataset, a)


def test_named_group_source_balances_parentheses_and_classes():
    pattern = r"(?P<id>A_(?P<x>[()\d]+)_(B|C))(?P<ext>[.]h5)$"
    assert named_group_source(pattern, "id") == r"(?P<id>A_(?P<x>[()\d]+)_(B|C))"
    assert named_group_source(pattern, "ext") == r"(?P<ext>[.]h5)"
    with pytest.raises(ValueError):
        named_group_source(pattern, "nope")


def test_tropo_id_pattern_is_anchored_by_match():
    gid = gid_for("L4_TROPO")
    assert granule_id_of(Dataset.L4_TROPO, f"{gid}_low-res.png") == gid
    assert id_pattern(Dataset.L4_TROPO).fullmatch(gid).group("model") == "HRES"


@pytest.mark.parametrize("gid", granule_ids())
def test_files_match_grq_files(gid):
    """metadata.Files built from the listing equals GRQ's on every field but the job-local ones."""
    spec = spec_of(gid)
    parsed = parse_granule(spec, listing(gid))
    ours = {e["FileName"]: e for e in parsed.grq_files}
    theirs = {e["FileName"]: e for e in grq_doc(gid)["metadata"]["Files"]}
    extra = [n for n in set(ours) - set(theirs)]
    missing = [n for n in set(theirs) - set(ours)]
    allowed_extra = [r.split(":", 1)[1] for r in spec.files_not_in_grq if r.startswith("extra:")]
    allowed_missing = [r.split(":", 1)[1] for r in spec.files_not_in_grq if r.startswith("missing:")]
    assert all(any(n.endswith(s) for s in allowed_extra) for n in extra), extra
    assert all(any(n.endswith(s) for s in allowed_missing) for n in missing), missing
    for name in set(ours) & set(theirs):
        a = {k: v for k, v in ours[name].items() if k != "FileLocation"}
        b = {k: v for k, v in theirs[name].items() if k != "FileLocation"}
        if spec.dataset == Dataset.L4_TROPO:   # GRQ ran the unanchored regex on a worker path
            assert b.pop("model").startswith("HRES_v1.0/")
            assert a.pop("model") == "HRES"
        assert a == b, name
        assert ours[name]["FileLocation"] == f"/datasets/{gid}"


def test_files_entry_types_follow_the_extractor():
    parsed = parse_granule(spec_of(gid_for("L2_CSLC_S1")), listing(gid_for("L2_CSLC_S1")))
    h5 = next(e for e in parsed.grq_files if e["ext"] == "h5")
    assert h5["acquisition_ts"] == "2026-05-31T11:06:54.000000Z"
    assert h5["acq_month"] == 5 and isinstance(h5["acq_year"], int)
    assert h5["tags"] == ["PGE"]
    assert "dataset_version" not in h5 and "ProductReceivedTime" not in h5
    assert parsed.dataset_version == "v1.1" and parsed.product_version == "1.1"
    parsed = parse_granule(spec_of(gid_for("L3_DISP_S1")), listing(gid_for("L3_DISP_S1")))
    nc = next(e for e in parsed.grq_files if e["ext"] == "nc")
    assert nc["frame_id"] == "F16655"
    assert nc["creation_ts"] == "20260611T020530Z"          # not a Date_Time_Key for DISP-S1
    assert nc["sec_datetime"] == "2019-01-05T00:05:32.000000Z"


def test_roles():
    gid = gid_for("L2_CSLC_S1")
    spec = spec_of(gid)
    roles = {f.obj.name.removeprefix(gid): f.role for f in parse_granule(spec, listing(gid)).files}
    assert roles == {".h5": Role.PRIMARY, ".h5.md5": Role.CHECKSUM, ".iso.xml": Role.METADATA,
                     ".iso.xml.md5": Role.CHECKSUM, "_BROWSE.png": Role.BROWSE, "_BROWSE.png.md5": Role.CHECKSUM,
                     "_BROWSE_low-res.png": Role.DERIVED, "_BROWSE_low-res.png.md5": Role.CHECKSUM,
                     "_BROWSE_thumbnail.png": Role.DERIVED, "_BROWSE_thumbnail.png.md5": Role.CHECKSUM,
                     ".cmr.json": Role.SIDECAR}
    zarr = ListedObject("products", "b", f"x/{gid}.zarr.json.gz", 1, "2026-01-01T00:00:00Z")
    assert classify(spec, zarr) == (Role.DERIVED, False)


def test_natural_keys_follow_the_schema_examples():
    expected = {
        "L2_CSLC_S1": "L2_CSLC_S1|T113-241595-IW1|20260531T110654Z|S1A|VV|v1.1",
        "L2_RTC_S1": "L2_RTC_S1|T148-316184-IW2|20260802T201459Z|S1C|30|v1.0",
        "L2_CSLC_S1_STATIC": "L2_CSLC_S1_STATIC|T021-043330-IW3|20140403|S1C|v1.0",
        "L3_DISP_S1": "L3_DISP_S1|F16655|VV|20180522T000527Z|20190105T000532Z|v1.0",
        "L3_DISP_S1_STATIC": "L3_DISP_S1_STATIC|F44619|20140403|S1A|v1.0",
        "L3_DSWx_HLS": "L3_DSWx_HLS|T59KKS|20260627T224423Z|L9|30|v1.1",
        "L4_TROPO": "L4_TROPO|20260728T000000Z|HRES|v1.0",
    }
    for dataset, key in expected.items():
        gid = gid_for(dataset)
        assert parse_granule(spec_of(gid), listing(gid)).natural_key == key


def test_rejects():
    spec = DATASETS[Dataset.L2_CSLC_S1]
    with pytest.raises(RejectedGranule, match="does not match"):
        parse_granule(spec, GranuleListing("OPERA_L2_CSLC-S1_garbage", spec.dataset.value, []))
    gid = gid_for("L2_CSLC_S1")
    browse_only = [o for o in listing(gid).files if o.source == Source.BROWSE.value and o.name.endswith(".md5")]
    with pytest.raises(RejectedGranule, match="Primary"):
        parse_granule(spec, GranuleListing(gid, spec.dataset.value, browse_only))
    foreign = listing(gid)
    foreign.files.append(ListedObject("products", spec.product_bucket, f"{spec.product_prefix}{gid}/stray.txt", 1,
                                      "2026-01-01T00:00:00Z"))
    parsed = parse_granule(spec, foreign)
    assert parsed.problems == [f"foreign file {spec.product_prefix}{gid}/stray.txt"]
