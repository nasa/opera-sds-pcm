"""Listing against mocked DAAC buckets built from the fixture listings."""
import boto3
import pytest
from moto import mock_aws

from daac_catalog_testlib import gid_for, granule_ids, listing
from tools.ops.daac_catalog import listing as listing_mod
from tools.ops.daac_catalog.build import BuildContext
from tools.ops.daac_catalog.parse import parse_granule
from tools.ops.daac_catalog.registry import (
    ASF_BROWSE_BUCKET, ASF_PRODUCTS_BUCKET, DATASETS, PODAAC_PROTECTED_BUCKET, PODAAC_PUBLIC_BUCKET, Dataset,
    Source)
from tools.ops.daac_catalog.runs import RunDir, read_done, read_jsonl_gz
from tools.ops.daac_catalog.stages import BuildOptions, run_build


class FakeClients:
    def __init__(self, s3):
        self.s3 = s3

    def for_source(self, spec, source):
        return self.s3

    def uses_daac_credentials(self, spec, source):
        return False


@pytest.fixture
def s3(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        for bucket in (ASF_PRODUCTS_BUCKET, ASF_BROWSE_BUCKET, PODAAC_PROTECTED_BUCKET, PODAAC_PUBLIC_BUCKET):
            client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        for gid in granule_ids():
            for obj in listing(gid).files:
                client.put_object(Bucket=obj.bucket, Key=obj.key, Body=b"x" * min(obj.size, 64))
        # a second CSLC granule in the same shard, and one in another shard
        cslc = gid_for("L2_CSLC_S1")
        spec = DATASETS[Dataset.L2_CSLC_S1]
        for other in (cslc.replace("20260601T074626Z", "20260602T000000Z"),
                      cslc.replace("T113-241595", "T001-000001")):
            client.put_object(Bucket=ASF_PRODUCTS_BUCKET, Key=f"{spec.product_prefix}{other}/{other}.h5", Body=b"h5")
        checksum_only = cslc.replace("20260601T074626Z", "20260603T000000Z")
        client.put_object(Bucket=ASF_PRODUCTS_BUCKET,
                          Key=f"{spec.product_prefix}{checksum_only}/{checksum_only}.h5.md5", Body=b"0" * 32)
        client.put_object(Bucket=ASF_PRODUCTS_BUCKET, Key=f"{spec.product_prefix}OPERA_L2_CSLC-S1_T113-stray.txt",
                          Body=b"?")
        monkeypatch.setattr(listing_mod, "iter_objects", _small_pages(listing_mod.iter_objects))
        yield client


def _small_pages(original):
    def wrapper(*args, **kwargs):
        kwargs["page_size"] = 2
        return original(*args, **kwargs)
    return wrapper


def test_asf_shard_merges_products_browse_and_ummg(s3, tmp_path):
    spec = DATASETS[Dataset.L2_CSLC_S1]
    stats = listing_mod.ShardStats()
    granules = list(listing_mod.list_shard(spec, "OPERA_L2_CSLC-S1_T113-", FakeClients(s3), spec.sources(), stats))
    assert [g.id for g in granules] == sorted(g.id for g in granules)
    assert len(granules) == 3
    gid = gid_for("L2_CSLC_S1")
    full = next(g for g in granules if g.id == gid)
    assert {f.source for f in full.files} == {"products", "browse", "ummg"}
    assert len(full.files) == len(listing(gid).files)
    assert stats.unparsed_count == 1 and stats.unparsed_keys[0].endswith("stray.txt")
    assert stats.requests["products"] >= 3   # paginated at 2 keys per page
    parsed = parse_granule(spec, full)
    assert len(parsed.grq_files) == 3


def test_podaac_flat_layout_groups_by_id(s3):
    spec = DATASETS[Dataset.L3_DSWX_HLS]
    gid = gid_for("L3_DSWx_HLS")
    stats = listing_mod.ShardStats()
    granules = list(listing_mod.list_shard(spec, "OPERA_L3_DSWx-HLS_T59K", FakeClients(s3), spec.sources(), stats))
    assert [g.id for g in granules] == [gid]
    sources = {f.source for f in granules[0].files}
    assert sources == {"products", "browse"}
    assert len(granules[0].files) == len(listing(gid).files)
    assert stats.unparsed_count == 0


def test_run_listing_writes_manifests_and_skips_done_shards(s3, tmp_path):
    spec = DATASETS[Dataset.L2_CSLC_S1]
    run = RunDir(tmp_path, "inv-20261020T2100Z")
    clients = FakeClients(s3)
    summary = listing_mod.run_listing(spec, run, clients, spec.sources(), workers=4, shard_filter="T11*")
    assert summary["failed"] == 0 and summary["listed"] == 10 and summary["granules"] == 3
    records = list(read_jsonl_gz(run.listing(spec.dataset.value, "T113-")))
    assert len(records) == 3
    done = read_done(run.listing(spec.dataset.value, "T113-"))
    assert done["complete"] and done["shard_prefix"] == "OPERA_L2_CSLC-S1_T113-"
    again = listing_mod.run_listing(spec, run, clients, spec.sources(), workers=4, shard_filter="T11*")
    assert again["skipped"] == 10 and again["listed"] == 0

    build = run_build(spec, run, None, BuildContext(run.run_id, run.listed_at), BuildOptions(tier=0, workers=2))
    assert build["docs"] == 2 and build["rejects"] == 1
    rejects = run.rejects(spec.dataset.value, "T113-").read_text()
    assert "no data file" in rejects
    docs = list(read_jsonl_gz(run.docs(spec.dataset.value, "T113-")))
    assert {d["catalog"]["tier"] for d in docs} == {0}
    rerun = run_build(spec, run, None, BuildContext(run.run_id, run.listed_at), BuildOptions(tier=0, workers=2))
    assert rerun["skipped"] == 10 and rerun["docs"] == 0
