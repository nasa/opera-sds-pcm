"""Collections whose granules live under more than one bucket prefix (DSWx-HLS v1.0 under PROVISIONAL_V1)."""
import json
from dataclasses import replace

import boto3
import pytest
from moto import mock_aws

from daac_catalog_testlib import gid_for, listing
from tools.ops.daac_catalog import cli, export
from tools.ops.daac_catalog import listing as listing_mod
from tools.ops.daac_catalog.build import BuildContext
from tools.ops.daac_catalog.parse import GranuleListing, parse_granule
from tools.ops.daac_catalog.registry import (
    DATASETS, PODAAC_PROTECTED_BUCKET, PODAAC_PUBLIC_BUCKET, Dataset, Source)
from tools.ops.daac_catalog.runs import RunDir, done_marker, read_done, read_jsonl_gz, write_json
from tools.ops.daac_catalog.stages import BuildOptions, run_build

SPEC = DATASETS[Dataset.L3_DSWX_HLS]
CURRENT = "OPERA_L3_DSWX-HLS_V1/"
PROVISIONAL = "OPERA_L3_DSWX-HLS_PROVISIONAL_V1/"
GID = gid_for("L3_DSWx_HLS")
OLD_GID = "OPERA_L3_DSWx-HLS_T59KKS_20230627T224423Z_20230702T012549Z_L8_30_v1.0"


class FakeClients:
    def __init__(self, s3):
        self.s3 = s3

    def for_source(self, spec, source):
        return self.s3


def provisional_copy(gid: str = GID, new_gid: str = OLD_GID) -> list:
    return [replace(o, key=o.key.replace(CURRENT, PROVISIONAL).replace(gid, new_gid)) for o in listing(gid).files]


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-west-2")
        for bucket in (PODAAC_PROTECTED_BUCKET, PODAAC_PUBLIC_BUCKET):
            client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        for obj in listing(GID).files + provisional_copy():
            client.put_object(Bucket=obj.bucket, Key=obj.key, Body=b"x" * min(obj.size, 64))
        yield client


def test_registry_prefixes():
    assert SPEC.prefixes(Source.PRODUCTS) == [CURRENT, PROVISIONAL] == SPEC.prefixes(Source.BROWSE)
    assert SPEC.prefix_of(Source.BROWSE, f"{PROVISIONAL}{OLD_GID}.iso.xml") == PROVISIONAL
    assert SPEC.prefix_of(Source.PRODUCTS, "elsewhere/x.tif") is None
    assert SPEC.granule_prefix(Source.PRODUCTS, OLD_GID, PROVISIONAL) == f"{PROVISIONAL}{OLD_GID}"
    assert SPEC.granule_prefix(Source.PRODUCTS, GID) == f"{CURRENT}{GID}"
    cslc = DATASETS[Dataset.L2_CSLC_S1]
    assert cslc.prefixes(Source.UMMG) == ["UMMG/OPERA_L2_CSLC-S1_V1/"]
    assert cslc.prefixes(Source.PRODUCTS) == ["OPERA_L2_CSLC-S1/"]
    with pytest.raises(ValueError, match="no bucket prefix"):
        listing_mod.listed_prefixes(SPEC, ["OPERA_L3_DSWX-HLS_PROVISIONAL_V0/"])


def test_shard_merges_every_prefix_or_the_chosen_ones(s3):
    stats = listing_mod.ShardStats()
    granules = list(listing_mod.list_shard(SPEC, "OPERA_L3_DSWx-HLS_T59K", FakeClients(s3), SPEC.sources(), stats))
    assert [g.id for g in granules] == [OLD_GID, GID]
    old = granules[0]
    assert {o.key.split("/")[0] + "/" for o in old.files} == {PROVISIONAL}
    assert len(old.files) == len(granules[1].files) == len(listing(GID).files)
    assert stats.unparsed_count == 0
    only = list(listing_mod.list_shard(SPEC, "OPERA_L3_DSWx-HLS_T59K", FakeClients(s3), SPEC.sources(),
                                       listing_mod.ShardStats(), [PROVISIONAL]))
    assert [g.id for g in only] == [OLD_GID]


def test_a_listing_of_some_prefixes_builds_a_delta(s3, tmp_path, capsys):
    run = RunDir(tmp_path, "inv-20261022T2300Z")
    summary = listing_mod.run_listing(SPEC, run, FakeClients(s3), SPEC.sources(), workers=2, shard_filter="59K",
                                      prefixes=[PROVISIONAL])
    assert summary["granules"] == 1 and summary["prefixes"] == [PROVISIONAL]
    done = read_done(run.listing(SPEC.dataset.value, "59K"))
    assert done["prefixes"] == [PROVISIONAL] and not done["complete"]   # reconcile skips it

    build = run_build(SPEC, run, None, BuildContext(run.run_id, run.listed_at), BuildOptions(tier=0, workers=2))
    assert build["docs"] == 1
    doc = next(read_jsonl_gz(run.docs(SPEC.dataset.value, "59K")))
    assert doc["id"] == OLD_GID and doc["daac_collection"] == "OPERA_L3_DSWX-HLS_V1"
    assert doc["urls"][0] == f"s3://{PODAAC_PROTECTED_BUCKET}/{PROVISIONAL}{OLD_GID}"
    assert doc["browse_urls"][0] == f"s3://{PODAAC_PUBLIC_BUCKET}/{PROVISIONAL}{OLD_GID}"
    assert doc["catalog"]["product_prefix"] == f"s3://{PODAAC_PROTECTED_BUCKET}/{PROVISIONAL}{OLD_GID}"
    assert all(f["key"].startswith(PROVISIONAL) for f in doc["catalog"]["files"])
    assert "problems" not in doc["catalog"]

    assert export.partial_listing(SPEC, run)
    argv = ["export", "--dataset", SPEC.dataset.value, "--workdir", str(tmp_path), "--run-id", run.run_id,
            "--from", "build"]
    assert cli.main(argv) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "delta" and out["doc_count"] == 1


def test_a_listing_of_every_prefix_builds_a_full_bundle(s3, tmp_path):
    run = RunDir(tmp_path, "inv-20261022T2300Z")
    listing_mod.run_listing(SPEC, run, FakeClients(s3), SPEC.sources(), workers=2, shard_filter="59K")
    path = run.listing(SPEC.dataset.value, "59K")
    done = read_done(path)
    assert done["prefixes"] == [CURRENT, PROVISIONAL] and done["complete"]
    assert not export.partial_listing(SPEC, run)
    build = run_build(SPEC, run, None, BuildContext(run.run_id, run.listed_at), BuildOptions(tier=0, workers=2))
    assert build["docs"] == 2
    urls = {d["id"]: d["urls"][0] for d in read_jsonl_gz(run.docs(SPEC.dataset.value, "59K"))}
    assert urls == {GID: f"s3://{PODAAC_PROTECTED_BUCKET}/{CURRENT}{GID}",
                    OLD_GID: f"s3://{PODAAC_PROTECTED_BUCKET}/{PROVISIONAL}{OLD_GID}"}
    # listings made before they recorded their prefixes covered the dataset as it was
    write_json(done_marker(path), {k: v for k, v in done.items() if k != "prefixes"})
    assert not export.partial_listing(SPEC, run)


def test_files_under_two_prefixes_are_a_problem():
    files = listing(GID).files
    moved = next(o for o in files if o.source == Source.PRODUCTS.value)
    files = [o for o in files if o is not moved] + [replace(moved, key=moved.key.replace(CURRENT, PROVISIONAL))]
    parsed = parse_granule(SPEC, GranuleListing(GID, SPEC.dataset.value, files))
    assert parsed.problems == [f"files under several bucket prefixes: {PROVISIONAL}, {CURRENT}"]
