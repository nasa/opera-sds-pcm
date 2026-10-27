import io
import json

import boto3
import h5py
import pytest
from moto import mock_aws

from tools.ops.daac_catalog import cli
from tools.ops.daac_catalog.creds import CredsMode, S3Clients, _botocore_metadata
from tools.ops.daac_catalog.registry import DATASETS, Dataset, Source
from tools.ops.daac_catalog.tier2 import S3RangeFile, cslc_acquisition_time, read_disp_s1_runconfig, \
    runconfig_inputs

RUNCONFIG = """input_file_group:
  cslc_file_list:
  - /home/mamba/input_dir/OPERA_L2_CSLC-S1_T063-133239-IW3_20181212T000533Z_20240705T124735Z_S1A_VV_v1.1.h5
  - /home/mamba/input_dir/OPERA_L2_COMPRESSED-CSLC-S1_F16655_T063-133239-IW3_20170726T000000Z_20160719T000000Z_20170726T000000Z_20250807T230655Z_VV_v1.0.h5
  frame_id: 16655
"""


def _h5_bytes() -> bytes:
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as h5:
        h5.create_dataset("displacement", data=list(range(200_000)))
        h5.create_dataset("metadata/pge_runconfig", data=RUNCONFIG)
    return buffer.getvalue()


def test_runconfig_over_ranged_reads():
    data = _h5_bytes()
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(Bucket="bkt", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        s3.put_object(Bucket="bkt", Key="p.nc", Body=data)
        text, stats = read_disp_s1_runconfig(s3, "bkt", "p.nc")
    assert text == RUNCONFIG
    assert 0 < stats["bytes_fetched"] <= len(data)
    cslc, ccslc = runconfig_inputs(text)
    assert cslc == ["OPERA_L2_CSLC-S1_T063-133239-IW3_20181212T000533Z_20240705T124735Z_S1A_VV_v1.1.h5"]
    assert len(ccslc) == 1 and ccslc[0].startswith("OPERA_L2_COMPRESSED-CSLC-S1_")


def test_range_file_seek_and_read():
    data = bytes(range(256)) * 50
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(Bucket="bkt", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        s3.put_object(Bucket="bkt", Key="k", Body=data)
        g = S3RangeFile(s3, "bkt", "k", block_size=1000, max_blocks=8)
        g.prefetch(0, 2500)
        assert g.requests == 1 and g.read(2000) == data[:2000] and g.requests == 1
        assert g.read(1000) == data[2000:3000] and g.requests == 2   # third block was partial, fetched on demand
        g.prefetch(len(data) - 1500, len(data))                     # widened to the block boundary
        g.seek(len(data) - 1800)
        assert g.read(1800) == data[-1800:] and g.requests == 3
        f = S3RangeFile(s3, "bkt", "k", block_size=1000, max_blocks=2)
        f.seek(995)
        assert f.read(10) == data[995:1005]
        f.seek(-5, io.SEEK_END)
        assert f.read(100) == data[-5:]
        assert f.read(1) == b""
        assert f.requests == 3


def test_cslc_acquisition_time():
    t = cslc_acquisition_time("OPERA_L2_CSLC-S1_T063-133239-IW3_20181212T000533Z_20240705T124735Z_S1A_VV_v1.1.h5")
    assert t.isoformat() == "2018-12-12T00:05:33+00:00"
    assert cslc_acquisition_time("OPERA_L2_COMPRESSED-CSLC-S1_F1_T1_x") is None


def test_creds_modes():
    asf, podaac = DATASETS[Dataset.L2_CSLC_S1], DATASETS[Dataset.L3_DSWX_HLS]
    auto = S3Clients(CredsMode.AUTO)
    assert not auto.uses_daac_credentials(asf, Source.PRODUCTS)
    assert not auto.uses_daac_credentials(asf, Source.UMMG)
    assert auto.uses_daac_credentials(asf, Source.BROWSE)
    assert auto.uses_daac_credentials(podaac, Source.PRODUCTS)
    assert not S3Clients(CredsMode.ROLE).uses_daac_credentials(podaac, Source.BROWSE)
    assert S3Clients(CredsMode.DAAC).uses_daac_credentials(asf, Source.PRODUCTS)
    meta = _botocore_metadata({"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t",
                               "expiration": "2026-10-20 22:11:31+00:00"})
    assert meta["expiry_time"] == "2026-10-20T22:11:31+00:00"


def test_daac_client_uses_refreshable_credentials():
    calls = []

    def fetcher(endpoint):
        calls.append(endpoint)
        return {"accessKeyId": "AKIA", "secretAccessKey": "s", "sessionToken": "t",
                "expiration": "2099-01-01 00:00:00+00:00"}

    clients = S3Clients(CredsMode.AUTO, fetcher=fetcher)
    client = clients.for_source(DATASETS[Dataset.L3_DSWX_S1], Source.PRODUCTS)
    assert client is clients.for_source(DATASETS[Dataset.L3_DSWX_HLS], Source.BROWSE)
    assert calls == ["https://archive.podaac.earthdata.nasa.gov/s3credentials"]


def test_cli_parser_and_plan(capsys):
    parser = cli.build_parser()
    args = parser.parse_args(["build", "--dataset", "L3_DISP_S1", "--tier", "2", "--parquet"])
    assert args.tier == 2 and args.parquet and args.creds == "auto"
    assert cli.main(["plan", "--dataset", "L2_RTC_S1,L4_TROPO"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert [r["dataset"] for r in rows] == ["L2_RTC_S1", "L4_TROPO"]
    assert rows[0]["shards"] == 175 and rows[0]["first_shards"] == ["T001-", "T002-", "T003-"]


def test_cli_list_dry_run(tmp_path, capsys):
    assert cli.main(["list", "--dataset", "L3_DSWx_S1", "--workdir", str(tmp_path), "--dry-run",
                     "--run-id", "inv-20261020T2100Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    result = out["results"][0]
    assert result["shards"] == 1200 and result["uses_daac_credentials"] == ["products", "browse"]


def test_cli_requires_dataset():
    with pytest.raises(SystemExit):
        cli.main(["plan"])
