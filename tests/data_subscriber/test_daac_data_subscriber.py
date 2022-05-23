import random
from contextlib import contextmanager
from pathlib import Path

import pytest

import data_subscriber.daac_data_subscriber


def setup_module():
    # create dummy `_job.json` to mimic runtime environment
    job_context = Path("_job.json")
    job_context.unlink(missing_ok=True)
    with job_context.open("w") as f:
        f.write(
            """
            {
                "job_info": {
                    "job_payload": {
                        "payload_task_id": "123456"
                    }
                }
            }
            """
        )


def teardown_module():
    # delete dummy `_job.json`
    job_context = Path("_job.json")
    job_context.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_full(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py full " \
           "--isl-bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--isl-bucket=dummy_bucket " \
           "--transfer-protocol=not-https " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None
    assert results["download"] is None


@pytest.mark.asyncio
async def test_query(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--isl-bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None


@pytest.mark.asyncio
async def test_query_chunked(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--isl-bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--chunk-size=1 " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert len(results["query"]["success"]) > 0
    assert len(results["query"]["fail"]) == 0


@pytest.mark.asyncio
async def test_query_no_schedule_download(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--isl-bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--chunk-size=1 " \
           "--no-schedule-download " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None


@pytest.mark.asyncio
async def test_query_smoke_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--isl-bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--chunk-size=1 " \
           "--smoke-run " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert len(results["query"]["success"]) == 1
    assert len(results["query"]["fail"]) == 0


@pytest.mark.asyncio
async def test_download(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--transfer-protocol=not-https " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tile(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--tile-ids=T00000 " \
           "--transfer-protocol=not-https " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tiles(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--tile-ids T00000 T00001 " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_https(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_https_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--tile-ids=T00000 " \
           "--transfer-protocol=https " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tiles_smoke_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--tile-ids T00000 T00001 " \
           "--smoke-run " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tiles_dry_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    args = "dummy.py download " \
           "--isl-bucket=dummy_bucket " \
           "--tile-ids T00000 T00001 " \
           "--dry-run " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@contextmanager
def mock_token_ctx(*args):
    yield "test_token"


def patch_subscriber(monkeypatch):
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.get_hls_catalog_connection.__name__,
        lambda *args, **kwargs: MockDataSubscriberProductCatalog()
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.get_hls_spatial_catalog_connection.__name__,
        lambda *args, **kwargs: MockHlsSpatialCatalog()
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.setup_earthdata_login_auth.__name__,
        lambda *args: ("test_username", "test_password")
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.token_ctx.__name__,
        mock_token_ctx
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.request_search.__name__,
        lambda *args, **kwargs: (
            [
                {
                    "granule_id": "dummy_granule_id",
                    "related_urls": [
                        "https://example.com/T00000.B01.tif",
                        "https://example.com/T00001.B01.tif",
                        "https://example.com/T00001.B02.tif",
                        "https://example.com/T00002.B01.tif",
                    ]
                }
            ],
            False  # search_after
        )
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.update_url_index.__name__,
        lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.submit_mozart_job.__name__,
        lambda *args, **kwargs: "dummy_job_id_" + str(random.randint(0, 100))
    )


def mock_get_aws_creds(monkeypatch):
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.get_aws_creds.__name__,
        lambda *args, **kwargs: {
            "accessKeyId": None,
            "secretAccessKey": None,
            "sessionToken": None
        }
    )


def mock_https_transfer(monkeypatch):
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.https_transfer.__name__,
        lambda *args, **kwargs: {}
    )


def mock_s3_transfer(monkeypatch):
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.s3_transfer.__name__,
        lambda *args, **kwargs: {}
    )


def mock_boto3(monkeypatch):
    class MockSession:
        def __init__(self, *args, **kwargs):
            pass

        def client(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber.boto3,
        data_subscriber.daac_data_subscriber.boto3.Session.__name__,
        MockSession
    )


class MockDataSubscriberProductCatalog:
    def get_all_undownloaded(self):
        return [
            {
                "https_url": "https://example.com/T00000.B01.tif",
                "s3_url": "s3://example/T00000.B01.tif"
            },
            {
                "https_url": "https://example.com/T00001.B01.tif",
                "s3_url": "s3://example/T00001.B01.tif"
            },
            {
                "https_url": "https://example.com/T00001.B02.tif",
                "s3_url": "s3://example/T00001.B02.tif"
            },
            {
                "https_url": "https://example.com/T00002.B01.tif",
                "s3_url": "s3://example/T00002.B01.tif"
            },
        ]

    def product_is_downloaded(self, *args, **kwargs):
        return False

    def mark_product_as_downloaded(self, *args, **kwargs):
        pass


class MockHlsSpatialCatalog:
    def process_granule(self, *args, **kwargs):
        pass
