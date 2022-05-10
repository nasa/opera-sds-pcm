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
async def test_query(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py " \
           "--s3bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--verbose " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--index-mode=query " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert len(results["success"]) > 0
    assert len(results["fail"]) == 0


@pytest.mark.asyncio
async def test_download_by_tile(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.upload_url_list_from_s3.__name__,
        lambda *args, **kwargs: None
    )

    args = "dummy.py " \
           "--s3bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--verbose " \
           "--index-mode=download " \
           "--tile-ids=T00000 " \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results is None


@pytest.mark.asyncio
async def test_download_by_tiles(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.upload_url_list_from_s3.__name__,
        lambda *args, **kwargs: None
    )

    args = "dummy.py " \
           "--s3bucket=dummy_bucket " \
           "--collection-shortname=dummy_collection_shortname " \
           "--verbose " \
           "--index-mode=download " \
           "--tile-ids T00000 T00001" \
           "".split()

    # ACT
    results = await data_subscriber.daac_data_subscriber.run(args)

    # ASSERT
    assert results is None


@contextmanager
def mock_token_ctx(*args):
    yield "test_token"


def patch_subscriber(monkeypatch):
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.get_data_subscriber_connection.__name__,
        lambda *args, **kwargs: MockDataSubscriberProductCatalog()
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
        data_subscriber.daac_data_subscriber.query_cmr.__name__,
        lambda *args, **kwargs: [
            "https://example.com/T00000.B01.tif",
            "https://example.com/T00001.B01.tif",
            "https://example.com/T00001.B02.tif",
            "https://example.com/T00002.B01.tif",
        ]
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.update_es_index.__name__,
        lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        data_subscriber.daac_data_subscriber,
        data_subscriber.daac_data_subscriber.submit_mozart_job.__name__,
        lambda *args, **kwargs: "dummy_job_id_" + str(random.randint(0, 100))
    )


class MockDataSubscriberProductCatalog:
    def get_all_undownloaded(self):
        return [
            {
                "_source": {
                    "url": "https://example.com/T00000.B01.tif"
                },
            },
            {
                "_source": {
                    "url": "https://example.com/T00001.B01.tif"
                },
            },
            {
                "_source": {
                    "url": "https://example.com/T00001.B02.tif"
                },
            },
            {
                "_source": {
                    "url": "https://example.com/T00002.B01.tif"
                },
            },
        ]
