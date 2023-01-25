import random
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data_subscriber import daac_data_subscriber, download_util, query_util, token_util, url_util
import data_subscriber.hls


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
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_download_product_using_https = MagicMock(return_value=Path("downloads/T00003/T00003.B01").resolve())

    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download_util,
        download_util.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )

    mock_download_product_using_s3 = MagicMock(side_effect=[
        Path("downloads/T00000/T00000.B01").resolve(),
        Path("downloads/T00001/T00001.B01").resolve(),
        Path("downloads/T00001/T00001.B02").resolve(),
        Path("downloads/T00002/T00002.B01").resolve()
    ])
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    mock_extract = MagicMock(side_effect=[
        "extracts/T00000/T00000.B01",
        "extracts/T00001/T00001.B01",
        "extracts/T00001/T00001.B02",
        "extracts/T00001/T00002.B02",
        "extracts/T00003/T00003.B01",
    ])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)

    args = "dummy.py full " \
           "--collection-shortname=HLSS30 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None
    assert results["download"] is None


@pytest.mark.asyncio
async def test_query(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--collection-shortname=HLSS30 " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None


@pytest.mark.asyncio
async def test_query_chunked(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--collection-shortname=HLSS30 " \
           "--chunk-size=1 " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert len(results["query"]["success"]) > 0
    assert len(results["query"]["fail"]) == 0


@pytest.mark.asyncio
async def test_query_no_schedule_download(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--collection-shortname=HLSS30 " \
           "--chunk-size=1 " \
           "--no-schedule-download " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["query"] is None


@pytest.mark.asyncio
async def test_query_smoke_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)

    args = "dummy.py query " \
           "--collection-shortname=HLSS30 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--chunk-size=1 " \
           "--smoke-run " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert len(results["query"]["success"]) == 1
    assert len(results["query"]["fail"]) == 0


@pytest.mark.asyncio
async def test_download(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_download_product_using_https = MagicMock(return_value=Path("downloads/T00003/T00003.B01").resolve())

    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download_util,
        download_util.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )


    mock_download_product_using_s3 = MagicMock(side_effect=[
        Path("downloads/T00000/T00000.B01").resolve(),
        Path("downloads/T00001/T00001.B01").resolve(),
        Path("downloads/T00001/T00001.B02").resolve(),
        Path("downloads/T00002/T00002.B01").resolve()
    ])
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    mock_extract = MagicMock([
        "extracts/T00000/T00000.B01",
        "extracts/T00001/T00001.B01",
        "extracts/T00001/T00001.B02",
        "extracts/T00001/T00002.B02",
        "extracts/T00003/T00003.B01",
    ])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)

    args = "dummy.py download " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tile(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_extract = MagicMock(side_effect=["extracts/T00000/T00000.B01"])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)

    mock_download_product_using_s3 = MagicMock(side_effect=[Path("downloads/T00000/T00000.B01").resolve()])
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    args = "dummy.py download " \
           "--batch-ids=T00000 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tiles(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_download_product_using_s3 = MagicMock(side_effect=[
        Path("downloads/T00000/T00000.B01").resolve(),
        Path("downloads/T00000/T00001.B01").resolve(),
        Path("downloads/T00001/T00001.B02").resolve(),
        Path("downloads/T00001/T00002.B02").resolve()
    ])
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    mock_extract = MagicMock(side_effect=[
        "extracts/T00000/T00000.B01",
        "extracts/T00001/T00001.B01",
        "extracts/T00001/T00001.B02",
        "extracts/T00001/T00002.B02",
    ])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)

    args = "dummy.py download " \
           "--batch-ids T00000 T00001 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None

@pytest.mark.asyncio
async def test_download_https(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_https_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_extract = MagicMock(side_effect=["extracts/T00003/T00003.Fmask"])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)
    monkeypatch.setattr(
        download_util,
        download_util.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )

    args = "dummy.py download " \
           "--batch-ids=T00003 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None

@pytest.mark.asyncio
async def test_download_by_tiles_smoke_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_extract = MagicMock(side_effect=[
        "extracts/T00000/T00000.Fmask",
        "extracts/T00001/T00001.Fmask"
    ])
    mock_extract_metadata(monkeypatch, mock_extract)
    mock_create_merged_files(monkeypatch)

    args = "dummy.py download " \
           "--batch-ids T00000 T00001 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--smoke-run " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


@pytest.mark.asyncio
async def test_download_by_tiles_dry_run(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)
    mock_create_merged_files(monkeypatch)

    args = "dummy.py download " \
           "--batch-ids T00000 T00001 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--dry-run " \
           "".split()

    # ACT
    results = await daac_data_subscriber.run(args)

    # ASSERT
    assert results["download"] is None


def test_download_granules_using_https(monkeypatch):
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)

    mock_download_product_using_https = MagicMock(return_value=Path("downloads/granule1/granule1.Fmask.tif").resolve())
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download_util.extractor.extract,
        download_util.extractor.extract.extract.__name__,
        MagicMock(return_value="extracts/granule1/granule1.Fmask")
    )
    mock_create_merged_files(monkeypatch)

    mock_es_conn = MagicMock()
    mock_es_conn.product_is_downloaded.return_value = False

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True

    download_util.download_granules(None, mock_es_conn, {
        "granule1": ["http://example.com/granule1.Fmask.tif"]
    }, Args(), None, None)

    mock_download_product_using_https.assert_called()


def test_download_granules_using_s3(monkeypatch):
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)

    mock_download_product_using_s3 = MagicMock(return_value=Path("downloads/granule1/granule1.Fmask.tif").resolve())
    monkeypatch.setattr(
        download_util,
        download_util.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    monkeypatch.setattr(
        download_util.extractor.extract,
        download_util.extractor.extract.extract.__name__,
        MagicMock(return_value="extracts/granule1/granule1.Fmask")
    )
    mock_create_merged_files(monkeypatch)

    mock_es_conn = MagicMock()
    mock_es_conn.product_is_downloaded.return_value = False

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True

    download_util.download_granules(None, mock_es_conn, {
        "granule1": ["s3://example.com/granule1.Fmask.tif"]
    }, Args(), None, None)

    mock_download_product_using_s3.assert_called()


def test_download_from_asf(monkeypatch):
    # ARRANGE
    patch_subscriber_io(monkeypatch)

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True
        provider = "ASF"

    # mock ASF download functions
    monkeypatch.setattr(
        download_util,
        download_util._handle_url_redirect.__name__,
        MagicMock()
    )

    mock_extract_one_to_one = MagicMock()
    monkeypatch.setattr(
        download_util,
        download_util.extract_one_to_one.__name__,
        mock_extract_one_to_one
    )

    monkeypatch.setattr(
        download_util.stage_orbit_file,
        download_util.stage_orbit_file.get_parser.__name__,
        MagicMock()
    )
    mock_stage_orbit_file = MagicMock()
    monkeypatch.setattr(
        download_util.stage_orbit_file,
        download_util.stage_orbit_file.main.__name__,
        mock_stage_orbit_file
    )

    # ACT
    download_util.download_from_asf(session=MagicMock(), es_conn=MagicMock(), downloads=[{"https_url": "https://www.example.com/dummy_slc_product.zip"}], args=Args(), token=None, job_id=None)

    # ASSERT
    mock_extract_one_to_one.assert_called_once()
    mock_stage_orbit_file.assert_called_once()


def mock_token(*args):
    return "test_token"


def patch_subscriber(monkeypatch):
    monkeypatch.setattr(
        daac_data_subscriber,
        daac_data_subscriber.get_hls_catalog_connection.__name__,
            MagicMock(
                return_value=MagicMock(
                get_all_between=MagicMock(
                    return_value=[
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
                        {
                            "https_url": "https://example.com/T00003.B01.tif",
                        }
                    ]
                )
            )
        )
    )
    monkeypatch.setattr(
        query_util,
        query_util.get_hls_spatial_catalog_connection.__name__,
        MagicMock(
            return_value=MagicMock(process_granule=MagicMock())
        )
    )
    monkeypatch.setattr(
        query_util,
        query_util.get_slc_spatial_catalog_connection.__name__,
        MagicMock(
            return_value=MagicMock(process_granule=MagicMock())
        )
    )
    monkeypatch.setattr(
        daac_data_subscriber.netrc,
        daac_data_subscriber.netrc.netrc.__name__,
        MagicMock(
            return_value=MagicMock(
                authenticators=MagicMock(
                    return_value=(
                        "dummy_username",
                        "dummy_host",
                        "dummy_password",
                    )
                )
            )
        )
    )
    monkeypatch.setattr(
        daac_data_subscriber,
        daac_data_subscriber.supply_token.__name__,
        mock_token
    )
    monkeypatch.setattr(
        query_util,
        query_util._request_search.__name__,
        MagicMock(return_value=(
            [
                {
                    "granule_id": "dummy_granule_id",
                    "filtered_urls": [
                        "https://example.com/T00000.B02.tif",
                    ],
                    "related_urls": [
                        "https://example.com/T00000.B02.tif",
                    ],
                    "identifier": "S2A_dummy",
                    "temporal_extent_beginning_datetime": datetime.now().isoformat(),
                    "revision_date": datetime.now().isoformat(),
                },
                {
                    "granule_id": "dummy_granule_id_2",
                    "filtered_urls": [
                        "https://example.com/T00001.B02.tif",
                        "https://example.com/T00001.B03.tif",
                    ],
                    "related_urls": [
                        "https://example.com/T00001.B02.tif",
                        "https://example.com/T00001.B03.tif",
                    ],
                    "identifier": "S2A_dummy",
                    "temporal_extent_beginning_datetime": datetime.now().isoformat(),
                    "revision_date": datetime.now().isoformat(),
                },
                {
                    "granule_id": "dummy_granule_id_3",
                    "filtered_urls": [
                        "https://example.com/T00002.B02.tif",
                    ],
                    "related_urls": [
                        "https://example.com/T00002.B02.tif",
                    ],
                    "identifier": "S2A_dummy",
                    "temporal_extent_beginning_datetime": datetime.now().isoformat(),
                    "revision_date": datetime.now().isoformat(),
                },
                {
                    "granule_id": "dummy_granule_id_4",
                    "filtered_urls": [
                        "https://example.com/T00003.B01.tif",
                    ],
                    "related_urls": [
                        "https://example.com/T00003.B01.tif",
                    ],
                    "identifier": "S2A_dummy",
                    "temporal_extent_beginning_datetime": datetime.now().isoformat(),
                    "revision_date": datetime.now().isoformat(),
                }
            ],
            False  # search_after
        ))
    )
    monkeypatch.setattr(
        daac_data_subscriber,
        daac_data_subscriber.update_url_index.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        query_util,
        query_util.submit_mozart_job.__name__,
        MagicMock(return_value="dummy_job_id_" + str(random.randint(0, 100)))
    )


def mock_extract_metadata(monkeypatch, mock_extract):
    monkeypatch.setattr(
        download_util.extractor.extract,
        download_util.extractor.extract.extract.__name__,
        mock_extract
    )


def mock_create_merged_files(monkeypatch):
    monkeypatch.setattr(
        download_util.product2dataset,
        download_util.product2dataset.merge_dataset_met_json.__name__,
        MagicMock(return_value=(1, {"dataset_version": "v2.0"}))
    )
    monkeypatch.setattr(
        download_util.extractor.extract,
        download_util.extractor.extract.create_dataset_json.__name__,
        MagicMock(return_value={})
    )


def patch_subscriber_io(monkeypatch):
    """Patch I/O operations from smart_open, shutil, and json modules.

    Patched functions will do no-op, returning None.
    """
    mock_smart_open(monkeypatch)
    mock_path_package(monkeypatch)
    mock_shutil_package(monkeypatch)
    mock_json_package(monkeypatch)


def mock_smart_open(monkeypatch):
    mock_open = MagicMock()
    monkeypatch.setattr(
        download_util,
        download_util.open.__name__,
        MagicMock(return_value=mock_open)
    )


def mock_path_package(monkeypatch):
    monkeypatch.setattr(
        daac_data_subscriber.Path,
        daac_data_subscriber.Path.mkdir.__name__,
        MagicMock()
    )


def mock_shutil_package(monkeypatch):
    monkeypatch.setattr(
        download_util.shutil,
        download_util.shutil.rmtree.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        download_util.shutil,
        download_util.shutil.copy.__name__,
        MagicMock()
    )


def mock_json_package(monkeypatch):
    monkeypatch.setattr(
        daac_data_subscriber.json,
        daac_data_subscriber.json.dump.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        daac_data_subscriber.json,
        daac_data_subscriber.json.load.__name__,
        MagicMock()
    )


def mock_get_aws_creds(monkeypatch):
    monkeypatch.setattr(
        download_util,
        download_util._get_aws_creds.__name__,
        MagicMock(return_value={
            "accessKeyId": None,
            "secretAccessKey": None,
            "sessionToken": None
        })
    )


def mock_https_transfer(monkeypatch):
    monkeypatch.setattr(
        download_util,
        download_util._https_transfer.__name__,
        MagicMock(return_value={})
    )


def mock_s3_transfer(monkeypatch):
    monkeypatch.setattr(
        download_util,
        download_util._s3_transfer.__name__,
        MagicMock(return_value={})
    )
    monkeypatch.setattr(
        download_util,
        download_util._s3_download.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        download_util,
        download_util._s3_upload.__name__,
        MagicMock(return_value="dummy_target_key")
    )


def mock_boto3(monkeypatch):
    class MockSession:
        def __init__(self, *args, **kwargs):
            pass

        def client(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        download_util.boto3,
        download_util.boto3.Session.__name__,
        MockSession
    )