import random
import sys
from datetime import datetime
from pathlib import Path

import pytest
import smart_open

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()
from mock import MagicMock

from data_subscriber import daac_data_subscriber, download, query, cmr
from data_subscriber.download import DaacDownload
from data_subscriber.lpdaac_download import DaacDownloadLpdaac
from product2dataset import product2dataset


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

def test_doc_creation():
    pass

@pytest.mark.asyncio
async def test_full(monkeypatch):
    # ARRANGE
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)
    mock_get_aws_creds(monkeypatch)
    #mock_s3_transfer(monkeypatch)
    mock_boto3(monkeypatch)

    mock_download_product_using_https = MagicMock(return_value=Path("downloads/T00003/T00003.B01").resolve())

    monkeypatch.setattr(
        DaacDownloadLpdaac,
        DaacDownloadLpdaac.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download,
        download.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )

    mock_download_product_using_s3 = MagicMock(side_effect=[
        Path("downloads/T00000/T00000.B01").resolve(),
        Path("downloads/T00001/T00001.B01").resolve(),
        Path("downloads/T00001/T00001.B02").resolve(),
        Path("downloads/T00002/T00002.B01").resolve()
    ])
    monkeypatch.setattr(
        DaacDownload,
        DaacDownload.download_product_using_s3.__name__,
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
           "--transfer-protocol=auto " \
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
        download,
        download.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download,
        download.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )

    mock_download_product_using_s3 = MagicMock(side_effect=[
        Path("downloads/T00000/T00000.B01").resolve(),
        Path("downloads/T00001/T00001.B01").resolve(),
        Path("downloads/T00001/T00001.B02").resolve(),
        Path("downloads/T00002/T00002.B01").resolve()
    ])
    monkeypatch.setattr(
        download,
        download.download_product_using_s3.__name__,
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
           "--transfer-protocol=auto " \
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
        download,
        download.download_product_using_s3.__name__,
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
        download,
        download.download_product_using_s3.__name__,
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
        download,
        download.SessionWithHeaderRedirection.__name__,
        MagicMock()
    )

    args = "dummy.py download " \
           "--batch-ids=T00003 " \
           "--start-date=1970-01-01T00:00:00Z " \
           "--end-date=1970-01-01T00:00:00Z " \
           "--transfer-protocol=https " \
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

# TODO: Either find way to get this working or re-design this to reflect the new code design
@pytest.mark.skip
def test_download_granules_using_https(monkeypatch):
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)

    mock_download_product_using_https = MagicMock(return_value=Path("downloads/granule1/granule1.Fmask.tif").resolve())
    monkeypatch.setattr(
        DaacDownloadLpdaac,
        DaacDownloadLpdaac.download_product_using_https.__name__,
        mock_download_product_using_https
    )

    monkeypatch.setattr(
        download.extractor.extract,
        download.extractor.extract.extract.__name__,
        MagicMock(return_value="extracts/granule1/granule1.Fmask")
    )
    mock_create_merged_files(monkeypatch)

    mock_es_conn = MagicMock()

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True
        transfer_protocol = "https"

    daac_download = DaacDownloadLpdaac("LPCLOUD")
    daac_download.downloads_dir = Path("downloads")
    daac_download.downloads_dir.mkdir(exist_ok=True)

    daac_download.perform_download(None, mock_es_conn, [{
        "granule_id": "granule1",
        "_id": "granule_1",
        "revision_id": 1,
        "https_url": ["http://example.com/granule1.Fmask.tif"]
    }], Args(), None, None)

    mock_download_product_using_https.assert_called()

# TODO: Either find way to get this working or re-design this to reflect the new code design
@pytest.mark.skip
def test_download_granules_using_s3(monkeypatch):
    patch_subscriber(monkeypatch)
    patch_subscriber_io(monkeypatch)

    mock_download_product_using_s3 = MagicMock(return_value=Path("downloads/granule1/granule1.Fmask.tif").resolve())
    monkeypatch.setattr(
        download,
        download.download_product_using_s3.__name__,
        mock_download_product_using_s3
    )

    monkeypatch.setattr(
        download.extractor.extract,
        download.extractor.extract.extract.__name__,
        MagicMock(return_value="extracts/granule1/granule1.Fmask")
    )
    mock_create_merged_files(monkeypatch)

    mock_es_conn = MagicMock()

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True
        transfer_protocol = "s3"

    download.download_granules(None, mock_es_conn, {
        "granule1": ["s3://example.com/granule1.Fmask.tif"]
    }, Args(), None, None)

    mock_download_product_using_s3.assert_called()

# TODO: Either find way to get this working or re-design this to reflect the new code design
@pytest.mark.skip
def test_download_from_asf(monkeypatch):
    # ARRANGE
    patch_subscriber_io(monkeypatch)

    from dataclasses import dataclass

    @dataclass
    class Args:
        dry_run = False
        smoke_run = True
        provider = "ASF"
        transfer_protocol = "https"

    # mock ASF download functions
    monkeypatch.setattr(
        download,
        download._handle_url_redirect.__name__,
        MagicMock()
    )

    mock_extract_one_to_one = MagicMock()
    monkeypatch.setattr(
        download,
        download.extract_one_to_one.__name__,
        mock_extract_one_to_one
    )

    monkeypatch.setattr(
        download,
        download.update_pending_dataset_with_index_name.__name__,
        MagicMock()
    )

    monkeypatch.setattr(
        download.stage_orbit_file,
        download.stage_orbit_file.get_parser.__name__,
        MagicMock()
    )

    mock_stage_orbit_file = MagicMock()
    monkeypatch.setattr(
        download.stage_orbit_file,
        download.stage_orbit_file.main.__name__,
        mock_stage_orbit_file
    )

    mock_stage_ionosphere_file = MagicMock()
    monkeypatch.setattr(
        download.ionosphere_download,
        download.ionosphere_download.download_ionosphere_correction_file.__name__,
        mock_stage_ionosphere_file
    )

    mock_stage_ionosphere_file_url = MagicMock()
    monkeypatch.setattr(
        download.ionosphere_download,
        download.ionosphere_download.get_ionosphere_correction_file_url.__name__,
        mock_stage_ionosphere_file_url
    )

    monkeypatch.setattr(
        download.ionosphere_download,
        download.ionosphere_download.generate_ionosphere_metadata.__name__,
        MagicMock()
    )

    monkeypatch.setattr(
        download,
        download.update_pending_dataset_metadata_with_ionosphere_metadata.__name__,
        MagicMock()
    )

    # ACT
    download.download_from_asf(session=MagicMock(),
                               es_conn=MagicMock(),
                               downloads=[
                                   {
                                       "https_url": "https://www.example.com/dummy_slc_product.zip",
                                       "intersects_north_america": True,
                                       "processing_mode": "historical"
                                   }
                               ],
                               args=Args(),
                               token=None,
                               job_id=None)

    # ASSERT
    mock_extract_one_to_one.assert_called_once()
    mock_stage_orbit_file.assert_called_once()
    mock_stage_ionosphere_file.assert_called_once()
    mock_stage_ionosphere_file_url.assert_called_once()


def mock_token(*args):
    return "test_token"


def patch_subscriber(monkeypatch):
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
        cmr,
        cmr._request_search_cmr_granules.__name__,
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
        query,
        query.submit_mozart_job.__name__,
        MagicMock(return_value="dummy_job_id_" + str(random.randint(0, 100)))
    )


def mock_extract_metadata(monkeypatch, mock_extract):
    monkeypatch.setattr(
        download.extractor.extract,
        download.extractor.extract.extract.__name__,
        mock_extract
    )


def mock_create_merged_files(monkeypatch):
    monkeypatch.setattr(
        product2dataset,
        product2dataset.merge_dataset_met_json.__name__,
        MagicMock(return_value=(1, {"dataset_version": "v2.0", "ProductType": "dummy_product_type"}))
    )
    monkeypatch.setattr(
        download.extractor.extract,
        download.extractor.extract.create_dataset_json.__name__,
        MagicMock(return_value={"version": "v2.0"})
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
        smart_open,
        smart_open.open.__name__,
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
        download.shutil,
        download.shutil.rmtree.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        download.shutil,
        download.shutil.copy.__name__,
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
        DaacDownloadLpdaac,
        DaacDownloadLpdaac._get_aws_creds.__name__,
        MagicMock(return_value={
            "accessKeyId": None,
            "secretAccessKey": None,
            "sessionToken": None
        })
    )


def mock_https_transfer(monkeypatch):
    monkeypatch.setattr(
        DaacDownload,
        DaacDownload._https_transfer.__name__,
        MagicMock(return_value={})
    )


def mock_s3_transfer(monkeypatch):
    monkeypatch.setattr(
        DaacDownload,
        DaacDownload._s3_transfer.__name__,
        MagicMock(return_value={})
    )
    monkeypatch.setattr(
        DaacDownload,
        DaacDownload._s3_download.__name__,
        MagicMock()
    )
    monkeypatch.setattr(
        DaacDownload,
        DaacDownload._s3_upload.__name__,
        MagicMock(return_value="dummy_target_key")
    )


def mock_boto3(monkeypatch):
    class MockSession:
        def __init__(self, *args, **kwargs):
            pass

        def client(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        download.boto3,
        download.boto3.Session.__name__,
        MockSession
    )
