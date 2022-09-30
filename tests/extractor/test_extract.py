import io
from unittest.mock import MagicMock

from pytest_mock import MockerFixture
from ruamel.yaml.util import RegExp

import extractor.extract
from extractor.extract import create_dataset_json


def test_extract(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("os.path.exists", side_effect=[
        True,  # dataset_dir
        False  # dataset_met_file
    ])
    mocker.patch("shutil.copyfile")
    mocker.patch("os.path.getsize")

    mocker.patch("extractor.extract.create_dataset_id", return_value="HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask")
    mocker.patch("extractor.extract.extract_metadata", return_value=(True, {}, {"type": "L2_HLS_L30"}, {}))
    mocker.patch("extractor.extract.create_dataset_json", return_value={})

    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        product_met_file := mocker.mock_open(read_data="").return_value,
        dataset_met_file := mocker.mock_open(read_data="").return_value,
    ]
    mocker.patch("builtins.open", mock_open)  # *.met.json

    # ACT
    dataset_dir = extractor.extract.extract(
        product="HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask.tif",
        product_types={},
        workspace="/data/work/jobs/1970/01/01/00/00/00/dummy_workspace_dir/"
    )

    # ASSERT
    assert dataset_dir == "/data/work/jobs/1970/01/01/00/00/00/dummy_workspace_dir/" + "HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask"


def test_extract_multiple(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("os.path.exists", side_effect=[
        True, False,  # dataset_dir, dataset_met_file
        True, False,  # dataset_dir, dataset_met_file
    ])
    mocker.patch("shutil.copyfile")
    mocker.patch("os.path.getsize")

    mocker.patch("extractor.extract.create_dataset_id", side_effect=[
        "granule_1.Fmask.tif",
        "granule_1.B01.tif"
    ])
    mocker.patch("extractor.extract.extract_metadata", return_value=(True, {}, {"type": "L2_HLS_L30"}, {}))
    mocker.patch("extractor.extract.create_dataset_json", return_value={})
    mocker.patch("builtins.open", mocker.mock_open())  # *.met.json

    # ACT
    products = [
        "granule_1.Fmask.tif",
        "granule_1.tif"
    ]
    dataset_dirs = [
        extractor.extract.extract(
            product=product,
            product_types={},
            workspace="/data/work/jobs/1970/01/01/00/00/00/dummy_workspace_dir/"
        ) for product in products
    ]

    # ASSERT
    assert dataset_dirs == [
        "/data/work/jobs/1970/01/01/00/00/00/dummy_workspace_dir/" + "granule_1.Fmask.tif",
        "/data/work/jobs/1970/01/01/00/00/00/dummy_workspace_dir/" + "granule_1.B01.tif"
    ]


def test_extract_metadata(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("shutil.copyfile")
    mocker.patch("os.path.getsize")

    # ACT
    found, product_met, ds_met, alt_ds_met = extractor.extract.extract_metadata(
        product="HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask/HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask.tif",
        product_types={
            "L2_HLS_L30": {
                "Pattern": RegExp("(?P<product_shortname>HLS[.]L30)[.](?P<tile_id>T[^\W_]{5})[.](?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.](?P<collection_version>v\d+[.]\d+)[.](?P<band_or_qa>[^\W_]+)[.](?P<format>tif)$"),
                "Strip_File_Extension": True,
                "Extractor": "extractor.FilenameRegexMetExtractor",
                "Dataset_Keys": {},
                "Configuration": {
                    "Date_Time_Patterns": ['%Y%jT%H%M%S'],
                    "Date_Time_Keys": ['acquisition_ts'],
                    "Dataset_Version_Key": 'collection_version'
                }
            }
        }
    )

    # ASSERT
    assert found is True
    assert product_met
    assert ds_met == {"type": "L2_HLS_L30"}
    assert alt_ds_met == {}


def test_create_dataset_json__override_version_using_config_key():
    # ARRANGE
    product_metadata = {"dataset_version": "v1.2.3"}

    # ACT
    dataset_json = create_dataset_json(product_metadata, ds_met={}, alt_ds_met={})

    # ASSERT
    assert dataset_json["version"] == "v1.2.3"


def test_create_dataset_json__override_version_using_versionID():
    # ARRANGE
    product_metadata = {"VersionID": "VersionID"}

    # ACT
    dataset_json = create_dataset_json(product_metadata, ds_met={}, alt_ds_met={})

    # ASSERT
    assert dataset_json["version"] == "VersionID"


def test_create_dataset_json__default_version():
    # ARRANGE
    product_metadata = {}

    # ACT
    dataset_json = create_dataset_json(product_metadata, ds_met={}, alt_ds_met={})

    # ASSERT
    assert dataset_json["version"] == "1"
