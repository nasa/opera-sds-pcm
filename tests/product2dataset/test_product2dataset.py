from unittest.mock import MagicMock, Mock

from pytest_mock import MockerFixture

import product2dataset.product2dataset


def test_convert__when_L3_HLS_PGE__adds_PST_metadata(mocker: MockerFixture):
    """Tests that PST metadata such as input granule ID and output product URLs are added to the root of the dataset metadata."""
    # ARRANGE
    create_mock_PGEOutputsConf(mocker)
    create_mock_SettingsConf(mocker)

    mocker.patch("product2dataset.product2dataset.process_outputs", return_value={
        "Primary": {
            "dummy_product": {
                "hashcheck": False
            }
        },
        "Secondary": {},
        "Optional": {}
    })

    extract_mock = Mock()
    extract_mock.extract.return_value = "dir1/dir2/dummy_product"
    extract_mock.PRODUCT_TYPES_KEY = "PRODUCT_TYPES"
    mocker.patch("product2dataset.product2dataset.extract", extract_mock)

    mocker.patch("product2dataset.product2dataset.glob.iglob", return_value=["dir1/dir2/dummy_product/dummy_product.met.json"])
    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        met_json := mocker.mock_open(read_data="""
            {
                "FileSize": 0
            }
        """).return_value,
        jobs_json := mocker.mock_open(read_data="""
            {
                "params": {
                    "dataset_type": "L2_HLS_S30"
                },
                "context": {
                    "container_specification": {
                        "version": "v1.2.3"
                    },
                    "job_specification": {
                        "dependency_images": [
                            {
                                "container_image_name": "opera_pge/dswx_hls:1.0.0-rc.1.0"
                            }
                        ]
                    }
                }
            }
        """).return_value,
        datasets_json := mocker.mock_open(read_data="""
        {
            "datasets": [
              {
                "type": "L2_HLS_S30",
                "publish": {
                  "location": "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}",
                  "urls": [
                    "http://{{ DATASET_BUCKET }}.{{ DATASET_S3_WEBSITE_ENDPOINT }}/products/{id}",
                    "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
                  ]
                }
               }
            ]
        }
        """).return_value,
        dataset_catalog_json := mocker.mock_open(read_data="""
        {
            "PGE_Version": "dummy_pge_version",
            "SAS_Version": "dummy_sas_version"
        }
        """).return_value,
        dataset_met_json := mocker.mock_open(read_data="").return_value

    ]
    mocker.patch("builtins.open", mock_open)
    mocker.patch("product2dataset.product2dataset.os.path.abspath", lambda _: f"/{_}")
    mocker.patch("product2dataset.product2dataset.os.unlink", Mock())

    # ACT
    created_datasets = product2dataset.product2dataset.convert(
        "dummy_work_dir",
        "dummy_product_dir",
        "L3_HLS")

    # ASSERT
    assert created_datasets == ["dir1/dir2/dummy_product"]


def create_mock_SettingsConf(mocker):
    mock_SettingsConf = MagicMock()
    mock_SettingsConf.cfg = {
        "PRODUCT_TYPES": {},
        "DATASET_BUCKET": "dummy_dataset_bucket",  # in actual system, added in terraform scripts
        "DATASET_S3_ENDPOINT": "dummy_dataset_s3_endpoint"  # in actual system, added in terraform scripts
    }
    mocker.patch("product2dataset.product2dataset.SettingsConf", return_value=mock_SettingsConf)


def create_mock_PGEOutputsConf(mocker):
    mock_PGEOutputsConf = MagicMock()
    mock_PGEOutputsConf.cfg = {
        "L3_HLS": {
            "Outputs": {}
        }
    }
    mocker.patch("product2dataset.product2dataset.PGEOutputsConf", return_value=mock_PGEOutputsConf)
