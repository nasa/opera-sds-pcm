from extractor.extract import create_dataset_json


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
