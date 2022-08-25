from pytest_mock import MockerFixture

from purge_ISL import purge_isl


def test_main(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""
    {
        "isl_urls": [
            "s3://s3-us-west-2.amazonaws.com/my-bucket/dir1/file1",
            "",
            null
        ]
    }
    """))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass


def test_main_when_called_from_ingest_job(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""
    {
        "prod_met": {
            "ISL_urls" : [
                "s3://s3-us-west-2.amazonaws.com/my-bucket/dir1/file1",
                "",
                null
            ]
        }
    }
    """))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.purge_isl_urls([
        "s3://s3-us-west-2.amazonaws.com/my-bucket/dir1/file1",
        "",
        None
    ])

    # ASSERT
    pass


def test_main_when_str(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""{ "isl_urls": "s3://s3-us-west-2.amazonaws.com/my-bucket/dir1/file1"}"""))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass


def test_main_when_empty_str(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""{ "isl_urls": ""}"""))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass


def test_main_when_null(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""{ "isl_urls": null}"""))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass


def test_main_when_empty_isl_url_list(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""{ "isl_urls": []}"""))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass


def test_main_when_empty_string_isl_url(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("builtins.open", mocker.mock_open(read_data="""{ "isl_urls": [""]}"""))
    mocker.patch("purge_ISL.purge_isl.get_cached_s3_client")

    # ACT
    purge_isl.main()

    # ASSERT
    pass

