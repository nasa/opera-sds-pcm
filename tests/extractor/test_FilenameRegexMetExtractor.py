import os

from _pytest.monkeypatch import MonkeyPatch

from extractor.FilenameRegexMetExtractor import FilenameRegexMetExtractor


def test(monkeypatch: MonkeyPatch):
    # ARRANGE
    mock_CoreMetExtractor(monkeypatch)

    test_product = "HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask.tif"
    test_match_pattern = r".*[.](?P<collection_version>v\d+[.]\d+)[.].*"
    test_extractor_config = {"Dataset_Version_Key": "collection_version"}

    extractor = FilenameRegexMetExtractor()

    # ACT
    metadata = extractor.extract(product=test_product, match_pattern=test_match_pattern, extractor_config=test_extractor_config)

    # ASSERT
    assert metadata["dataset_version"] == "v2.0"


def mock_CoreMetExtractor(monkeypatch):
    monkeypatch.setattr(os.path, os.path.dirname.__name__, lambda *args, **kwags: "")
    monkeypatch.setattr(os.path, os.path.getsize.__name__, lambda *args, **kwags: 0)
    monkeypatch.setattr(os.path, os.path.basename.__name__, lambda *args, **kwags: "")
