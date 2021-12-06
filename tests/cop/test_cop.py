import os
import sys
import pytest
from lxml import etree
from cop import parser as cop_parser
from cop import run_cop_pge as cop_pge


try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()


current_directory = os.path.dirname(os.path.abspath(__file__))
GOOD_COP_FILE = os.path.join(
    current_directory, "test-files", "COP_e2019-038_c2019-039_v001.xml"
)
BAD_COP_FILE = os.path.join(current_directory, "test-files", "bad_cop.xml")


def test_good_cop():
    try:
        xml_doc = cop_parser.parse(GOOD_COP_FILE)
        assert isinstance(xml_doc, etree._ElementTree)
    except Exception as e:
        raise AssertionError("Error parsing good cop file: {}".format(e))


def test_bad_cop():
    with pytest.raises(AssertionError) as excinfo:
        cop_parser.parse(BAD_COP_FILE)
    assert len(excinfo.value.args) == 1532


def test_convert_to_json():
    xml_doc = cop_parser.parse(GOOD_COP_FILE)
    json_blob = cop_parser.convert_to_json(xml_doc)
    assert isinstance(json_blob, dict)
    # Make sure values were cast appropriately
    obs = json_blob[cop_pge.ROOT_TAG][cop_pge.OBSERVATIONS][cop_pge.OBS][0]
    assert isinstance(obs["lsar_config_id"], int)


test_good_cop()
test_bad_cop()
test_convert_to_json()
