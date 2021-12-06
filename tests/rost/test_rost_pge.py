import unittest
import datetime
import os
import shutil
import sys

from unittest.mock import MagicMock
from elasticmock import elasticmock

from util.conf_util import SettingsConf
from extractor import extract
from hysds.celery import app

from rost.catalog import RostCatalog
from rost import rost_pge

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()

ES_URL = app.conf.get("GRQ_ES_URL", "http://localhost:9200")

current_directory = os.path.dirname(os.path.realpath(__file__))
EXTRACTOR_HOME = os.path.join(current_directory, "../../extractor")
RC_DIR = os.path.join(current_directory, "../radar_config")

ROST_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files/orost",
    "id_00-0a-0100_orost-2023001-c001-d01-v01.xml"
)

SCLK_SCET_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "NISAR_198900_SCLKSCET_LRCLK.00004"
)

SROST_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files/srost",
    "id_00-0a-0100_srost-2023001-c001-d01-v01"
)

OFS_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files/ofs",
    "ofs_00-0a-0100_srost-2023001-c001-d01-v01.tsv"
)

RADAR_CFG_FILENAME = os.path.normpath(os.path.join(
    RC_DIR,
    "test-files",
    "id_01-00-0701_radar-configuration_v45-14"
))


class TestRostPGE(unittest.TestCase):
    settings = {}
    dataset = None

    def setUp(self):
        self.settings = SettingsConf(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "../../conf",
                "settings.yaml",
            )
        ).cfg
        self.dataset = extract.extract(
            ROST_FILENAME, self.settings["PRODUCT_TYPES"], os.getcwd()
        )

    def tearDown(self):
        shutil.rmtree(self.dataset)

    def get_dataset(self, _id):
        """Query for existence of dataset by ID."""

        es_index = "rost_catalog"
        query = {"query": {"bool": {"must": [{"term": {"_id": _id}}]}}}
        print(query)

        rost_catalog = RostCatalog(ES_URL)
        results = rost_catalog.search(index=es_index)
        results = results['hits']['hits']
        print(results)

    @elasticmock
    def test_pge(self):
        rost_catalog = RostCatalog(ES_URL)
        rost_catalog.es.transport = MagicMock()

        rost_xml_filename = os.path.basename(self.dataset)
        records = rost_pge.convert_rost_file(rost_xml_filename, SCLK_SCET_FILENAME, SROST_FILENAME, OFS_FILENAME,
                                             RADAR_CFG_FILENAME)

        es_index = "rost_catalog"
        rost_catalog.create_index()

        for record in records:
            _id = record["refrec_id"]
            rost_catalog.index_document(index=es_index, id=_id, body=record)

        rost_catalog_count = rost_catalog.get_count(index=es_index)
        self.assertEqual(rost_catalog_count, len(records))

        rost_catalog.post(records)
        rost_catalog_count = rost_catalog.get_count(index=es_index)
        self.assertEqual(rost_catalog_count, len(records))

        # check start_time_iso in the first record to validate the
        # radarTimeToUTC() function
        start_time_iso = records[0]['start_time_iso']
        if type(start_time_iso) is str:
            if start_time_iso[-1] == 'Z':
                start_time_iso = start_time_iso[:-1]
            start_time = datetime.datetime.fromisoformat(start_time_iso)

        expected_start_time = datetime.datetime(2019, 1, 1, 0, 0, 26, 459613)
        self.assertEqual(start_time, expected_start_time)


if __name__ == "__main__":
    unittest.main()
