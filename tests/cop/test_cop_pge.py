import re
import unittest
import os
import shutil
import sys
from datetime import datetime

from util.common_util import convert_datetime

from unittest.mock import MagicMock
from elasticmock import elasticmock

from util.conf_util import SettingsConf
from extractor import extract

from cop import cop_catalog as cop_cat
from observation_accountability import catalog as obs_cat

from cop import run_cop_pge as cop_pge

try:
    import unittest.mock as umock
except ImportError:
    import mock as umock
sys.modules["hysds.celery"] = umock.MagicMock()

ES_URL = "http://localhost:9200"

EXTRACTOR_HOME = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../../extractor"
)

COP_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "COP_e2019-038_c2019-039_v001.xml",
)


class TestCopPGE(unittest.TestCase):
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
        product_types = self.settings["PRODUCT_TYPES"]
        for type in list(product_types.keys()):
            product_types[type]["Extractor"] = re.sub(
                "\\{\\{ EXTRACTOR_HOME \\}\\}",
                EXTRACTOR_HOME,
                str(product_types[type]["Extractor"]),
            )
        self.dataset = extract.extract(
            COP_FILE, self.settings["PRODUCT_TYPES"], os.getcwd()
        )

    def tearDown(self):
        shutil.rmtree(self.dataset)

    @elasticmock
    def find_accountability_record(self, datatake_id, observation_es):
        obs_es_index = 'observation_accountability_catalog'
        # make a query that retrieves all found observation_accountability records associated with a datatake_id.
        #
        body = {"query": {"bool": {"match": {"datatake_id": datatake_id}}}}

        result = observation_es.search(
            index=obs_es_index, body=body
        )
        results = result.get("hits", {}).get("hits", [])
        if len(results) < 1:
            return None, None
        for entry in results:
            entry_id = entry.get("_id", None)
            return entry_id, entry.get("_source", {})

    @elasticmock
    def set_accountability_record(self, obs_id, observation, obs_connection):
        obs_entry_id, obs_acc_entry = self.find_accountability_record(
            observation[cop_cat.DATATAKE_ID], obs_connection
        )
        if obs_entry_id is None and obs_acc_entry is None:
            obs_acc_entry = {
                "datatake_id": observation[cop_cat.DATATAKE_ID],
                "observation_ids": [obs_id],
                "L0A_L_RRST_ids": [],
                "created_at": convert_datetime(datetime.utcnow()),
                "last_modified": convert_datetime(datetime.utcnow()),
                "ref_start_datetime_iso": observation[
                    cop_cat.REF_START_DATETIME_ISO
                ],
                "ref_end_datetime_iso": observation[cop_cat.REF_END_DATETIME_ISO],
                "refrec_id": "",
            }
            obs_connection.post(
                [obs_acc_entry], header={}, index=obs_cat.ES_INDEX
            )
        else:
            if obs_id not in obs_acc_entry["observation_ids"]:
                obs_acc_entry["observation_ids"].append(obs_id)
                obs_acc_entry["last_modified"] = convert_datetime(datetime.utcnow())
                obs_connection.update_document(
                    index=obs_cat.ES_INDEX,
                    doc_type="_doc",
                    id=obs_entry_id,
                    body=obs_acc_entry,
                )

    @elasticmock
    def test_pge(self):
        cop_file_name, cop_data = cop_pge.parse(os.path.basename(self.dataset))
        observations, header = cop_pge.reformat_datetime_values(
            cop_data[cop_pge.ROOT_TAG][cop_pge.OBSERVATIONS][cop_pge.OBS],
            cop_data[cop_pge.ROOT_TAG][cop_pge.HEADER],
        )
        cop_catalog = cop_cat.CopCatalog(ES_URL)
        cop_catalog.es.transport = MagicMock()

        es_index = 'cop_catalog'
        cop_catalog.create_index()

        obs_acc_catalog = obs_cat.ObservationAccountabilityCatalog(ES_URL)
        obs_acc_catalog.es.transport = MagicMock()

        # obs_es_index = 'observation_accountability_catalog'
        # obs_acc_catalog.create_index()

        for observation in observations:
            # create cop_catalog entry
            observation[cop_pge.HEADER] = header
            _id = "{}_{}".format(observation["refobs_id"], observation["priority"])
            cop_catalog.index_document(index=es_index, id=_id, body=observation)
            # create observation_accountability entry
            # commenting out for now
            # self.set_accountability_record(observation["refobs_id"], observation, obs_acc_catalog)

        # check cop catalog count
        cop_catalog_count = cop_catalog.get_count(index=es_index)
        self.assertEqual(cop_catalog_count, len(observations))

        # check observation (datatake) entries are created and have the right nunmber of observations
        # obs_catalog_count = obs_acc_catalog.get_count(index=obs_es_index)
        # self.assertEqual(obs_catalog_count, len(observations))

        cop_catalog.post(observations)
        cop_catalog_count = cop_catalog.get_count(index=es_index)
        self.assertEqual(cop_catalog_count, len(observations) * 2)


if __name__ == "__main__":
    unittest.main()
