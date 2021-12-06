import re
import unittest
import os
import json
import shutil
from util.conf_util import SettingsConf
from extractor import extract

EXTRACTOR_HOME = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../../extractor"
)

FOE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "NISAR_ANC_J_PR_FOE_20200504T145019_20200603T225942_20200610T225942.xml.gz",
)

MOE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "NISAR_ANC_J_PR_MOE_20220104T145019_20220108T050942_20220108T235942.xml.gz",
)

NOE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "NISAR_ANC_J_PR_NOE_20200504T145019_20200504T225942_20200504T234442.xml.gz",
)

POE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "NISAR_ANC_J_PR_POE_20220104T145019_20220108T005942_20220108T225942.xml.gz",
)

KEYS = [
    "Instrument",
    "ProcessingType",
    "CreationDateTime",
    "ValidityStartDateTime",
    "ValidityEndDateTime",
    "GranuleName"
]


class TestOrbitEpehemeris(unittest.TestCase):
    settings = {}
    dataset = None
    output_dir = None

    def setUp(self):
        self.output_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "output"
        )
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

    def __to_dataset(self, file_path):
        return extract.extract(
            file_path, self.settings["PRODUCT_TYPES"], self.output_dir
        )

    def __validate(self, dataset, type):
        met_file = os.path.join(dataset, os.path.basename(dataset) + ".met.json")
        with open(met_file, "r") as f:
            metadata = json.load(f)

        self.assertEqual(metadata["Fidelity"], type)
        for key in KEYS:
            self.assertIn(key, metadata)

        dataset_met_file = os.path.join(
            dataset, os.path.basename(dataset) + ".dataset.json"
        )

        with open(dataset_met_file, "r") as f:
            ds_met = json.load(f)

        self.assertEqual(metadata["ValidityStartDateTime"], ds_met["starttime"])
        self.assertEqual(metadata["ValidityEndDateTime"], ds_met["endtime"])

    def test_moe(self):
        dataset = self.__to_dataset(MOE_FILE)
        self.__validate(dataset, "MOE")

    def test_poe(self):
        dataset = self.__to_dataset(POE_FILE)
        self.__validate(dataset, "POE")

    def test_noe(self):
        dataset = self.__to_dataset(NOE_FILE)
        self.__validate(dataset, "NOE")

    def test_foe(self):
        dataset = self.__to_dataset(FOE_FILE)
        self.__validate(dataset, "FOE")

    def tearDown(self):
        shutil.rmtree(self.output_dir)


if __name__ == "__main__":
    unittest.main()
