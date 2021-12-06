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

STUF_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "nisar_stuf_20190213184428_20211228115850_20220109115850.xml",
)


KEYS = ["CreationDateTime", "ValidityStartDateTime", "ValidityEndDateTime"]


class TestStuf(unittest.TestCase):
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

    def __validate(self, dataset):
        met_file = os.path.join(dataset, os.path.basename(dataset) + ".met.json")
        with open(met_file, "r") as f:
            metadata = json.load(f)

        for key in KEYS:
            self.assertIn(key, metadata)

        dataset_met_file = os.path.join(
            dataset, os.path.basename(dataset) + ".dataset.json"
        )

        with open(dataset_met_file, "r") as f:
            ds_met = json.load(f)

        self.assertEqual(metadata["ValidityStartDateTime"], ds_met["starttime"])
        self.assertEqual(metadata["ValidityEndDateTime"], ds_met["endtime"])

    def test_stuf(self):
        dataset = self.__to_dataset(STUF_FILE)
        self.__validate(dataset)

    def tearDown(self):
        shutil.rmtree(self.output_dir)


if __name__ == "__main__":
    unittest.main()
