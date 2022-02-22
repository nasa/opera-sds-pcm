import unittest
import os
import shutil
from product2dataset import product2dataset
from glob import glob
import hashlib
from jinja2 import Environment, FileSystemLoader

EXTRACTOR_HOME = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../../extractor"
)

good_pge_outputs = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "test-files", "good_pge_outputs.yaml"
)

bad_pge_outputs = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "test-files", "bad_pge_outputs.yaml"
)

# TODO: modify output filename with OPERA product name
#L3_DSWx_HLS_DIR = os.path.join(
#    os.path.dirname(os.path.abspath(__file__)),
#    "test-files",
#    "L3_DSWx_HLS",
#    "NISAR_L0_RRST_VC00_20220108T073544_20220108T075044_D00200_001",
#)
#OUTPUT_TYPE = {"L3_DSWx_HLS_PGE": ".tif" }
#CHECKSUM_TYPE = {"L3_DSWx_HLS_PGE": "md5"}

OUTPUT_TYPE = {}
CHECKSUM_TYPE = {}


class Product2Dataset(unittest.TestCase):
    settings = {}
    dataset = None
    output_dir = None

    def setUp(self):
        self.settings_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "test-files", "settings.yaml"
        )

        settings_tmpl_file_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "../../conf"
        )
        # settings_tmpl_file = os.path.join(settings_tmpl_file_dir, "settings.yaml")

        ENV = Environment(loader=FileSystemLoader(settings_tmpl_file_dir))
        template = ENV.get_template("settings.yaml")

        with open(self.settings_file, "w") as f:
            f.write(template.render(EXTRACTOR_HOME=EXTRACTOR_HOME))

    def __gethash(self, filename):
        hasher = hashlib.md5()
        with open(filename, "rb") as f:
            buf = f.read()
            hasher.update(buf)
            return hasher.hexdigest()

    def __validate(self, product_dir, product_type, pge_output_yaml=None):

        hash_type = CHECKSUM_TYPE[product_type]
        output_type = OUTPUT_TYPE[product_type]
        product = os.path.basename(product_dir)
        dataset_dir = os.path.join(product_dir, "datasets", product)
        print("dataset_dir : {}".format(dataset_dir))

        if os.path.exists(dataset_dir) and os.path.isdir(dataset_dir):
            print("Exists and Removing")
            shutil.rmtree(dataset_dir)
        rc_file = None
        product2dataset.convert(
            product_dir, product_type, rc_file, pge_output_yaml, self.settings_file
        )

        hash_file_regex = "*{}.{}".format(output_type, hash_type)
        input_hash_file = glob(os.path.join(product_dir, hash_file_regex))[0]
        output_hash_file = glob(os.path.join(dataset_dir, hash_file_regex))[0]

        return [input_hash_file, output_hash_file]

    def test_good_hash_algorithm(self):
#        hash_files = self.__validate(L3_DSWx_HLS_DIR, "L3_DSWx_HLS_PGE", good_pge_outputs)
#        self.assertEqual(
#            os.path.basename(hash_files[0]), os.path.basename(hash_files[1])
#        )
#        self.assertEqual(self.__gethash(hash_files[0]), self.__gethash(hash_files[1]))

    def test_bad_hash_algorithm(self):
#        try:
#            self.__validate(L3_DSWx_HLS_DIR, "L3_DSWx_HLS_PGE", bad_pge_outputs)
#        except Exception as err:
#            self.assertTrue(str(err).startswith("Unsupported hashing algorithm"))
#
#        self.assertRaises(
#            Exception, self.__validate, L3_DSWx_HLS_DIR, "L3_DSWx_HLS_PGE", bad_pge_outputs
#        )


if __name__ == "__main__":
    unittest.main()
