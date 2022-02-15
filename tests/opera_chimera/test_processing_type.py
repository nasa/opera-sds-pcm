import unittest
from opera_chimera.processing_type import ProcessingType


class TestProcessingType(unittest.TestCase):

    # checks to verify that we're getting back the appropriate processing type value
    def test_processing_type_values(self):
        pt_values = [ProcessingType.FORWARD, ProcessingType.REPROCESSING, ProcessingType.URGENT]
        for pt in pt_values:
            self.assertIn(pt.value, ["forward", "reprocessing", "urgent"])

    # verifies that we're returning a list of processing types
    def test_processing_type_list(self):
        pt_values = ProcessingType.list()
        self.assertIsInstance(pt_values, list)


if __name__ == "__main__":
    unittest.main()
