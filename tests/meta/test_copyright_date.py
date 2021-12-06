import unittest
import os
import re
import datetime


class TestCopyingYear(unittest.TestCase):

    # check if to see if the current year is in the COPYING file.
    def test_copying_year(self):
        path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "../../COPYING"
        )
        # read the file content and find the date. Then check that it's 2019-2020
        content = ""
        with open(path, "r") as reader:
            content = reader.read()
        # regex grabs all years or year ranges. Should only grab one.
        year_regex_pattern = r"(?:\d{4})"

        years = re.findall(year_regex_pattern, content)
        current_year = datetime.datetime.now().strftime("%Y")
        # assert that the current year is in the list of extracted years
        self.assertTrue(current_year in years)


if __name__ == "__main__":
    unittest.main()
