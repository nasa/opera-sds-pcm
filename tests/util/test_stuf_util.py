import unittest
import os
import util.stuf_util

STUF_FILE_1 = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "nisar_stuf_20201201232340_20200125235959_20200210000001.xml",
)
STUF_FILE_2 = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "nisar_stuf_20201202232340_20200209235959_20200225000001.xml",
)


class TestStufUtil(unittest.TestCase):
    def test_has_orbit_number(self):
        util.stuf_util.file_has_orbit_number(STUF_FILE_2, 22)
        self.assertEqual(util.stuf_util.file_has_orbit_number(STUF_FILE_2, 22), True)

    def test_has_no_orbit_number(self):
        util.stuf_util.file_has_orbit_number(STUF_FILE_2, 499)
        self.assertEqual(util.stuf_util.file_has_orbit_number(STUF_FILE_2, 499), False)

    def test_get_relative_orbit_number(self):
        self.assertEqual(util.stuf_util.get_relative_orbit_number(200), 27)

    def test_get_equator(self):
        direction, cross_time = util.stuf_util.get_equator(STUF_FILE_1, 33)
        self.assertEqual(direction, "Descending")
        ctime = "2022-01-03T06:59:07.163781"
        self.assertEqual(cross_time, util.stuf_util.convert_stuf_datetime(ctime))

    def test_get_ctz(self):
        timeval = util.stuf_util.get_ctz(STUF_FILE_1)
        self.assertEqual(
            timeval, util.stuf_util.convert_stuf_datetime("2020-02-09T12:32:08.531784")
        )


if __name__ == "__main__":
    unittest.main()
