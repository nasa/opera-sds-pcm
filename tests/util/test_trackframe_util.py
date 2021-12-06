"""
Tests of the track frame utilities
"""
from datetime import datetime, timedelta
import unittest
import os
import warnings

import pandas
import numpy

from util.trackframe_util import get_track_frames, CycleInfo

TRACK_FRAME_DB_FILENAME = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "test-files",
    "small_track_frame_db.gpkg"
)


class TestGetTrackFrames(unittest.TestCase):
    """
    Tests get_track_frames()
    """
    def test_get_track_frames_simple_case(self):
        """
        Test get_track_frames() under nominal conditions, simple test case
        involving only 1 cycle
        """
        cycles = [CycleInfo(
            id=123,
            ctz_utc=datetime(2020, 10, 11, 0, 0, 0),
            time_corrections_table=None
        )]

        start_time_utc = datetime(2020, 10, 11, 0, 1, 40)
        end_time_utc = datetime(2020, 10, 11, 0, 3, 20)
        track_frames = get_track_frames(cycles, start_time_utc, end_time_utc,
                                        TRACK_FRAME_DB_FILENAME)
        expected_cycles = numpy.array([123] * 4)
        expected_tracks = numpy.array([1] * 4)
        expected_frames = numpy.array([4, 5, 6, 7])
        expected_start_times = numpy.array([
            pandas.Timestamp(2020, 10, 11, 0, 1, 23, 118350),
            pandas.Timestamp(2020, 10, 11, 0, 1, 56, 567507),
            pandas.Timestamp(2020, 10, 11, 0, 2, 30, 20577),
            pandas.Timestamp(2020, 10, 11, 0, 3, 3, 479498)
        ])
        expected_end_times = numpy.array([
            pandas.Timestamp(2020, 10, 11, 0, 1, 57, 567507),
            pandas.Timestamp(2020, 10, 11, 0, 2, 31, 20578),
            pandas.Timestamp(2020, 10, 11, 0, 3, 4, 479499),
            pandas.Timestamp(2020, 10, 11, 0, 3, 37, 946237)
        ])

        max_time_tolerance = pandas.Timedelta(1000)  # 1 microsecond
        self.assertTrue((track_frames.cycle == expected_cycles).all())
        self.assertTrue((track_frames.track == expected_tracks).all())
        self.assertTrue((track_frames.frame == expected_frames).all())

        # "not vectorized" performance warnings are unimportant since only 4
        # array elements are being compared
        warnings.simplefilter(action='ignore', category=pandas.errors.PerformanceWarning)

        self.assertTrue((abs(track_frames.start_time_utc - expected_start_times)
                        <= max_time_tolerance).all())

        self.assertTrue((abs(track_frames.end_time_utc - expected_end_times)
                        <= max_time_tolerance).all())

        warnings.resetwarnings()

    def test_get_track_frames_with_date_time_strings(self):
        """
        Like test_get_track_frames_simple_case but with string representations
        of date/time values
        """
        cycles = [CycleInfo(
            id=123,
            ctz_utc='2020-10-11T00:00:00.000000Z',
            time_corrections_table=None
        )]

        start_time_utc = '2020-10-11T00:01:40.123456Z'
        end_time_utc = '2020-10-11T00:03:20.123456Z'
        track_frames = get_track_frames(cycles, start_time_utc, end_time_utc,
                                        TRACK_FRAME_DB_FILENAME)
        expected_cycles = numpy.array([123] * 4)
        expected_tracks = numpy.array([1] * 4)
        expected_frames = numpy.array([4, 5, 6, 7])
        expected_start_times = numpy.array([
            pandas.Timestamp(2020, 10, 11, 0, 1, 23, 118350),
            pandas.Timestamp(2020, 10, 11, 0, 1, 56, 567507),
            pandas.Timestamp(2020, 10, 11, 0, 2, 30, 20577),
            pandas.Timestamp(2020, 10, 11, 0, 3, 3, 479498)
        ])
        expected_end_times = numpy.array([
            pandas.Timestamp(2020, 10, 11, 0, 1, 57, 567507),
            pandas.Timestamp(2020, 10, 11, 0, 2, 31, 20578),
            pandas.Timestamp(2020, 10, 11, 0, 3, 4, 479499),
            pandas.Timestamp(2020, 10, 11, 0, 3, 37, 946237)
        ])

        max_time_tolerance = pandas.Timedelta(1000)  # 1 microsecond
        self.assertTrue((track_frames.cycle == expected_cycles).all())
        self.assertTrue((track_frames.track == expected_tracks).all())
        self.assertTrue((track_frames.frame == expected_frames).all())

        # "not vectorized" performance warnings are unimportant since only 4
        # array elements are being compared
        warnings.simplefilter(action='ignore', category=pandas.errors.PerformanceWarning)

        self.assertTrue((abs(track_frames.start_time_utc - expected_start_times)
                        <= max_time_tolerance).all())

        self.assertTrue((abs(track_frames.end_time_utc - expected_end_times)
                        <= max_time_tolerance).all())

        warnings.resetwarnings()

    def test_get_track_frames_multi_cycle(self):
        """
        Test get_track_frames() under nominal conditions for a time range
        spanning a cycle boundary
        """
        cycles = [
            CycleInfo(
                id=2,
                ctz_utc=datetime(2020, 12, 8, 13, 24, 1),
                time_corrections_table=None
            ),
            CycleInfo(
                id=3,
                ctz_utc=datetime(2020, 12, 8, 13, 35, 12),
                time_corrections_table=None
            ),
            CycleInfo(
                id=4,
                ctz_utc=datetime(2020, 12, 8, 13, 46, 22),
                time_corrections_table=None
            ),
            CycleInfo(
                id=5,
                ctz_utc=datetime(2020, 12, 8, 13, 57, 32),
                time_corrections_table=None
            )
        ]

        start_time_utc = datetime(2020, 12, 8, 13, 45, 0)
        end_time_utc = datetime(2020, 12, 8, 13, 47, 0)
        track_frames = get_track_frames(cycles, start_time_utc, end_time_utc,
                                        TRACK_FRAME_DB_FILENAME)
        expected_cycles = numpy.array([3, 3, 4, 4])
        expected_tracks = numpy.array([1, 1, 1, 1])
        expected_frames = numpy.array([19, 20, 1, 2])
        expected_start_times = numpy.array([
            pandas.Timestamp(2020, 12, 8, 13, 44, 58, 213861),
            pandas.Timestamp(2020, 12, 8, 13, 45, 31, 952855),
            pandas.Timestamp(2020, 12, 8, 13, 46, 4, 775209),
            pandas.Timestamp(2020, 12, 8, 13, 46, 38, 224101)
        ])
        expected_end_times = numpy.array([
            pandas.Timestamp(2020, 12, 8, 13, 45, 32, 952855),
            pandas.Timestamp(2020, 12, 8, 13, 46, 6, 734185),
            pandas.Timestamp(2020, 12, 8, 13, 46, 39, 224101),
            pandas.Timestamp(2020, 12, 8, 13, 47, 12, 671183)
        ])

        max_time_tolerance = pandas.Timedelta(1000)  # 1 microsecond
        self.assertTrue((track_frames.cycle == expected_cycles).all())
        self.assertTrue((track_frames.track == expected_tracks).all())
        self.assertTrue((track_frames.frame == expected_frames).all())

        # "not vectorized" performance warnings are unimportant since only 4
        # array elements are being compared
        warnings.simplefilter(action='ignore', category=pandas.errors.PerformanceWarning)

        self.assertTrue((abs(track_frames.start_time_utc - expected_start_times)
                        <= max_time_tolerance).all())

        self.assertTrue((abs(track_frames.end_time_utc - expected_end_times)
                        <= max_time_tolerance).all())

        warnings.resetwarnings()

    def test_get_track_frames_raises_exception(self):
        """
        Test that get_track_frames() raises an exception if the provided
        cycle data does not completely cover the specified time range
        """
        cycles = [
            CycleInfo(
                id=3,
                ctz_utc=datetime(2020, 12, 8, 13, 35, 12),
                time_corrections_table=None
            ),
            CycleInfo(
                id=4,
                ctz_utc=datetime(2020, 12, 8, 13, 46, 22),
                time_corrections_table=None
            )
        ]

        # nominal conditions
        start_time_utc = datetime(2020, 12, 8, 13, 34, 54, 775210)
        end_time_utc = datetime(2020, 12, 8, 13, 40, 00)
        get_track_frames(cycles, start_time_utc, end_time_utc,
                         TRACK_FRAME_DB_FILENAME)

        # get_track_frames should raise an exception if the start time is
        # before the first track frame
        start_time_utc -= timedelta(microseconds=10)
        with self.assertRaises(ValueError):
            get_track_frames(cycles, start_time_utc, end_time_utc,
                             TRACK_FRAME_DB_FILENAME)

        # nominal
        start_time_utc = datetime(2020, 12, 8, 13, 56, 8)
        end_time_utc = datetime(2020, 12, 8, 13, 57, 16, 734184)
        get_track_frames(cycles, start_time_utc, end_time_utc,
                         TRACK_FRAME_DB_FILENAME)

        # get_track_frames should raise an exception if the end time is
        # after the last track frame
        end_time_utc += timedelta(microseconds=10)
        with self.assertRaises(ValueError):
            get_track_frames(cycles, start_time_utc, end_time_utc,
                             TRACK_FRAME_DB_FILENAME)


if __name__ == "__main__":
    unittest.main()
