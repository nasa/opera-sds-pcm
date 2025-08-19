#!/usr/bin/env python3

import tempfile
import unittest


import tools.stage_ionosphere_file
from tools.stage_ionosphere_file import (IONOSPHERE_TYPE_RAP,
                                         IONOSPHERE_TYPE_FIN,
                                         PROVIDER_JPL,
                                         PROVIDER_ESA,
                                         PROVIDER_COD,
                                         IonosphereFileNotFoundException)


class TestStageIonosphereFile(unittest.TestCase):
    """Unit tests for the stage_ionosphere_file.py script"""

    def setUp(self) -> None:
        # Create a temporary working directory
        self.working_dir = tempfile.TemporaryDirectory(suffix="_temp", prefix="test_stage_ionosphere_file_")

    def tearDown(self) -> None:
        self.working_dir.cleanup()

    def test_parse_start_date_from_safe(self):
        """Tests for the parse_start_date_from_safe() function"""
        # Typical case: name of a valid input SLC file
        test_safe_file_name = "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F.zip"

        safe_start_date = tools.stage_ionosphere_file.parse_start_date_from_safe(test_safe_file_name)

        self.assertEqual(safe_start_date, "20180504")

        # Error case: invalid SAFE file name
        test_safe_file_name = "invalid_safe_file_name.zip"

        with self.assertRaises(RuntimeError):
            tools.stage_ionosphere_file.parse_start_date_from_safe(test_safe_file_name)

    def test_parse_start_date_from_cslc(self):
        """Tests for the parse_start_date_from_cslc() function"""
        # Typical case: name of a valid input CSLC file
        test_cslc_file_name = "OPERA_L2_CSLC-S1_T042-088937-IW1_20250804T000000Z_20250804T000000Z_S1A_VV_v1.0.h5"

        cslc_start_date = tools.stage_ionosphere_file.parse_start_date_from_cslc(test_cslc_file_name)

        self.assertEqual(cslc_start_date, "20250804")

        # Error case: invalid CSLC file name
        test_cslc_file_name = "invalid_cslc_file_name.cslc"

        with self.assertRaises(RuntimeError):
            tools.stage_ionosphere_file.parse_start_date_from_cslc(test_cslc_file_name)

    def test_parse_start_date_from_archive(self):
        """Tests for the parse_start_date_from_archive() function"""
        # Typical case: name of a valid input archive file
        for test_archive_file_name in ("S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC.zip",
                                       "OPERA_L2_CSLC-S1_T093-197858-IW3_20220501T013640Z_20220501T073552Z_S1A_VV_v1.0.h5"):
            archive_start_date = tools.stage_ionosphere_file.parse_start_date_from_archive(test_archive_file_name)

            self.assertEqual(archive_start_date, "20220501")

        # Error case: invalid archive file name
        test_archive_file_name = "invalid_archive_file_name.zip"

        with self.assertRaises(RuntimeError):
            tools.stage_ionosphere_file.parse_start_date_from_archive(test_archive_file_name)

    def test_start_date_to_julian_day(self):
        """Tests for the start_date_to_julian_day() function"""
        test_date = "20250804"
        year, julian_day = tools.stage_ionosphere_file.start_date_to_julian_day(test_date)

        self.assertIsInstance(year, str)
        self.assertIsInstance(julian_day, str)

        self.assertEqual(year, "2025")
        self.assertEqual(julian_day, "216")

    def test_get_legacy_archive_name(self):
        """Tests for the get_legacy_archive_name() function"""
        test_date = "20250804"
        year, julian_day = tools.stage_ionosphere_file.start_date_to_julian_day(test_date)
        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_JPL

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "jplg2160.25i.Z")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "jprg2160.25i.Z")

        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_ESA

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "esag2160.25i.Z")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "esrg2160.25i.Z")

        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_COD

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "codg2160.25i.Z")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        legacy_archive_name = tools.stage_ionosphere_file.get_legacy_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(legacy_archive_name, "corg2160.25i.Z")

    def test_get_new_archive_name(self):
        """Tests for the get_new_archive_name() function"""
        test_date = "20250804"
        year, julian_day = tools.stage_ionosphere_file.start_date_to_julian_day(test_date)
        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_JPL

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "JPL0OPSFIN_20252160000_01D_02H_GIM.INX.gz")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "JPL0OPSRAP_20252160000_01D_02H_GIM.INX.gz")

        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_ESA

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "ESA0OPSFIN_20252160000_01D_02H_GIM.INX.gz")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "ESA0OPSRAP_20252160000_01D_01H_GIM.INX.gz")

        ionosphere_type = IONOSPHERE_TYPE_FIN
        provider = PROVIDER_COD

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "COD0OPSFIN_20252160000_01D_01H_GIM.INX.gz")

        ionosphere_type = IONOSPHERE_TYPE_RAP

        new_archive_name = tools.stage_ionosphere_file.get_new_archive_name(
            ionosphere_type, provider, julian_day, year
        )

        self.assertEqual(new_archive_name, "COD0OPSRAP_20252160000_01D_01H_GIM.INX.gz")
