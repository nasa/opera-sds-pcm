"""Tests for DISP-S1 state-config CRUD operations."""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_csc_id,
    make_ksc_id,
    find_csc,
    find_ksc,
    create_csc,
    create_ksc,
)


class TestIdGeneration(unittest.TestCase):

    def test_csc_id_format(self):
        self.assertEqual(
            make_csc_id(14883, "20240801"),
            "cslc_s1-cycle-f14883-20240801-state-config"
        )

    def test_csc_id_different_frame(self):
        self.assertEqual(
            make_csc_id(7098, "20240105"),
            "cslc_s1-cycle-f7098-20240105-state-config"
        )

    def test_ksc_id_format(self):
        self.assertEqual(
            make_ksc_id(14883, "20240801", 15, 6),
            "disp_s1-kcycle-k15-m6-f14883-20240801-state-config"
        )

    def test_ksc_id_different_params(self):
        self.assertEqual(
            make_ksc_id(7098, "20240105", 10, 3),
            "disp_s1-kcycle-k10-m3-f7098-20240105-state-config"
        )


class TestFindStateConfig(unittest.TestCase):

    def test_find_csc_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {
            "found": True,
            "_source": {
                "metadata": {
                    c.STATE_CONFIG_TYPE: c.CSLC_S1_CYCLE_STATE_CONFIG,
                    c.FRAME_ID: 14883,
                    c.SENSING_DATE: "20240801",
                    c.IS_COMPLETE: True,
                }
            },
            "_index": "grq_1_cslc_s1-cycle-state-config",
        }

        metadata, index = find_csc(
            es_conn, "cslc_s1-cycle-f14883-20240801-state-config"
        )

        self.assertEqual(metadata[c.FRAME_ID], 14883)
        self.assertTrue(metadata[c.IS_COMPLETE])
        self.assertEqual(index, "grq_1_cslc_s1-cycle-state-config")

    def test_find_csc_not_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {"found": False}

        metadata, index = find_csc(
            es_conn, "cslc_s1-cycle-f14883-20240801-state-config"
        )

        self.assertEqual(metadata, {})
        self.assertIsNone(index)

    def test_find_ksc_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {
            "found": True,
            "_source": {
                "metadata": {
                    c.STATE_CONFIG_TYPE: c.DISP_S1_KCYCLE_STATE_CONFIG,
                    c.FRAME_ID: 14883,
                    c.IS_COMPLETE: True,
                }
            },
            "_index": "grq_1_disp_s1-kcycle-state-config",
        }

        metadata, index = find_ksc(
            es_conn, "disp_s1-kcycle-k15-m6-f14883-20240801-state-config"
        )

        self.assertTrue(metadata[c.IS_COMPLETE])

    def test_find_ksc_not_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {"found": False}

        metadata, index = find_ksc(
            es_conn, "disp_s1-kcycle-k15-m6-f14883-20240801-state-config"
        )

        self.assertEqual(metadata, {})
        self.assertIsNone(index)


class TestCreateCSC(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_create_with_partial_coverage(self):
        expected = ["T074-157286-IW3", "T074-157287-IW1", "T074-157288-IW2"]
        found = ["T074-157286-IW3"]

        sc_id, metadata = create_csc(
            frame_id=7098,
            acquisition_cycle=5,
            sensing_date="20240801",
            expected_burst_ids=expected,
            found_burst_ids=found,
            cslc_product_paths=["s3://bucket/products/CSLC_S1/file.h5"],
            start_time="2024-08-01T00:00:00",
        )

        self.assertEqual(sc_id, "cslc_s1-cycle-f7098-20240801-state-config")
        self.assertFalse(metadata[c.IS_COMPLETE])
        self.assertEqual(metadata[c.COVERAGE_ACTUAL], 1)
        self.assertEqual(metadata[c.COVERAGE_EXPECTED], 3)
        self.assertIn("incomplete", metadata[c.COMPLETENESS_REASON])
        self.assertEqual(len(metadata[c.MISSING_BURST_IDS]), 2)

        # Verify files were created
        met_path = os.path.join(sc_id, f"{sc_id}.met.json")
        ds_path = os.path.join(sc_id, f"{sc_id}.dataset.json")
        self.assertTrue(os.path.exists(met_path))
        self.assertTrue(os.path.exists(ds_path))

        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met[c.STATE_CONFIG_TYPE], c.CSLC_S1_CYCLE_STATE_CONFIG)
        self.assertEqual(met[c.SENSING_DATE], "20240801")

    def test_create_with_full_coverage(self):
        expected = ["T074-157286-IW3", "T074-157287-IW1"]
        found = ["T074-157286-IW3", "T074-157287-IW1"]

        sc_id, metadata = create_csc(
            frame_id=7098,
            acquisition_cycle=5,
            sensing_date="20240801",
            expected_burst_ids=expected,
            found_burst_ids=found,
            cslc_product_paths=["s3://b/p1", "s3://b/p2"],
            start_time="2024-08-01T00:00:00",
        )

        self.assertTrue(metadata[c.IS_COMPLETE])
        self.assertIn("complete", metadata[c.COMPLETENESS_REASON])
        self.assertEqual(metadata[c.MISSING_BURST_IDS], [])

    def test_recreates_existing_dir(self):
        """Test that creating a CSC when the dir already exists overwrites it."""
        sc_id = "cslc_s1-cycle-f7098-20240801-state-config"
        os.makedirs(sc_id)
        dummy_file = os.path.join(sc_id, "dummy.txt")
        with open(dummy_file, "w") as f:
            f.write("old")

        create_csc(
            frame_id=7098,
            acquisition_cycle=5,
            sensing_date="20240801",
            expected_burst_ids=["b1"],
            found_burst_ids=["b1"],
            cslc_product_paths=["s3://b/p1"],
            start_time="2024-08-01T00:00:00",
        )

        # Old file should be gone, new .met.json should exist
        self.assertFalse(os.path.exists(dummy_file))
        self.assertTrue(os.path.exists(os.path.join(sc_id, f"{sc_id}.met.json")))


class TestCreateKSC(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_create_complete_ksc(self):
        window_entries = [
            {
                "id": "cslc_s1-cycle-f7098-20240105-state-config",
                c.SENSING_DATE: "20240105",
                c.ACQUISITION_CYCLE: 100,
                c.IS_COMPLETE: True,
                c.EXPECTED_BURST_IDS: ["b1", "b2"],
                c.FOUND_BURST_IDS: ["b1", "b2"],
                c.CSLC_PRODUCT_PATHS: ["s3://p1", "s3://p2"],
            },
            {
                "id": "cslc_s1-cycle-f7098-20240117-state-config",
                c.SENSING_DATE: "20240117",
                c.ACQUISITION_CYCLE: 112,
                c.IS_COMPLETE: True,
                c.EXPECTED_BURST_IDS: ["b1", "b2"],
                c.FOUND_BURST_IDS: ["b1", "b2"],
                c.CSLC_PRODUCT_PATHS: ["s3://p3", "s3://p4"],
            },
            {
                "id": "cslc_s1-cycle-f7098-20240129-state-config",
                c.SENSING_DATE: "20240129",
                c.ACQUISITION_CYCLE: 124,
                c.IS_COMPLETE: True,
                c.EXPECTED_BURST_IDS: ["b1", "b2"],
                c.FOUND_BURST_IDS: ["b1", "b2"],
                c.CSLC_PRODUCT_PATHS: ["s3://p5", "s3://p6"],
            },
        ]

        sc_id, metadata = create_ksc(
            frame_id=7098,
            sensing_date="20240129",
            k=3,
            m=2,
            window_sensing_dates=["20240105", "20240117", "20240129"],
            window_entries=window_entries,
            product_paths={
                "L2_CSLC_S1": ["s3://p1", "s3://p2", "s3://p3", "s3://p4", "s3://p5", "s3://p6"],
                "L2_CSLC_S1_COMPRESSED": ["s3://cc1"],
            },
            compressed_cslc_satisfied=True,
            compressed_cslc_ids=["ccslc1"],
            bounding_box=[-118.5, 33.5, -117.0, 35.0],
            save_compressed_cslc=False,
            start_time=None,
        )

        self.assertEqual(sc_id, "disp_s1-kcycle-k3-m2-f7098-20240129-state-config")
        self.assertTrue(metadata[c.IS_COMPLETE])
        self.assertTrue(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertTrue(metadata[c.COMPRESSED_CSLC_SATISFIED])
        self.assertEqual(metadata[c.CYCLES_COMPLETE], 3)
        self.assertEqual(metadata[c.CYCLES_EXPECTED], 3)
        self.assertIn("ready", metadata[c.COMPLETENESS_REASON])

        # Verify files
        met_path = os.path.join(sc_id, f"{sc_id}.met.json")
        self.assertTrue(os.path.exists(met_path))
        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met[c.STATE_CONFIG_TYPE], c.DISP_S1_KCYCLE_STATE_CONFIG)
        self.assertEqual(len(met[c.WINDOW_ENTRIES]), 3)

    def test_create_incomplete_missing_cscs(self):
        window_entries = [
            {
                "id": "cslc_s1-cycle-f7098-20240105-state-config",
                c.SENSING_DATE: "20240105",
                c.IS_COMPLETE: True,
                c.CSLC_PRODUCT_PATHS: ["s3://p1"],
            },
            {
                "id": "cslc_s1-cycle-f7098-20240117-state-config",
                c.SENSING_DATE: "20240117",
                c.IS_COMPLETE: False,
                c.CSLC_PRODUCT_PATHS: [],
            },
        ]

        _, metadata = create_ksc(
            frame_id=7098,
            sensing_date="20240117",
            k=3,
            m=2,
            window_sensing_dates=["20240105", "20240117"],
            window_entries=window_entries,
            product_paths={"L2_CSLC_S1": ["s3://p1"], "L2_CSLC_S1_COMPRESSED": []},
            compressed_cslc_satisfied=False,
            compressed_cslc_ids=[],
            bounding_box=[-118.5, 33.5, -117.0, 35.0],
            save_compressed_cslc=False,
            start_time=None,
        )

        self.assertFalse(metadata[c.IS_COMPLETE])
        self.assertFalse(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertEqual(metadata[c.CYCLES_COMPLETE], 1)
        self.assertIn("K-window incomplete", metadata[c.COMPLETENESS_REASON])

    def test_gap_unresolved_persists_in_metadata(self):
        """gap_unresolved flag flows through to KSC metadata
        and augments completeness_reason."""
        window_entries = [
            {"id": "csc1", c.IS_COMPLETE: True},
            {"id": "csc2", c.IS_COMPLETE: True},
        ]

        _, metadata = create_ksc(
            frame_id=7098,
            sensing_date="20240117",
            k=2,
            m=2,
            window_sensing_dates=["20240105", "20240117"],
            window_entries=window_entries,
            product_paths={"L2_CSLC_S1": [], "L2_CSLC_S1_COMPRESSED": []},
            compressed_cslc_satisfied=True,
            compressed_cslc_ids=["cc1"],
            bounding_box=[],
            save_compressed_cslc=False,
            start_time=None,
            gap_unresolved=True,
            gap_detail="partial CSC at 20240126 (1/2)",
        )
        self.assertTrue(metadata[c.GAP_UNRESOLVED])
        self.assertIn("gap_unresolved", metadata[c.COMPLETENESS_REASON])
        self.assertIn("20240126", metadata[c.COMPLETENESS_REASON])

    def test_gap_unresolved_default_false(self):
        """gap_unresolved defaults to False — backward compat for existing callers."""
        window_entries = [{"id": "csc1", c.IS_COMPLETE: True}]
        _, metadata = create_ksc(
            frame_id=7098,
            sensing_date="20240117",
            k=1,
            m=1,
            window_sensing_dates=["20240117"],
            window_entries=window_entries,
            product_paths={"L2_CSLC_S1": [], "L2_CSLC_S1_COMPRESSED": []},
            compressed_cslc_satisfied=True,
            compressed_cslc_ids=[],
            bounding_box=[],
            save_compressed_cslc=False,
            start_time=None,
        )
        self.assertFalse(metadata[c.GAP_UNRESOLVED])

    def test_all_cycles_complete_but_ccslc_not_satisfied(self):
        window_entries = [
            {"id": "csc1", c.IS_COMPLETE: True},
            {"id": "csc2", c.IS_COMPLETE: True},
        ]

        _, metadata = create_ksc(
            frame_id=7098,
            sensing_date="20240117",
            k=2,
            m=2,
            window_sensing_dates=["20240105", "20240117"],
            window_entries=window_entries,
            product_paths={"L2_CSLC_S1": [], "L2_CSLC_S1_COMPRESSED": []},
            compressed_cslc_satisfied=False,
            compressed_cslc_ids=[],
            bounding_box=[],
            save_compressed_cslc=False,
            start_time=None,
        )

        self.assertTrue(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertFalse(metadata[c.COMPRESSED_CSLC_SATISFIED])
        self.assertFalse(metadata[c.IS_COMPLETE])
        self.assertIn("missing CCSLCs", metadata[c.COMPLETENESS_REASON])


if __name__ == "__main__":
    unittest.main()
