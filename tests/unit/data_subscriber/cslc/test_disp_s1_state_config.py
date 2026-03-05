"""Tests for DISP-S1 state-config CRUD operations."""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from data_subscriber.cslc import disp_s1_constants as c
from data_subscriber.cslc.disp_s1_state_config import (
    make_cycle_state_config_id,
    make_k_group_state_config_id,
    find_cycle_state_config,
    find_k_group_state_config,
    create_cycle_state_config,
    update_cycle_state_config,
    create_k_group_state_config,
    update_k_group_state_config,
)


class TestIdGeneration(unittest.TestCase):

    def test_cycle_state_config_id(self):
        self.assertEqual(
            make_cycle_state_config_id(7098, 5),
            "disp-s1_f7098_a5_state-config"
        )

    def test_cycle_state_config_id_large_values(self):
        self.assertEqual(
            make_cycle_state_config_id(14883, 145),
            "disp-s1_f14883_a145_state-config"
        )

    def test_k_group_state_config_id(self):
        self.assertEqual(
            make_k_group_state_config_id(7098, 1),
            "disp-s1_f7098_k1_state-config"
        )

    def test_k_group_state_config_id_zero(self):
        self.assertEqual(
            make_k_group_state_config_id(7098, 0),
            "disp-s1_f7098_k0_state-config"
        )


class TestFindStateConfig(unittest.TestCase):

    def test_find_cycle_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {
            "found": True,
            "_source": {
                "metadata": {
                    c.STATE_CONFIG_TYPE: c.DISP_S1_CYCLE_STATE_CONFIG,
                    c.FRAME_ID: 7098,
                    c.CYCLE_COMPLETE: False,
                }
            },
            "_index": "grq_1_disp-s1-cycle-state-config",
        }

        metadata, index = find_cycle_state_config(
            es_conn, "disp-s1_f7098_a5_state-config"
        )

        self.assertEqual(metadata[c.FRAME_ID], 7098)
        self.assertFalse(metadata[c.CYCLE_COMPLETE])
        self.assertEqual(index, "grq_1_disp-s1-cycle-state-config")

    def test_find_cycle_not_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {
            "found": False,
            "_index": "grq_*_disp-s1-cycle-state-config",
            "_id": "disp-s1_f7098_a5_state-config",
        }

        metadata, index = find_cycle_state_config(
            es_conn, "disp-s1_f7098_a5_state-config"
        )

        self.assertEqual(metadata, {})
        self.assertIsNone(index)

    def test_find_k_group_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {
            "found": True,
            "_source": {
                "metadata": {
                    c.STATE_CONFIG_TYPE: c.DISP_S1_K_GROUP_STATE_CONFIG,
                    c.FRAME_ID: 7098,
                    c.IS_COMPLETE: True,
                }
            },
            "_index": "grq_1_disp-s1-k-group-state-config",
        }

        metadata, index = find_k_group_state_config(
            es_conn, "disp-s1_f7098_k1_state-config"
        )

        self.assertTrue(metadata[c.IS_COMPLETE])

    def test_find_k_group_not_found(self):
        es_conn = MagicMock()
        es_conn.search_by_id.return_value = {"found": False}

        metadata, index = find_k_group_state_config(
            es_conn, "disp-s1_f7098_k1_state-config"
        )

        self.assertEqual(metadata, {})
        self.assertIsNone(index)


class TestCreateCycleStateConfig(unittest.TestCase):

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

        sc_id, metadata = create_cycle_state_config(
            frame_id=7098,
            acquisition_cycle=5,
            expected_burst_ids=expected,
            found_burst_ids=found,
            found_cslc_granule_ids=["OPERA_L2_CSLC-S1_T074-157286-IW3_20240801"],
            cslc_product_paths=["s3://bucket/products/CSLC_S1/file.h5"],
            start_time="2024-08-01T00:00:00",
        )

        self.assertEqual(sc_id, "disp-s1_f7098_a5_state-config")
        self.assertFalse(metadata[c.CYCLE_COMPLETE])
        self.assertEqual(metadata[c.COVERAGE_ACTUAL], 1)
        self.assertEqual(metadata[c.COVERAGE_EXPECTED], 3)
        self.assertAlmostEqual(metadata[c.COVERAGE_PERCENTAGE], 33.3)
        self.assertEqual(len(metadata[c.MISSING_BURST_IDS]), 2)

        # Verify files were created
        met_path = os.path.join(sc_id, f"{sc_id}.met.json")
        ds_path = os.path.join(sc_id, f"{sc_id}.dataset.json")
        self.assertTrue(os.path.exists(met_path))
        self.assertTrue(os.path.exists(ds_path))

        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met[c.STATE_CONFIG_TYPE], c.DISP_S1_CYCLE_STATE_CONFIG)

    def test_create_with_full_coverage(self):
        expected = ["T074-157286-IW3", "T074-157287-IW1"]
        found = ["T074-157286-IW3", "T074-157287-IW1"]

        sc_id, metadata = create_cycle_state_config(
            frame_id=7098,
            acquisition_cycle=5,
            expected_burst_ids=expected,
            found_burst_ids=found,
            found_cslc_granule_ids=["g1", "g2"],
            cslc_product_paths=["s3://b/p1", "s3://b/p2"],
            start_time="2024-08-01T00:00:00",
        )

        self.assertTrue(metadata[c.CYCLE_COMPLETE])
        self.assertEqual(metadata[c.COVERAGE_PERCENTAGE], 100.0)
        self.assertEqual(metadata[c.MISSING_BURST_IDS], [])

    def test_download_batch_id_format(self):
        _, metadata = create_cycle_state_config(
            frame_id=7098,
            acquisition_cycle=145,
            expected_burst_ids=["b1"],
            found_burst_ids=["b1"],
            found_cslc_granule_ids=["g1"],
            cslc_product_paths=["s3://b/p1"],
            start_time="2024-08-01T00:00:00",
        )

        self.assertEqual(metadata[c.DOWNLOAD_BATCH_ID], "f7098_a145")


class TestUpdateCycleStateConfig(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_update_adds_burst(self):
        existing = {
            c.EXPECTED_BURST_IDS: ["b1", "b2", "b3"],
            c.FOUND_BURST_IDS: ["b1"],
            c.FOUND_CSLC_GRANULE_IDS: ["g1"],
            c.CSLC_PRODUCT_PATHS: ["s3://p1"],
        }

        sc_id, metadata = update_cycle_state_config(
            existing_metadata=existing,
            new_burst_id="b2",
            new_cslc_granule_id="g2",
            new_cslc_product_path="s3://p2",
            frame_id=7098,
            acquisition_cycle=5,
            start_time="2024-08-01T00:00:00",
        )

        self.assertEqual(metadata[c.COVERAGE_ACTUAL], 2)
        self.assertIn("b2", metadata[c.FOUND_BURST_IDS])
        self.assertIn("b3", metadata[c.MISSING_BURST_IDS])
        self.assertFalse(metadata[c.CYCLE_COMPLETE])

    def test_update_completes_cycle(self):
        existing = {
            c.EXPECTED_BURST_IDS: ["b1", "b2"],
            c.FOUND_BURST_IDS: ["b1"],
            c.FOUND_CSLC_GRANULE_IDS: ["g1"],
            c.CSLC_PRODUCT_PATHS: ["s3://p1"],
        }

        _, metadata = update_cycle_state_config(
            existing_metadata=existing,
            new_burst_id="b2",
            new_cslc_granule_id="g2",
            new_cslc_product_path="s3://p2",
            frame_id=7098,
            acquisition_cycle=5,
            start_time="2024-08-01T00:00:00",
        )

        self.assertTrue(metadata[c.CYCLE_COMPLETE])
        self.assertEqual(metadata[c.COVERAGE_PERCENTAGE], 100.0)

    def test_update_idempotent_duplicate_burst(self):
        existing = {
            c.EXPECTED_BURST_IDS: ["b1", "b2"],
            c.FOUND_BURST_IDS: ["b1"],
            c.FOUND_CSLC_GRANULE_IDS: ["g1"],
            c.CSLC_PRODUCT_PATHS: ["s3://p1"],
        }

        _, metadata = update_cycle_state_config(
            existing_metadata=existing,
            new_burst_id="b1",
            new_cslc_granule_id="g1",
            new_cslc_product_path="s3://p1",
            frame_id=7098,
            acquisition_cycle=5,
            start_time="2024-08-01T00:00:00",
        )

        # Should not double-count
        self.assertEqual(metadata[c.COVERAGE_ACTUAL], 1)


class TestCreateKGroupStateConfig(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_create_incomplete(self):
        sc_id, metadata = create_k_group_state_config(
            frame_id=7098,
            k_group_index=0,
            k=3,
            m=2,
            acquisition_cycles=[0, 6, 12],
            cycle_state_config_ids=[
                "disp-s1_f7098_a0_state-config",
                "disp-s1_f7098_a6_state-config",
                "disp-s1_f7098_a12_state-config",
            ],
            cycle_completeness={"0": True, "6": True, "12": False},
            total_cslcs_found=54,
            total_cslcs_expected=81,
            compressed_cslc_satisfied=False,
            compressed_cslc_ids=[],
            start_time=None,
        )

        self.assertEqual(sc_id, "disp-s1_f7098_k0_state-config")
        self.assertFalse(metadata[c.IS_COMPLETE])
        self.assertFalse(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertEqual(metadata[c.CYCLES_COMPLETE], 2)
        self.assertEqual(metadata[c.CYCLES_EXPECTED], 3)

    def test_create_complete(self):
        _, metadata = create_k_group_state_config(
            frame_id=7098,
            k_group_index=0,
            k=3,
            m=2,
            acquisition_cycles=[0, 6, 12],
            cycle_state_config_ids=["sc1", "sc2", "sc3"],
            cycle_completeness={"0": True, "6": True, "12": True},
            total_cslcs_found=81,
            total_cslcs_expected=81,
            compressed_cslc_satisfied=True,
            compressed_cslc_ids=["ccslc1", "ccslc2"],
            start_time=None,
        )

        self.assertTrue(metadata[c.IS_COMPLETE])
        self.assertTrue(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertTrue(metadata[c.COMPRESSED_CSLC_SATISFIED])

    def test_all_cycles_complete_but_ccslc_not_satisfied(self):
        _, metadata = create_k_group_state_config(
            frame_id=7098,
            k_group_index=0,
            k=2,
            m=2,
            acquisition_cycles=[0, 6],
            cycle_state_config_ids=["sc1", "sc2"],
            cycle_completeness={"0": True, "6": True},
            total_cslcs_found=54,
            total_cslcs_expected=54,
            compressed_cslc_satisfied=False,
            compressed_cslc_ids=[],
            start_time=None,
        )

        self.assertTrue(metadata[c.ALL_CYCLES_COMPLETE])
        self.assertFalse(metadata[c.COMPRESSED_CSLC_SATISFIED])
        self.assertFalse(metadata[c.IS_COMPLETE])


if __name__ == "__main__":
    unittest.main()
