"""Unit tests for extract_native_ids and extract_fields utilities.

Tests Phase 1.5 of MEMORY_OPTIMIZATION_IMPLEMENTATION.md.
"""
import json
import tempfile
from pathlib import Path
import pytest

from tools.ops.cmr_audit.cmr_audit_utils import extract_native_ids, extract_fields


# Sample CMR granule data structures based on actual audit script usage
SAMPLE_RTC_GRANULE = {
    "meta": {
        "native-id": "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0",
        "revision-id": "1",
        "revision-date": "2025-05-16T15:57:14.123Z"
    },
    "umm": {
        "GranuleUR": "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-05-16T05:31:45.000Z",
                "EndingDateTime": "2025-05-16T05:31:50.000Z"
            }
        }
    }
}

SAMPLE_DSWX_GRANULE = {
    "meta": {
        "native-id": "OPERA_L3_DSWx-S1_T55GCQ_20250512T193408Z_20250513T064736Z_S1A_30_v1.0",
        "revision-id": "2",
        "revision-date": "2025-05-13T06:47:36.456Z"
    },
    "umm": {
        "GranuleUR": "OPERA_L3_DSWx-S1_T55GCQ_20250512T193408Z_20250513T064736Z_S1A_30_v1.0",
        "InputGranules": [
            "OPERA_L2_RTC-S1_T118-252625-IW2_20250512T193412Z_20250512T193437Z_S1A_30_v1.0",
            "OPERA_L2_RTC-S1_T118-252626-IW2_20250512T193409Z_20250512T193434Z_S1A_30_v1.0"
        ],
        "RelatedUrls": [
            {"URL": "https://example.com/data.tif", "Type": "GET DATA"}
        ]
    }
}

SAMPLE_HLS_GRANULE = {
    "meta": {
        "native-id": "HLS.L30.T10TEM.2024001T185931.v2.0",
        "revision-id": "1",
        "revision-date": "2024-01-01T18:59:31.000Z"
    },
    "umm": {
        "GranuleUR": "HLS.L30.T10TEM.2024001T185931.v2.0"
    }
}

SAMPLE_TROPO_GRANULE = {
    "meta": {
        "native-id": "NISAR_L2_PR_RRSD_001_005_A_219_4020_SHNA_A_20240101T000000_20240101T235959_v0.1",
        "revision-id": "1",
        "revision-date": "2024-01-01T12:00:00.000Z"
    },
    "umm": {
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2024-01-01T00:00:00.000Z",
                "EndingDateTime": "2024-01-01T23:59:59.000Z"
            }
        }
    }
}

SAMPLE_SLC_GRANULE = {
    "meta": {
        "native-id": "S1A_IW_SLC__1SDV_20250101T120000_20250101T120030_051234_063456_ABCD",
        "revision-id": "1",
        "revision-date": "2025-01-01T12:00:30.000Z"
    },
    "umm": {
        "GranuleUR": "S1A_IW_SLC__1SDV_20250101T120000_20250101T120030_051234_063456_ABCD",
        "SpatialExtent": {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "GPolygons": [
                        {
                            "Boundary": {
                                "Points": [
                                    {"Longitude": -122.0, "Latitude": 37.0},
                                    {"Longitude": -121.0, "Latitude": 37.0},
                                    {"Longitude": -121.0, "Latitude": 38.0},
                                    {"Longitude": -122.0, "Latitude": 38.0}
                                ]
                            }
                        }
                    ]
                }
            }
        }
    }
}

SAMPLE_DISP_S1_GRANULE = {
    "meta": {
        "native-id": "OPERA_L3_DISP-S1_IW_F01234_VV_20250101_20250113_v1.0_20250114T000000Z",
        "revision-id": "1",
        "revision-date": "2025-01-14T00:00:00.000Z"
    },
    "umm": {
        "GranuleUR": "OPERA_L3_DISP-S1_IW_F01234_VV_20250101_20250113_v1.0_20250114T000000Z",
        "AdditionalAttributes": [
            {"Name": "FRAME_NUMBER", "Values": ["1234"]},
            {"Name": "PRODUCT_VERSION", "Values": ["1.0"]}
        ],
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-01-01T00:00:00.000Z",
                "EndingDateTime": "2025-01-13T23:59:59.000Z"
            }
        },
        "RelatedUrls": [
            {"URL": "https://example.com/disp.nc", "Type": "GET DATA"}
        ]
    }
}

SAMPLE_GRANULE_WITH_NONE = {
    "meta": {
        "native-id": "OPERA_L2_RTC-S1_TEST_20250101T000000Z_v1.0",
        "revision-id": "1",
        "revision-date": "2025-01-01T00:00:00.000Z"
    },
    "umm": {
        "GranuleUR": "OPERA_L2_RTC-S1_TEST_20250101T000000Z_v1.0",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-01-01T00:00:00.000Z",
                "EndingDateTime": None
            }
        }
    }
}


class TestExtractNativeIds:
    """Test extract_native_ids function."""

    def test_single_file_multiple_records(self):
        """Test extracting native IDs from a single file with multiple records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_HLS_GRANULE) + '\n')

            native_ids = extract_native_ids([jsonl_path])

            assert len(native_ids) == 3
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_DSWX_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_HLS_GRANULE["meta"]["native-id"] in native_ids

    def test_multiple_files(self):
        """Test extracting native IDs from multiple files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.jsonl"
            file2 = Path(tmpdir) / "file2.jsonl"

            with open(file1, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            with open(file2, 'w') as f:
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            native_ids = extract_native_ids([file1, file2])

            assert len(native_ids) == 2
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_DSWX_GRANULE["meta"]["native-id"] in native_ids

    def test_empty_file(self):
        """Test that empty files return empty set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_file = Path(tmpdir) / "empty.jsonl"
            empty_file.touch()

            native_ids = extract_native_ids([empty_file])

            assert native_ids == set()

    def test_mixed_collections(self):
        """Test extracting native IDs from mixed collection types in same directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            rtc_file = Path(tmpdir) / "rtc.jsonl"
            dswx_file = Path(tmpdir) / "dswx.jsonl"
            hls_file = Path(tmpdir) / "hls.jsonl"

            with open(rtc_file, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            with open(dswx_file, 'w') as f:
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            with open(hls_file, 'w') as f:
                f.write(json.dumps(SAMPLE_HLS_GRANULE) + '\n')

            # Test reading all files together
            all_ids = extract_native_ids([rtc_file, dswx_file, hls_file])
            assert len(all_ids) == 3

            # Test reading subsets
            rtc_ids = extract_native_ids([rtc_file])
            assert len(rtc_ids) == 1
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in rtc_ids

    def test_duplicate_native_ids(self):
        """Test that duplicate native IDs are deduplicated (set behavior)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                # Write same record twice
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            native_ids = extract_native_ids([jsonl_path])

            # Should only have one unique ID
            assert len(native_ids) == 1
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids


class TestExtractFields:
    """Test extract_fields function."""

    def test_simple_field_extraction(self):
        """Test extracting simple top-level nested fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            records = extract_fields([jsonl_path], ["meta.native-id"])

            assert len(records) == 1
            assert records[0]["meta.native-id"] == SAMPLE_RTC_GRANULE["meta"]["native-id"]

    def test_multiple_simple_fields(self):
        """Test extracting multiple simple fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "meta.revision-id",
                "meta.revision-date"
            ])

            assert len(records) == 1
            assert records[0]["meta.native-id"] == SAMPLE_RTC_GRANULE["meta"]["native-id"]
            assert records[0]["meta.revision-id"] == SAMPLE_RTC_GRANULE["meta"]["revision-id"]
            assert records[0]["meta.revision-date"] == SAMPLE_RTC_GRANULE["meta"]["revision-date"]

    def test_deeply_nested_field(self):
        """Test extracting deeply nested fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            records = extract_fields([jsonl_path], [
                "umm.TemporalExtent.RangeDateTime.BeginningDateTime"
            ])

            assert len(records) == 1
            expected = SAMPLE_RTC_GRANULE["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            assert records[0]["umm.TemporalExtent.RangeDateTime.BeginningDateTime"] == expected

    def test_array_field_extraction(self):
        """Test extracting array fields - should return full array."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            records = extract_fields([jsonl_path], ["umm.InputGranules"])

            assert len(records) == 1
            assert isinstance(records[0]["umm.InputGranules"], list)
            assert len(records[0]["umm.InputGranules"]) == 2
            assert records[0]["umm.InputGranules"] == SAMPLE_DSWX_GRANULE["umm"]["InputGranules"]

    def test_missing_required_field_raises_keyerror(self):
        """Test that missing required fields raise KeyError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            # Try to extract a field that doesn't exist
            with pytest.raises(KeyError, match="Key 'NonExistentField' not found"):
                extract_fields([jsonl_path], ["meta.NonExistentField"])

    def test_missing_nested_field_raises_keyerror(self):
        """Test that missing nested fields raise KeyError with informative message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_HLS_GRANULE) + '\n')

            # HLS granule doesn't have InputGranules
            with pytest.raises(KeyError, match="Key 'InputGranules' not found in path 'umm.InputGranules'"):
                extract_fields([jsonl_path], ["umm.InputGranules"])

    def test_multiple_records_from_single_file(self):
        """Test extracting fields from multiple records in one file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            records = extract_fields([jsonl_path], ["meta.native-id"])

            assert len(records) == 2
            native_ids = [r["meta.native-id"] for r in records]
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_DSWX_GRANULE["meta"]["native-id"] in native_ids

    def test_multiple_files_multiple_records(self):
        """Test extracting fields from multiple files with multiple records each."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.jsonl"
            file2 = Path(tmpdir) / "file2.jsonl"

            with open(file1, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            with open(file2, 'w') as f:
                f.write(json.dumps(SAMPLE_HLS_GRANULE) + '\n')

            records = extract_fields([file1, file2], ["meta.native-id", "meta.revision-id"])

            assert len(records) == 3
            native_ids = [r["meta.native-id"] for r in records]
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_DSWX_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_HLS_GRANULE["meta"]["native-id"] in native_ids

    def test_empty_file_returns_empty_list(self):
        """Test that empty files return empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_file = Path(tmpdir) / "empty.jsonl"
            empty_file.touch()

            records = extract_fields([empty_file], ["meta.native-id"])

            assert records == []

    def test_optional_field_with_none_value(self):
        """Test that fields with explicit None values are returned as None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_GRANULE_WITH_NONE) + '\n')

            records = extract_fields([jsonl_path], [
                "umm.TemporalExtent.RangeDateTime.EndingDateTime"
            ])

            assert len(records) == 1
            assert records[0]["umm.TemporalExtent.RangeDateTime.EndingDateTime"] is None

    def test_traversal_through_none_returns_none(self):
        """Test that traversing past a None value returns None rather than raising."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # A record where an intermediate value is None
            granule = {
                "meta": {"native-id": "test-id"},
                "umm": {"TemporalExtent": None}
            }
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(granule) + '\n')

            records = extract_fields([jsonl_path], [
                "umm.TemporalExtent.RangeDateTime.BeginningDateTime"
            ])

            assert len(records) == 1
            assert records[0]["umm.TemporalExtent.RangeDateTime.BeginningDateTime"] is None


class TestFieldAccessPatterns:
    """Test field access patterns needed by audit scripts."""

    def test_rtc_audit_pattern(self):
        """Test field access pattern used by cmr_audit_dswx_s1.py for RTC granules."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "rtc.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')

            # Pattern from implementation plan line 366
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "meta.revision-id",
                "meta.revision-date"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify we can access fields with dot notation as keys
            native_id = record["meta.native-id"]
            revision_id = record["meta.revision-id"]
            revision_date = record["meta.revision-date"]

            # Verify transformations still work
            burst_id = native_id[16:31]
            assert burst_id == "T168-359595-IW3"

    def test_dswx_audit_pattern(self):
        """Test field access pattern used by cmr_audit_dswx_s1.py for DSWx granules."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "dswx.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            # Pattern from implementation plan line 367
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "umm.InputGranules"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify we can iterate over InputGranules array
            input_granules = record["umm.InputGranules"]
            assert isinstance(input_granules, list)
            assert len(input_granules) == 2

            # Verify we can process the array as in the actual script
            for granule in input_granules:
                assert granule.startswith("OPERA_L2_RTC-S1_")

    def test_tropo_audit_pattern(self):
        """Test field access pattern used by cmr_audit_tropo.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "tropo.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_TROPO_GRANULE) + '\n')

            # Pattern from implementation plan line 482
            records = extract_fields([jsonl_path], [
                "umm.TemporalExtent.RangeDateTime.BeginningDateTime"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify we can process the datetime as in the actual script
            begin_dt = record["umm.TemporalExtent.RangeDateTime.BeginningDateTime"]
            date_str = begin_dt.split('T')[0]
            assert date_str == "2024-01-01"

    def test_dist_s1_audit_pattern(self):
        """Test field access pattern used by cmr_audit_dist_s1.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "dist.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            # Pattern from implementation plan line 425
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "umm.RelatedUrls"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify we can access nested array of dicts
            related_urls = record["umm.RelatedUrls"]
            assert isinstance(related_urls, list)
            assert len(related_urls) == 1
            assert related_urls[0]["URL"] == "https://example.com/data.tif"

    def test_slc_audit_pattern(self):
        """Test field access pattern used by cmr_audit_slc.py for spatial geometry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "slc.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_SLC_GRANULE) + '\n')

            # Pattern from implementation plan line 408
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "umm.SpatialExtent.HorizontalSpatialDomain.Geometry.GPolygons"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify deeply nested spatial geometry is returned as-is
            gpolygons = record["umm.SpatialExtent.HorizontalSpatialDomain.Geometry.GPolygons"]
            assert isinstance(gpolygons, list)
            assert len(gpolygons) == 1
            points = gpolygons[0]["Boundary"]["Points"]
            assert len(points) == 4
            assert points[0]["Latitude"] == 37.0

    def test_disp_s1_audit_pattern(self):
        """Test field access pattern used by opv_disp_s1.py for DISP-S1 granules."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "disp.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_DISP_S1_GRANULE) + '\n')

            # Pattern from implementation plan line 508-516
            records = extract_fields([jsonl_path], [
                "umm.AdditionalAttributes",
                "umm.TemporalExtent.RangeDateTime.EndingDateTime",
                "umm.GranuleUR",
                "umm.RelatedUrls"
            ])

            assert len(records) == 1
            record = records[0]

            # Verify AdditionalAttributes array
            attrs = record["umm.AdditionalAttributes"]
            assert isinstance(attrs, list)
            assert len(attrs) == 2
            frame_attr = next(a for a in attrs if a["Name"] == "FRAME_NUMBER")
            assert frame_attr["Values"] == ["1234"]

            # Verify EndingDateTime
            assert record["umm.TemporalExtent.RangeDateTime.EndingDateTime"] == "2025-01-13T23:59:59.000Z"

            # Verify GranuleUR
            assert "DISP-S1" in record["umm.GranuleUR"]

    def test_combining_extract_functions(self):
        """Test that scripts can use both extract functions together."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "combined.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            # Extract native IDs
            native_ids = extract_native_ids([jsonl_path])
            assert len(native_ids) == 2

            # Extract detailed fields
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "meta.revision-id"
            ])
            assert len(records) == 2

            # Verify consistency
            extracted_ids = {r["meta.native-id"] for r in records}
            assert native_ids == extracted_ids

    def test_avoid_redundant_reads(self):
        """Test pattern where extract_fields includes native-id to avoid redundant reads."""
        with tempfile.TemporaryDirectory() as tmpdir:
            jsonl_path = Path(tmpdir) / "test.jsonl"
            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(SAMPLE_RTC_GRANULE) + '\n')
                f.write(json.dumps(SAMPLE_DSWX_GRANULE) + '\n')

            # Extract fields including native-id
            records = extract_fields([jsonl_path], [
                "meta.native-id",
                "meta.revision-id"
            ])

            # Derive native ID set from extracted records (avoid calling extract_native_ids)
            native_ids = {r["meta.native-id"] for r in records}

            assert len(native_ids) == 2
            assert SAMPLE_RTC_GRANULE["meta"]["native-id"] in native_ids
            assert SAMPLE_DSWX_GRANULE["meta"]["native-id"] in native_ids


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
