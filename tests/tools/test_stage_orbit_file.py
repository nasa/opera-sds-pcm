#!/usr/bin/env python3

import unittest
import tools.stage_orbit_file
from tools.stage_orbit_file import (ORBIT_TYPE_POE,
                                    ORBIT_TYPE_RES,
                                    NoQueryResultsException)

class TestStageOrbitFile(unittest.TestCase):
    """Unit tests for the stage_orbit_file.py script"""

    def test_parse_orbit_range_from_safe(self):
        """Tests for the parse_orbit_range_from_safe() function"""
        # Typical case: name of a valid input SLC file
        test_safe_file_name = "S1B_IW_SLC__1SDV_20180504T104507_20180504T104535_010770_013AEE_919F.zip"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1B")
        self.assertEquals(safe_start_time, "20180504T104507")
        self.assertEquals(safe_stop_time, "20180504T104535")

        # Test with a full path to a SAFE file
        test_safe_file_name = "/tmp/S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC.zip"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1A")
        self.assertEquals(safe_start_time, "20220501T015035")
        self.assertEquals(safe_stop_time, "20220501T015102")

        # Test with no file extension
        test_safe_file_name = "/tmp/S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC"

        (mission_id,
         safe_start_time,
         safe_stop_time) = tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

        self.assertEquals(mission_id, "S1A")
        self.assertEquals(safe_start_time, "20220501T015035")
        self.assertEquals(safe_stop_time, "20220501T015102")

        # Test with invalid file name
        test_safe_file_name = "S2A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC"

        with self.assertRaises(RuntimeError):
            tools.stage_orbit_file.parse_orbit_time_range_from_safe(test_safe_file_name)

    def test_construct_orbit_file_query(self):
        """Tests for the construct_orbit_file_query() function"""
        mission_id = "S1A"
        safe_start_time = "20220501T000000"
        safe_stop_time = "20220501T000030"
        orbit_type = ORBIT_TYPE_POE

        query = tools.stage_orbit_file.construct_orbit_file_query(
            mission_id, orbit_type, safe_start_time, safe_stop_time
        )

        # Check that all portions of the query were constructed with the inputs
        # as expected
        self.assertIn("beginPosition:[2022-04-30T00:00:00.000000Z TO 2022-05-01T00:00:00.000000Z]", query)
        self.assertIn("endPosition:[2022-05-01T00:00:30.000000Z TO 2022-05-02T00:00:30.000000Z]", query)
        self.assertIn("platformname:Sentinel-1", query)
        self.assertIn("filename:S1A_*", query)
        self.assertIn("producttype:AUX_POEORB", query)

        # Test again for restituted orbit type
        orbit_type = ORBIT_TYPE_RES

        query = tools.stage_orbit_file.construct_orbit_file_query(
            mission_id, orbit_type, safe_start_time, safe_stop_time
        )

        self.assertIn("beginPosition:[2022-04-30T21:00:00.000000Z TO 2022-05-01T00:00:00.000000Z]", query)
        self.assertIn("endPosition:[2022-05-01T00:00:30.000000Z TO 2022-05-01T03:00:30.000000Z]", query)
        self.assertIn("platformname:Sentinel-1", query)
        self.assertIn("filename:S1A_*", query)
        self.assertIn("producttype:AUX_RESORB", query)

    def test_parse_orbit_file_query_xml(self):
        """Tests for the parse_orbit_file_query_xml() function"""
        # Test with a valid (partial) XML response, formatted the way the function
        # expects
        valid_xml_response = """<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
            <subtitle>Displaying 1 results. Request done in 0.012 seconds.</subtitle>
            <updated>2022-09-27T18:13:43.726Z</updated>
            <author>
            <name>Sentinels GNSS RINEX Hub</name>
            </author>
            <opensearch:totalResults>1</opensearch:totalResults>
            <opensearch:startIndex>0</opensearch:startIndex>
            <opensearch:itemsPerPage>10</opensearch:itemsPerPage>
            <entry>
            <title>S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T200000_20220502T005942</title>
            <id>a4c32eea-7c42-4bd7-ae4e-404151a11120</id>
            <str name="format">EOF</str>
            <str name="size">4.2 MB</str>
            <str name="platformname">Sentinel-1</str>
            <str name="platformshortname">S1</str>
            <str name="platformnumber">A</str>
            <str name="platformserialidentifier">1A</str>
            <str name="filename">S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T200000_20220502T005942.EOF</str>
            <str name="producttype">AUX_POEORB</str>
            <str name="filedescription">Precise Orbit Ephemerides (POE) Orbit File</str>
            <str name="fileclass">OPER</str>
            <str name="creator">OPOD</str>
            <str name="creatorversion">1.11.6</str>
            <str name="identifier">S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T200000_20220502T005942</str>
            <str name="uuid">a4c32eea-7c42-4bd7-ae4e-404151a11120</str>
            </entry>
            </feed>
        """

        safe_start_time = '20220430T230000'
        safe_stop_time = '20220430T233000'

        entry_elems, namespace_map = tools.stage_orbit_file.parse_orbit_file_query_xml(valid_xml_response)

        # Select an appropriate orbit file from the list returned from the query
        orbit_file_name, orbit_file_request_id = tools.stage_orbit_file.select_orbit_file(
            entry_elems, namespace_map, safe_start_time, safe_stop_time
        )

        # Make sure we parsed the results as expected
        self.assertEquals(
            orbit_file_name, "S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T200000_20220502T005942.EOF"
        )
        self.assertEquals(
            orbit_file_request_id, "a4c32eea-7c42-4bd7-ae4e-404151a11120"
        )

        # Test with valid XML, but no suitable orbit file that covers SAFE time range
        safe_start_time = '20220430T225942'
        safe_stop_time = '20220502T005942'

        with self.assertRaises(RuntimeError) as err:
            entry_elems, namespace_map = tools.stage_orbit_file.parse_orbit_file_query_xml(valid_xml_response)

            tools.stage_orbit_file.select_orbit_file(
                entry_elems, namespace_map, safe_start_time, safe_stop_time
            )

        self.assertIn('No suitable orbit file could be found within the results of the query',
                      str(err.exception))

        # Test with invalid XML (missing totalResults)
        invalid_xml_response = """<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
            <subtitle>Displaying 1 results. Request done in 0.012 seconds.</subtitle>
            <updated>2022-09-27T18:13:43.726Z</updated>
            <author>
            <name>Sentinels GNSS RINEX Hub</name>
            </author>
            <opensearch:startIndex>0</opensearch:startIndex>
            <opensearch:itemsPerPage>10</opensearch:itemsPerPage>
            </feed>
        """

        with self.assertRaises(RuntimeError) as err:
            tools.stage_orbit_file.parse_orbit_file_query_xml(invalid_xml_response)

        self.assertIn('Could not find a totalResults element within the provided XML',
                      str(err.exception))

        # Test with invalid XML (no hits returned)
        invalid_xml_response = """<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
            <subtitle>Displaying 1 results. Request done in 0.012 seconds.</subtitle>
            <updated>2022-09-27T18:13:43.726Z</updated>
            <author>
            <name>Sentinels GNSS RINEX Hub</name>
            </author>
            <opensearch:totalResults>0</opensearch:totalResults>
            <opensearch:startIndex>0</opensearch:startIndex>
            <opensearch:itemsPerPage>10</opensearch:itemsPerPage>
            </feed>
        """

        with self.assertRaises(NoQueryResultsException) as err:
            tools.stage_orbit_file.parse_orbit_file_query_xml(invalid_xml_response)

        self.assertIn('No results returned from parsed query results',
                      str(err.exception))

        # Test with invalid XML (missing "entry" tags)
        invalid_xml_response = """<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
            <subtitle>Displaying 1 results. Request done in 0.012 seconds.</subtitle>
            <updated>2022-09-27T18:13:43.726Z</updated>
            <author>
            <name>Sentinels GNSS RINEX Hub</name>
            </author>
            <opensearch:totalResults>1</opensearch:totalResults>
            <opensearch:startIndex>0</opensearch:startIndex>
            <opensearch:itemsPerPage>10</opensearch:itemsPerPage>
            </feed>
        """

        with self.assertRaises(RuntimeError) as err:
            tools.stage_orbit_file.parse_orbit_file_query_xml(invalid_xml_response)

        self.assertIn('Could not find any "entry" tags within parsed query results',
                      str(err.exception))

        # Test with invalid XML ("entry" tag missing filename)
        invalid_xml_response = """<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom" xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
            <subtitle>Displaying 1 results. Request done in 0.012 seconds.</subtitle>
            <updated>2022-09-27T18:13:43.726Z</updated>
            <author>
            <name>Sentinels GNSS RINEX Hub</name>
            </author>
            <opensearch:totalResults>1</opensearch:totalResults>
            <opensearch:startIndex>0</opensearch:startIndex>
            <opensearch:itemsPerPage>10</opensearch:itemsPerPage>
            <entry>
            <title>S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T225942_20220502T005942</title>
            <id>a4c32eea-7c42-4bd7-ae4e-404151a11120</id>
            <str name="format">EOF</str>
            <str name="size">4.2 MB</str>
            <str name="platformname">Sentinel-1</str>
            <str name="platformshortname">S1</str>
            <str name="platformnumber">A</str>
            <str name="platformserialidentifier">1A</str>
            <str name="producttype">AUX_POEORB</str>
            <str name="filedescription">Precise Orbit Ephemerides (POE) Orbit File</str>
            <str name="fileclass">OPER</str>
            <str name="creator">OPOD</str>
            <str name="creatorversion">1.11.6</str>
            <str name="identifier">S1A_OPER_AUX_POEORB_OPOD_20220521T081912_V20220430T225942_20220502T005942</str>
            <str name="uuid">a4c32eea-7c42-4bd7-ae4e-404151a11120</str>
            </entry>
            </feed>
        """

        with self.assertRaises(RuntimeError) as err:
            entry_elems, namespace_map = tools.stage_orbit_file.parse_orbit_file_query_xml(invalid_xml_response)

            tools.stage_orbit_file.select_orbit_file(
                entry_elems, namespace_map, safe_start_time, safe_stop_time
            )

        self.assertIn('No suitable orbit file could be found within the results of the query',
                      str(err.exception))
