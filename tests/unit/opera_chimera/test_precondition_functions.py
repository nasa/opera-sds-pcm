#!/usr/bin/env python3

import json
import os
import tempfile
import unittest
from os.path import exists, join
from unittest.mock import patch, MagicMock
from zipfile import ZipFile

import boto3.resources.collection
import boto3.s3.inject
import botocore.client
import botocore.exceptions

import tools.stage_ancillary_map
import tools.stage_dem
import tools.stage_worldcover
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from opera_chimera.precondition_functions import OperaPreConditionFunctions


class MockGdal:
    """
    Mock class for the osgeo.gdal module for use with testing ancillary data staging
    precondition functions (get_dems,get_worldcover)
    """
    GA_ReadOnly = 0

    @staticmethod
    def UseExceptions(*args):
        pass

    @staticmethod
    def Translate(destName, srcDS, **kwargs):
        with open(destName, 'w') as outfile:
            outfile.write("fake output data")

    @staticmethod
    def BuildVRT(destName, srcDSOrSrcDSTab, **kwargs):
        with open(destName, 'w') as outfile:
            outfile.write("fake vrt data")

    # pylint: disable=all
    class MockGdalDataset:
        """Mock class for gdal.Dataset objects, as returned from an Open call."""
        def GetGeoTransform(self):
            return 1, 1, 1, 1, 1, 1

        def GetRasterBand(self, index):
            class MockRasterBand:
                def __init__(self):
                    self.XSize = 1
                    self.YSize = 1

            return MockRasterBand()

    @staticmethod
    def Open(filename, filemode=None):
        """Mock implementation for gdal.Open. Returns an instance of the mock Dataset."""
        return MockGdal.MockGdalDataset()


def _check_aws_connection_patch(bucket, key):
    """
    No-op patch function for use with testing precondition functions that attempt
    AWS access
    """
    pass


def _object_download_file_patch(self, Filename, ExtraArgs=None, Callback=None, Config=None):
    """Patch for the boto3.s3.inject.object_download_file function"""
    # Create a dummy file in the expected location to simulate download
    with open(Filename, 'w') as outfile:
        outfile.write("fake ancillary data\n__PATTERN1__\n__PATTERN2__")


class MockCollectionManager:
    """
    Mock class for boto3.resources.collection.CollectionManager for use with
    tests that filter on s3 objects to locate an ancillary file
    """
    class MockS3Object:
        def __init__(self):
            self.key = None

    def __init__(self, collection_model, parent, factory, service_context):
        self.s3_object = self.MockS3Object()

    def filter(self, **kwargs):
        self.s3_object.key = "fake/key/to/S1A_OPER_AUX_RESORB_OPOD.EOF"

        return [self.s3_object]


class TestOperaPreConditionFunctions(unittest.TestCase):
    """Unit tests for the opera_chimera.precondition_functions module"""

    def setUp(self) -> None:
        # Create a temporary working directory
        self.working_dir = tempfile.TemporaryDirectory(suffix="_temp", prefix="test_precondition_functions_")

        self.start_dir = os.curdir
        os.chdir(self.working_dir.name)

        # Create the workunit.json file that points to our temp dir
        with open(join(self.working_dir.name, "workunit.json"), "w") as outfile:
            json.dump({'args': [self.working_dir.name + '/']}, outfile)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)
        self.working_dir.cleanup()

    @patch.object(boto3.s3.inject, "object_download_file", _object_download_file_patch)
    def test_get_slc_s1_safe_file(self):
        """Unit tests for the get_slc_s1_safe_file() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_path": "s3://s3-us-west-2.amazonaws.com:80/opera-bucket/fake/key/to",
            "product_metadata": {
                "metadata":
                    {
                        'FileName': "DUMMY_SAFE.zip"
                    }
            }
        }

        pge_config = {
            oc_const.GET_SLC_S1_SAFE_FILE: {}
        }

        # These are not used with get_dems()
        settings = None
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_slc_s1_safe_file()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.SAFE_FILE_PATH, rc_params)

        # Make sure the SAFE file was created
        expected_safe_file = join(self.working_dir.name, 'DUMMY_SAFE.zip')
        self.assertEqual(rc_params[oc_const.SAFE_FILE_PATH], expected_safe_file)
        self.assertTrue(exists(expected_safe_file))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    def test_get_slc_polarization(self):
        """Unit tests for the get_slc_polarization() function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata":
                    {
                        'FileName': "S1A_IW_SLC__1SDV_..._043011_0522A4_42CC.zip"
                    }
            }
        }

        pge_config = {
            oc_const.GET_SLC_S1_POLARIZATION: {}
        }

        # These are not used by get_slc_s1_orbit_file
        settings = None
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_slc_polarization()

        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.POLARIZATION, rc_params)

        # "DV" portion of test file name should translate to "dual-pol" setting
        # for runconfig
        expected_polarization = 'dual-pol'
        self.assertEqual(rc_params[oc_const.POLARIZATION], expected_polarization)

        # Test again with single polarization setting ("SH")
        context['product_metadata']['metadata']['FileName'] = "S1A_IW_SLC__1SSH_..._043011_0522A4_42CC.zip"

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_slc_polarization()

        expected_polarization = 'co-pol'
        self.assertEqual(rc_params[oc_const.POLARIZATION], expected_polarization)

    @patch.object(boto3.resources.collection, "CollectionManager", MockCollectionManager)
    def test_get_slc_s1_orbit_file(self):
        """Unit tests for the get_slc_s1_orbit_file() function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_path": "s3://s3-us-west-2.amazonaws.com:80/opera-bucket/fake/key/to",
        }

        pge_config = {
            oc_const.GET_SLC_S1_SAFE_FILE: {}
        }

        # These are not used by get_slc_s1_orbit_file
        settings = None
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_slc_s1_orbit_file()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.ORBIT_FILE_PATH, rc_params)

        # Make sure the path to the orbit file was assigned to the runconfig params as expected
        expected_s3_paths = ["s3://opera-bucket/fake/key/to/S1A_OPER_AUX_RESORB_OPOD.EOF"]
        self.assertListEqual(rc_params[oc_const.ORBIT_FILE_PATH], expected_s3_paths)

    @patch.object(tools.stage_dem, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_dem, "gdal", MockGdal)
    def test_get_slc_s1_dem(self):
        """Unit tests for the get_slc_s1_dem() precondition function"""

        # Create a dummy SAFE zip archive containing a stub version of
        # manifest.safe with the portion of XML we'll be looking for
        manifest_safe_text = """<?xml version="1.0" encoding="UTF-8"?>
        <xfdu:XFDU xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                   xmlns:gml="http://www.opengis.net/gml" 
                   xmlns:xfdu="urn:ccsds:schema:xfdu:1" 
                   xmlns:safe="http://www.esa.int/safe/sentinel-1.0" 
                   version="esa/safe/sentinel-1.0/sentinel-1/sar/level-1/slc/standard/iwdp">
            <metadataSection>
                <metadataObject ID="measurementFrameSet" classification="DESCRIPTION" category="DMD">
                  <metadataWrap mimeType="text/xml" vocabularyName="SAFE" textInfo="Frame Set">
                    <xmlData>
                      <safe:frameSet>
                        <safe:frame>
                          <safe:footPrint srsName="http://www.opengis.net/gml/srs/epsg.xml#4326">
                            <gml:coordinates>35.360844,-119.156471 35.760201,-116.393867 34.082375,-116.057800 33.681068,-118.762573</gml:coordinates>
                          </safe:footPrint>
                        </safe:frame>
                      </safe:frameSet>
                    </xmlData>
                  </metadataWrap>
                </metadataObject>
            </metadataSection>
        </xfdu:XFDU>
        """

        with ZipFile(join(self.working_dir.name, 'DUMMY_SAFE.zip'), 'w') as myzip:
            myzip.writestr('DUMMY_SAFE.SAFE/manifest.safe', manifest_safe_text)

        # Set up the arguments to OperaPreConditionFunctions
        job_params = {
            oc_const.SAFE_FILE_PATH: join(self.working_dir.name, 'DUMMY_SAFE.zip')
        }

        pge_config = {
            'pge_name': 'L2_CSLC_S1',
            oc_const.GET_SLC_S1_DEM: {
                oc_const.S3_BUCKET: 'opera-bucket',
                oc_const.S3_KEY: 'key/to/dem/'
            }
        }

        settings = {
            'CSLC_S1': {
                'ANCILLARY_MARGIN': 50
            }
        }

        # These are not used with get_cslc_s1_dem()
        context = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_slc_s1_dem()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.DEM_FILE, rc_params)

        # Make sure the vrt file was created
        expected_dem_vrt = join(self.working_dir.name, 'dem.vrt')
        self.assertEqual(rc_params[oc_const.DEM_FILE], expected_dem_vrt)
        self.assertTrue(exists(expected_dem_vrt))

        # Make sure the tif was created
        expected_dem_tif = join(self.working_dir.name, 'dem_0.tif')
        self.assertTrue(exists(expected_dem_tif))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    @patch.object(tools.stage_dem, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_dem, "gdal", MockGdal)
    def test_get_dswx_hls_dem(self):
        """Unit tests for get_dswx_hls_dem() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "id": "HLS.S30.T15SXR.2021250T163901.v2.0"
                }
            }
        }

        pge_config = {
            oc_const.GET_DSWX_HLS_DEM: {
                oc_const.BBOX: []
            }
        }

        settings = {"DSWX_HLS": {"ANCILLARY_MARGIN": 50}}

        # These are not used with get_dems()
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_dswx_hls_dem()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.DEM_FILE, rc_params)

        # Make sure the vrt file was created
        expected_dem_vrt = join(self.working_dir.name, 'dem.vrt')
        self.assertEqual(rc_params[oc_const.DEM_FILE], expected_dem_vrt)
        self.assertTrue(exists(expected_dem_vrt))

        # Make sure the tif was created
        expected_dem_tif = join(self.working_dir.name, 'dem_0.tif')
        self.assertTrue(exists(expected_dem_tif))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    @patch.object(boto3.s3.inject, "object_download_file", _object_download_file_patch)
    def test_get_landcover(self):
        """Unit tests for get_landcover() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {}

        pge_config = {
            oc_const.GET_LANDCOVER: {
                oc_const.S3_BUCKET: "opera-land-cover",
                oc_const.S3_KEY: "PROBAV_LC100_global_v3.0.1_2019-nrt_Discrete-Classification-map_EPSG-4326.tif"
            }
        }

        # These are not used with get_landcover()
        settings = None
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_landcover()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.LANDCOVER_FILE, rc_params)

        # Make sure the vrt file was created
        expected_landcover_tif = join(self.working_dir.name, 'landcover.tif')
        self.assertEqual(rc_params[oc_const.LANDCOVER_FILE], expected_landcover_tif)
        self.assertTrue(exists(expected_landcover_tif))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    @patch.object(tools.stage_worldcover, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_worldcover, "gdal", MockGdal)
    def test_get_worldcover(self):
        """Unit tests for get_worldcover() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "id": "HLS.S30.T15SXR.2021250T163901.v2.0"
                }
            }
        }

        pge_config = {
            oc_const.GET_DSWX_HLS_DEM: {
                oc_const.BBOX: []
            }
        }

        settings = {"DSWX_HLS": {"ANCILLARY_MARGIN": 50}}

        # These are not used with get_worldcover()
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_worldcover()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.WORLDCOVER_FILE, rc_params)

        # Make sure the vrt file was created
        expected_worldcover_vrt = join(self.working_dir.name, 'worldcover.vrt')
        self.assertEqual(rc_params[oc_const.WORLDCOVER_FILE], expected_worldcover_vrt)
        self.assertTrue(exists(expected_worldcover_vrt))

        # Make sure the tif was created
        expected_worldcover_tif = join(self.working_dir.name, 'worldcover_0.tif')
        self.assertTrue(exists(expected_worldcover_tif))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    @patch.object(tools.stage_ancillary_map, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_ancillary_map, "gdal", MockGdal)
    def test_get_dswx_s1_dynamic_ancillary_maps(self):
        """Unit tests for get_dswx_s1_dynamic_ancillary_maps() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "bounding_box": [-119.156471, 33.681068, -116.0578, 35.760201]
                }
            }
        }

        pge_config = {
            oc_const.GET_DSWX_S1_DYNAMIC_ANCILLARY_MAPS: {
                "hand_file": {
                    "s3_bucket": "opera-hand",
                    "s3_key": "v1/2021/glo-30-hand-2021.vrt"
                },
                "worldcover_file": {
                    "s3_bucket": "opera-world-cover",
                    "s3_key": "v100/2020/ESA_WorldCover_10m_2020_v100_Map_AWS.vrt"
                },
                "reference_water_file": {
                    "s3_bucket": "opera-reference-water",
                    "s3_key": "2021/occurrence/occurrence_v1_4_2021.vrt"
                }
            }
        }

        settings = {"DSWX_S1": {"ANCILLARY_MARGIN": 50}}

        # These are not used with get_worldcover()
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_dswx_s1_dynamic_ancillary_maps()

        expected_dynamic_ancillary_maps = ["hand_file", "worldcover_file", "reference_water_file"]

        # Make sure we got paths back for each staged ancillary map
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)

        for expected_dynamic_ancillary_map_name in expected_dynamic_ancillary_maps:
            # Ensure the rc_params dictionary was populated correctly
            self.assertIn(expected_dynamic_ancillary_map_name, rc_params)
            self.assertIsInstance(rc_params[expected_dynamic_ancillary_map_name], str)

            # Ensure the VRT file was created as expected
            expected_vrt_file = join(self.working_dir.name, f'{expected_dynamic_ancillary_map_name}.vrt')

            self.assertEqual(expected_vrt_file, rc_params[expected_dynamic_ancillary_map_name])
            self.assertTrue(os.path.exists(expected_vrt_file))

            # Ensure the tif file was created as expected
            expected_tif_file = join(self.working_dir.name, f'{expected_dynamic_ancillary_map_name}_0.tif')

            self.assertTrue(os.path.exists(expected_tif_file))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    @patch.object(tools.stage_dem, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_dem, "gdal", MockGdal)
    def test_get_dswx_s1_dem(self):
        """Unit tests for get_dswx_s1_dem() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "bounding_box": [-119.156471, 33.681068, -116.0578, 35.760201]
                }
            }
        }

        pge_config = {
            oc_const.GET_DSWX_S1_DEM: {
                oc_const.S3_BUCKET: "opera-dem",
                oc_const.S3_KEY: "v1.1"
            }
        }

        settings = {"DSWX_S1": {"ANCILLARY_MARGIN": 50}}

        # These are not used with get_dems()
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_dswx_s1_dem()

        # Make sure we got a path back for replacement within the PGE runconfig
        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn(oc_const.DEM_FILE, rc_params)

        # Make sure the vrt file was created
        expected_dem_vrt = join(self.working_dir.name, 'dem.vrt')
        self.assertEqual(rc_params[oc_const.DEM_FILE], expected_dem_vrt)
        self.assertTrue(exists(expected_dem_vrt))

        # Make sure the tif was created
        expected_dem_tif = join(self.working_dir.name, 'dem_0.tif')
        self.assertTrue(exists(expected_dem_tif))

        # Make sure the metrics for the "download" were written to disk
        expected_pge_metrics = join(self.working_dir.name, 'pge_metrics.json')
        self.assertTrue(exists(expected_pge_metrics))

    def test_get_dswx_s1_static_ancillary_files(self):
        """Unit tests for get_dswx_s1_static_ancillary_files() precondition function"""
        # Set up the arguments to OperaPreConditionFunctions
        pge_config = {
            oc_const.GET_STATIC_ANCILLARY_FILES: {
                "algorithm_parameters": {
                    oc_const.S3_BUCKET: "opera-ancillaries",
                    oc_const.S3_KEY: "algorithm_parameters/pge_name/algorithm_parameters_beta_0.1.0.yaml"
                },
                "mgrs_database_file": {
                    oc_const.S3_BUCKET: "opera-ancillaries",
                    oc_const.S3_KEY: "mgrs_tiles/dswx_s1/MGRS_tile_v0.2.1.sqlite"
                },
                "mgrs_collection_database_file": {
                    oc_const.S3_BUCKET: "opera-ancillaries",
                    oc_const.S3_KEY: "mgrs_tiles/dswx_s1/MGRS_tile_collection_v0.3.sqlite"
                }
            }
        }

        # These are not used with get_dswx_s1_static_ancillary_files()
        context = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_static_ancillary_files()

        expected_static_ancillary_products = ["algorithm_parameters",
                                              "mgrs_database_file",
                                              "mgrs_collection_database_file"]

        for expected_static_ancillary_product in expected_static_ancillary_products:
            self.assertIn(expected_static_ancillary_product, rc_params)
            self.assertIsInstance(rc_params[expected_static_ancillary_product], str)
            self.assertEqual(
                rc_params[expected_static_ancillary_product],
                "s3://{}/{}".format(
                    pge_config[oc_const.GET_STATIC_ANCILLARY_FILES][expected_static_ancillary_product][oc_const.S3_BUCKET],
                    pge_config[oc_const.GET_STATIC_ANCILLARY_FILES][expected_static_ancillary_product][oc_const.S3_KEY]
                )
            )

    def test_get_dswx_s1_input_filepaths(self):
        """Unit tests for get_s3_input_filepaths() precondition function for dswx-s1"""
        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "dataset_type": "L2_RTC_S1",
            "product_metadata": {
                "metadata": {
                    "product_paths": {
                        "L2_RTC_S1": [
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                        ]
                    }
                }
            }
        }

        # These are not used with get_dswx_s1_input_filepaths()
        pge_config = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_s3_input_filepaths()

        # Ensure the list of input file paths was populated with only the set of
        # unique S3 directories that make up the set of input RTC files
        self.assertIn(oc_const.INPUT_FILE_PATHS, rc_params)
        self.assertIsInstance(rc_params[oc_const.INPUT_FILE_PATHS], list)
        self.assertEqual(len(rc_params[oc_const.INPUT_FILE_PATHS]), 3)
        self.assertIn("s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146", rc_params[oc_const.INPUT_FILE_PATHS])
        self.assertIn("s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147", rc_params[oc_const.INPUT_FILE_PATHS])
        self.assertIn("s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148", rc_params[oc_const.INPUT_FILE_PATHS])

    def test_get_dswx_s1_inundated_vegetation_enabled(self):
        """Unit tests for get_dswx_s1_inundated_vegetation_enabled() precondition function"""
        # First test the case where dual-polarization is provided
        context = {
            "dataset_type": "L2_RTC_S1",
            "product_metadata": {
                "metadata": {
                    "product_paths": {
                        "L2_RTC_S1": [
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VH.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_VV.tif",
                            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
                        ]
                    }
                }
            }
        }

        # These are not used with get_dswx_s1_exclude_inundated_vegetation()
        pge_config = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_dswx_s1_inundated_vegetation_enabled()

        self.assertIn(oc_const.INUNDATED_VEGETATION_ENABLED, rc_params)
        self.assertIsInstance(rc_params[oc_const.INUNDATED_VEGETATION_ENABLED], bool)

        # For dual-pol, inundated vegetation SHOULD be enabled
        self.assertTrue(rc_params[oc_const.INUNDATED_VEGETATION_ENABLED])

        # Now try the single-pol case
        context["product_metadata"]["metadata"]["product_paths"]["L2_RTC_S1"] = [
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_HH.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_HH.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_HH.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
        ]

        rc_params = precondition_functions.get_dswx_s1_inundated_vegetation_enabled()

        self.assertIn(oc_const.INUNDATED_VEGETATION_ENABLED, rc_params)
        self.assertIsInstance(rc_params[oc_const.INUNDATED_VEGETATION_ENABLED], bool)

        # For single-pol, inundated vegetation SHOULD NOT be enabled
        self.assertFalse(rc_params[oc_const.INUNDATED_VEGETATION_ENABLED])

        # Lastly, test the error case where no polarization files are provided
        context["product_metadata"]["metadata"]["product_paths"]["L2_RTC_S1"] = [
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_17$146/OPERA_L2_RTC-S1_T012-023801-IW1_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_18$147/OPERA_L2_RTC-S1_T013-023802-IW2_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0_mask.tif",
            "s3://opera-dev-rs-fwd/dswx_s1/MS_12_19$148/OPERA_L2_RTC-S1_T014-023803-IW3_20231019T121502Z_20231019T232415Z_S1A_30_v1.0.h5",
        ]

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        with self.assertRaises(ValueError):
            precondition_functions.get_dswx_s1_inundated_vegetation_enabled()

    def test_get_disp_s1_input_filepaths(self):
        """Unit tests for get_s3_input_filepaths() precondition function for disp-s1"""
        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "dataset_type": "L2_CSLC_S1",
            "product_metadata": {
                "metadata": {
                    "product_paths": {
                        "L2_CSLC_S1": [
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000703-IW2/OPERA_L2_CSLC-S1_T001-000703-IW2_20231006T183312Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000699-IW3/OPERA_L2_CSLC-S1_T001-000699-IW3_20231006T183302Z_20231009T185644Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000700-IW1/OPERA_L2_CSLC-S1_T001-000700-IW1_20231006T183303Z_20231009T185644Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000702-IW1/OPERA_L2_CSLC-S1_T001-000702-IW1_20231006T183309Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000705-IW3/OPERA_L2_CSLC-S1_T001-000705-IW3_20231006T183319Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000701-IW3/OPERA_L2_CSLC-S1_T001-000701-IW3_20231006T183308Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000702-IW3/OPERA_L2_CSLC-S1_T001-000702-IW3_20231006T183311Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000699-IW2/OPERA_L2_CSLC-S1_T001-000699-IW2_20231006T183301Z_20231009T185644Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000698-IW1/OPERA_L2_CSLC-S1_T001-000698-IW1_20231006T183258Z_20231009T185644Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000700-IW2/OPERA_L2_CSLC-S1_T001-000700-IW2_20231006T183304Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000705-IW1/OPERA_L2_CSLC-S1_T001-000705-IW1_20231006T183317Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000703-IW1/OPERA_L2_CSLC-S1_T001-000703-IW1_20231006T183311Z_20231009T185701Z_S1A_VV_v1.0.h5",
                        ]
                    }
                }
            }
        }

        # These are not used with get_dswx_s1_input_filepaths()
        pge_config = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_s3_input_filepaths()

        # Ensure the list of input file paths was populated with only the set of
        # unique S3 directories that make up the set of input CSLC files
        self.assertIn(oc_const.INPUT_FILE_PATHS, rc_params)
        self.assertIsInstance(rc_params[oc_const.INPUT_FILE_PATHS], list)
        self.assertEqual(len(rc_params[oc_const.INPUT_FILE_PATHS]), 12)
        for s3_path in context['product_metadata']['metadata']['product_paths']['L2_CSLC_S1']:
            self.assertIn(os.path.dirname(s3_path), rc_params[oc_const.INPUT_FILE_PATHS])

    def test_get_disp_s1_frame_id(self):
        """Unit tests for the get_disp_s1_frame_id() precondition function"""
        context = {
            "product_metadata": {
                "metadata": {
                    "frame_id": "88"
                }
            }
        }

        # These are not used with get_disp_s1_frame_id()
        pge_config = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_disp_s1_frame_id()

        self.assertIn(oc_const.FRAME_ID, rc_params)
        self.assertIsInstance(rc_params[oc_const.FRAME_ID], str)
        self.assertEqual("88", rc_params[oc_const.FRAME_ID])

    def test_get_disp_s1_product_type(self):
        """Unit tests for the get_disp_s1_product_type() precondition function"""
        context = {
            "processing_mode": oc_const.PROCESSING_MODE_HISTORICAL
        }

        # These are not used with get_disp_s1_product_type()
        pge_config = {}
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_disp_s1_product_type()

        self.assertIn(oc_const.PRODUCT_TYPE, rc_params)
        self.assertIsInstance(rc_params[oc_const.PRODUCT_TYPE], str)
        self.assertEqual(oc_const.DISP_S1_HISTORICAL, rc_params[oc_const.PRODUCT_TYPE])

        for proc_mode in [oc_const.PROCESSING_MODE_FORWARD, oc_const.PROCESSING_MODE_REPROCESSING]:
            context["processing_mode"] = proc_mode

            precondition_functions = OperaPreConditionFunctions(
                context, pge_config, settings, job_params
            )

            rc_params = precondition_functions.get_disp_s1_product_type()

            self.assertEqual(oc_const.DISP_S1_FORWARD, rc_params[oc_const.PRODUCT_TYPE])

    @patch.object(boto3.s3.inject, "object_download_file", _object_download_file_patch)
    def test_get_disp_s1_algorithm_parameters(self):
        """Unit tests for get_disp_s1_algorithm_parameters() precondition function"""
        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "processing_mode": oc_const.PROCESSING_MODE_HISTORICAL
        }

        pge_config = {
            oc_const.GET_DISP_S1_ALGORITHM_PARAMETERS: {
                oc_const.S3_BUCKET: "opera-ancillaries",
                oc_const.S3_KEY: "algorithm_parameters/disp_s1/0.1.0/algorithm_parameters_{processing_mode}.yaml"
            }
        }

        # These are not used with get_algorithm_parameters()
        settings = {}
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_disp_s1_algorithm_parameters()

        # Ensure the S3 URI was formed as expected
        self.assertIn(oc_const.ALGORITHM_PARAMETERS, rc_params)
        self.assertIsInstance(rc_params[oc_const.ALGORITHM_PARAMETERS], str)
        self.assertIn("algorithm_parameters_historical.yaml",
                       rc_params[oc_const.ALGORITHM_PARAMETERS])
        self.assertTrue(exists(rc_params[oc_const.ALGORITHM_PARAMETERS]))

        # Ensure both forward and reprocessing modes resolve to the forward parameters
        for proc_mode in [oc_const.PROCESSING_MODE_FORWARD, oc_const.PROCESSING_MODE_REPROCESSING]:
            context["processing_mode"] = proc_mode

            precondition_functions = OperaPreConditionFunctions(
                context, pge_config, settings, job_params
            )

            rc_params = precondition_functions.get_disp_s1_algorithm_parameters()
            self.assertIn("algorithm_parameters_forward.yaml",
                          rc_params[oc_const.ALGORITHM_PARAMETERS])
            self.assertTrue(exists(rc_params[oc_const.ALGORITHM_PARAMETERS]))

    def test_get_disp_s1_troposphere_files(self):
        """
        Unit tests for the get_disp_s1_troposphere_files() precondition function.
        """
        context = {
            "product_metadata": {
                "metadata": {
                    "product_paths": {
                        "L2_CSLC_S1": [
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000703-IW2/OPERA_L2_CSLC-S1_T001-000703-IW2_20231006T183312Z_20231009T185701Z_S1A_VV_v1.0.h5",
                            "s3://opera-dev-rs-fwd/disp_s1/88_145/T001-000703-IW2/OPERA_L2_CSLC-S1_T001-000703-IW2_20231018T043311Z_20231021T185701Z_S1A_VV_v1.0.h5"
                        ],
                        "L2_CSLC_S1_COMPRESSED": [
                            "s3://opera-dev-lts-fwd/products/CSLC_S1_COMPRESSED/OPERA_L2_COMPRESSED-CSLC-S1_T001-000703-IW2_20230805T000000Z_20230805T000000Z_20230917T000000Z_20240717T225540Z_VV_v1.0.h5"
                        ]
                    }
                }
            }
        }

        pge_config = {
            oc_const.GET_DISP_S1_TROPOSPHERE_FILES: {
                oc_const.S3_BUCKET: "opera-ancillaries",
                oc_const.S3_KEY: "ecmwf"
            }
        }

        settings = {
            'DISP_S1': {'STRICT_ANCILLARY_USAGE': True}
        }

        # These are not used with get_disp_s1_troposphere_files()
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        # Test with valid result from s3_client.head_object()
        mock_head_object = MagicMock()

        with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
            rc_params = precondition_functions.get_disp_s1_troposphere_files()

        expected_troposphere_s3_paths = ['s3://opera-ancillaries/ecmwf/20231018/D10180000101800001.subset.zz.nc',
                                         's3://opera-ancillaries/ecmwf/20230805/D08050000080500001.subset.zz.nc',
                                         's3://opera-ancillaries/ecmwf/20231006/D10061800100618001.subset.zz.nc']
        self.assertIn(oc_const.TROPOSPHERE_FILES, rc_params)
        self.assertIsInstance(rc_params[oc_const.TROPOSPHERE_FILES], list)
        self.assertEqual(len(rc_params[oc_const.TROPOSPHERE_FILES]), 3)
        self.assertTrue(all(expected_troposphere_s3_path in rc_params[oc_const.TROPOSPHERE_FILES]
                            for expected_troposphere_s3_path in expected_troposphere_s3_paths))

        # Test with 404 not found result from s3_client.head_object()
        mock_head_object = MagicMock(
            side_effect=botocore.exceptions.ClientError({"Error": {"Code": "404"}}, "head_object")
        )

        with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
            with self.assertRaises(RuntimeError) as err:
                precondition_functions.get_disp_s1_troposphere_files()

        self.assertIn("One or more expected ECMWF files is missing from opera-ancillaries/ecmwf", str(err.exception))

        # Retry missing file test, but with strict mode disabled
        settings = {
            'DISP_S1': {'STRICT_ANCILLARY_USAGE': False}
        }

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        with patch.object(botocore.client.BaseClient, "_make_api_call", mock_head_object):
            with self.assertLogs("opera_pcm", level="WARNING") as logger:
                rc_params = precondition_functions.get_disp_s1_troposphere_files()

        self.assertIsInstance(rc_params[oc_const.TROPOSPHERE_FILES], list)
        self.assertEqual(len(rc_params[oc_const.TROPOSPHERE_FILES]), 0)
        self.assertIn("WARNING:opera_pcm:One or more expected ECMWF files is missing from opera-ancillaries/ecmwf", logger.output)
        self.assertIn("WARNING:opera_pcm:No Tropospheres files will be included for this DISP-S1 job", logger.output)


    def test_instantiate_algorithm_parameters_template(self):
        """
        Unit tests for the instantiate_algorithm_parameters_template()
        precondition function
        """
        # Use the mock object download file function to write a dummy algorithm parameters file
        parameter_file = os.path.join(self.working_dir.name, 'algorithm_parameters.yaml.tmpl')
        _object_download_file_patch(self, Filename=parameter_file)

        pge_config = {
            oc_const.INSTANTIATE_ALGORITHM_PARAMETERS_TEMPLATE: {
                oc_const.TEMPLATE_MAPPING: {
                    "param1": "__PATTERN1__",
                    "param2": "__PATTERN2__"
                }
            }
        }

        job_params = {
            oc_const.ALGORITHM_PARAMETERS: parameter_file,
            "param1": "value1",
            "param2": "value2"
        }

        # These are not used with instantiate_algorithm_parameters_template()
        context = {}
        settings = {}

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.instantiate_algorithm_parameters_template()

        self.assertIn(oc_const.ALGORITHM_PARAMETERS, rc_params)
        self.assertIsInstance(rc_params[oc_const.ALGORITHM_PARAMETERS], str)
        self.assertTrue(os.path.exists(rc_params[oc_const.ALGORITHM_PARAMETERS]))
        self.assertFalse(rc_params[oc_const.ALGORITHM_PARAMETERS].endswith(".tmpl"))

        with open(rc_params[oc_const.ALGORITHM_PARAMETERS], 'r') as infile:
            instantiated_template = infile.read()

        self.assertNotIn("__PATTERN1__", instantiated_template)
        self.assertNotIn("__PATTERN2__", instantiated_template)
        self.assertIn("value1", instantiated_template)
        self.assertIn("value2", instantiated_template)


if __name__ == "__main__":
    unittest.main()
