#!/usr/bin/env python3

import json
import os
import unittest
import tempfile
from os.path import exists, join
from unittest.mock import patch
from zipfile import ZipFile

import boto3.s3.inject
import boto3.resources.collection

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


def _check_aws_connection_patch(bucket_name, dem_key=""):
    """
    No-op patch function for use with testing precondition functions that attempt
    AWS access
    """
    pass


def _object_download_file_patch(self, Filename, ExtraArgs=None, Callback=None, Config=None):
    """Patch for the boto3.s3.inject.object_download_file function"""
    # Create a dummy file in the expected location to simulate download
    with open(Filename, 'w') as outfile:
        outfile.write("fake ancillary data")


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
        expected_s3_path = "s3://opera-bucket/fake/key/to/S1A_OPER_AUX_RESORB_OPOD.EOF"
        self.assertEqual(rc_params[oc_const.ORBIT_FILE_PATH], expected_s3_path)

    def test_get_slc_static_layers_enabled(self):
        """Unit tests for the get_slc_static_layers_enabled function"""
        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "processing_mode": oc_const.PROCESSING_MODE_FORWARD
                }
            }
        }

        pge_config = {
            'pge_name': oc_const.L2_RTC_S1
        }

        settings = {
            'RTC_S1':  {
                'ENABLE_STATIC_LAYERS': True
            }
        }

        # These are not used by get_slc_s1_orbit_file
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        # For standard case (forward processing) we should get the value assigned
        # in settings.yaml
        rc_params = precondition_functions.get_slc_static_layers_enabled()

        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn('product_type', rc_params)
        self.assertEqual(rc_params['product_type'], 'RTC_S1_STATIC')

        # Make sure static layer generation is disabled when in "historical" mode
        context = {
            "product_metadata": {
                "metadata": {
                    "processing_mode": oc_const.PROCESSING_MODE_HISTORICAL
                }
            }
        }

        pge_config = {
            'pge_name': oc_const.L2_CSLC_S1
        }

        settings = {
            'CSLC_S1': {
                'ENABLE_STATIC_LAYERS': True
            }
        }

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        with self.assertLogs() as cm:
            rc_params = precondition_functions.get_slc_static_layers_enabled()

        self.assertIsNotNone(rc_params)
        self.assertIsInstance(rc_params, dict)
        self.assertIn('product_type', rc_params)
        self.assertEqual(rc_params['product_type'], 'CSLC_S1')

        # Check that we logged the flag being set to False
        self.assertIn('INFO:opera_pcm:Processing mode for L2_CSLC_S1 is set to historical, '
                      'static layer generation will be DISABLED.', cm.output)

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
                oc_const.S3_BUCKET: 'opera-bucket'
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


if __name__ == "__main__":
    unittest.main()
