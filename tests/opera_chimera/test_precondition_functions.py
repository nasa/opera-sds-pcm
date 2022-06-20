#!/usr/bin/env python3

import json
import os
import unittest
import tempfile

from unittest.mock import patch
from os.path import exists, join

import boto3.s3.inject

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
        pass

    @staticmethod
    def Open(filename, filemode=None):
        """Mock implementation for gdal.Open. Returns an instance of the mock Dataset."""
        return MockGdal.MockGdalDataset()


def _check_aws_connection_patch(bucket_name):
    """
    No-op patch function for use with testing precondition functions that attempt
    AWS access
    """
    pass


def _object_download_file_patch(self, Filename, ExtraArgs=None, Callback=None, Config=None):
    """Patch for the boto3.s3.inject.object_download_file function"""
    # Create a dummy file in the expected location to simulate download
    with open(Filename, 'w') as outfile:
        outfile.write("fake landcover data")


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

    @patch.object(tools.stage_dem, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_dem, "gdal", MockGdal)
    def test_get_dems(self):
        """Unit tests for get_dems() precondition function"""

        # Set up the arguments to OperaPreConditionFunctions
        context = {
            "product_metadata": {
                "metadata": {
                    "@timestamp": "2022-05-12T15:54:57.406535915Z",
                    "B02": "products/HLS.S30.T15SXR.2021250T163901.v2.0.B02.tif",
                    "Fmask": "products/HLS.S30.T15SXR.2021250T163901.v2.0.Fmask.tif"
                }
            }
        }

        pge_config = {
            oc_const.GET_DEMS: {
                oc_const.BBOX: []
            }
        }

        # These are not used with get_dems()
        settings = None
        job_params = None

        precondition_functions = OperaPreConditionFunctions(
            context, pge_config, settings, job_params
        )

        rc_params = precondition_functions.get_dems()

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
                    "@timestamp": "2022-05-12T15:54:57.406535915Z",
                    "B02": "products/HLS.L30.T22VEQ.2021250T163901.v2.0.B02.tif",
                    "Fmask": "products/HLS.L30.T22VEQ.2021250T163901.v2.0.Fmask.tif"
                }
            }
        }

        pge_config = {
            oc_const.GET_DEMS: {
                oc_const.BBOX: []
            }
        }

        # These are not used with get_worldcover()
        settings = None
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
