#!/usr/bin/env python3

import json
import os
import unittest
import tempfile

from unittest.mock import patch
from os.path import exists, join

import tools.stage_dem

from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from opera_chimera.precondition_functions import OperaPreConditionFunctions


class MockGdal:
    """
    Mock class for the osgeo.gdal module for use with testing get_dems/stage_dems
    """
    GA_ReadOnly = 0

    @staticmethod
    def UseExceptions(*args):
        pass

    @staticmethod
    def Translate(destName, srcDS, **kwargs):
        with open(destName, 'w') as outfile:
            outfile.write("fake dem data")

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


def _check_aws_connection_patch():
    """No-op patch function for use with testing stage_dem"""
    pass


class TestOperaPreConditionFunctions(unittest.TestCase):
    """Unit tests for the opera_chimera.precondition_functions module"""

    @patch.object(tools.stage_dem, "check_aws_connection", _check_aws_connection_patch)
    @patch.object(tools.stage_dem, "gdal", MockGdal)
    def test_get_dems(self):
        """Unit tests for get_dems() precondition function"""

        # Create a temporary working directory
        working_dir = tempfile.TemporaryDirectory(suffix="_temp", prefix="test_get_dems_")

        start_dir = os.curdir
        os.chdir(working_dir.name)

        # Create the workunit.json file that points to our temp dir
        with open(join(working_dir.name, "workunit.json"), "w") as outfile:
            json.dump({'args': [working_dir.name + '/']}, outfile)

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
                oc_const.DEM_BBOX: [-92, 33, -91, 32]
            }
        }

        # These are not used with get_dems()
        settings = None
        job_params = None

        try:
            precondition_functions = OperaPreConditionFunctions(
                context, pge_config, settings, job_params
            )

            rc_params = precondition_functions.get_dems()

            # Make sure we got a path back for replacement within the PGE runconfig
            self.assertIsNotNone(rc_params)
            self.assertIsInstance(rc_params, dict)
            self.assertIn(oc_const.DEM_FILE, rc_params)

            # Make sure the vrt file was created
            expected_dem_vrt = join(working_dir.name, 'dem.vrt')
            self.assertEqual(rc_params[oc_const.DEM_FILE], join(working_dir.name, 'dem.vrt'))
            self.assertTrue(exists(expected_dem_vrt))

            # Make sure the tif was created
            expected_dem_tif = join(working_dir.name, 'dem_0.tif')
            self.assertTrue(exists(expected_dem_tif))

            # Make sure the metrics for the "download" were written to disk
            expected_pge_metrics = join(working_dir.name, 'pge_metrics.json')
            self.assertTrue(exists(expected_pge_metrics))
        finally:
            os.chdir(start_dir)
            working_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
