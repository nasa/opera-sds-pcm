"""
Class that contains the precondition evaluation steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""

import argparse
import inspect
import json
import os
import re
import traceback
from datetime import datetime
from pathlib import PurePath
from typing import Dict, List
from urllib.parse import urlparse

import boto3

from chimera.precondition_functions import PreConditionFunctions
from commons.constants import product_metadata
from commons.logger import LogLevels
from commons.logger import logger
from data_subscriber.cslc_utils import parse_cslc_file_name, parse_compressed_cslc_file_name
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)
from tools.stage_ancillary_map import main as stage_ancillary_map
from tools.stage_dem import main as stage_dem
from tools.stage_ionosphere_file import VALID_IONOSPHERE_TYPES
from tools.stage_worldcover import main as stage_worldcover
from util import datasets_json_util
from util.common_util import get_working_dir
from util.ecmwf_util import check_s3_for_ecmwf, ecmwf_key_for_datetime
from util.geo_util import bounding_box_from_slc_granule
from util.pge_util import (download_object_from_s3,
                           get_disk_usage,
                           get_input_hls_dataset_tile_code,
                           write_pge_metrics)


class OperaPreConditionFunctions(PreConditionFunctions):
    def __init__(self, context, pge_config, settings, job_params):
        PreConditionFunctions.__init__(
            self, context, pge_config, settings, job_params
        )

    def __get_keys_from_dict(self, input_dict, keys, attribute_names=None):
        """
        Returns a dict with the requested keys from the input dict
        :param input_dict:
        :param keys:
        :param attribute_names:
        :return:
        """
        new_dict = dict()
        if attribute_names is None:
            attribute_names = dict()
        for key in keys:
            if key in input_dict:
                attribute_name = attribute_names.get(key, key)
                new_dict.update({attribute_name: input_dict.get(key)})
        return new_dict

    def get_ancillary_inputs_coverage_flag(self):
        """Gets the setting for the check_ancillary_inputs_coverage flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        check_ancillary_inputs_coverage = self._settings.get("DSWX_HLS").get("CHECK_ANCILLARY_INPUTS_COVERAGE")

        rc_params = {
            "check_ancillary_inputs_coverage": check_ancillary_inputs_coverage
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_apply_ocean_masking_flag(self):
        """Gets the setting for the apply_ocean_masking flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        apply_ocean_masking = self._settings.get("DSWX_HLS").get("APPLY_OCEAN_MASKING")

        rc_params = {
            "apply_ocean_masking": apply_ocean_masking
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_cnm_version(self):
        # we may need to choose different CNM data version for different product types
        # for now, it is set as CNM_VERSION in settings.yaml
        cnm_version = self._settings.get(oc_const.CNM_VERSION)
        print("cnm_version: {}".format(cnm_version))
        return {"cnm_version": cnm_version}

    def get_cslc_product_specification_version(self):
        """
        Returns the appropriate product spec version for a CSLC-S1 job based
        on the workflow (baseline vs. static layer).

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_shortname = oc_const.L2_CSLC_S1[3:].upper()

        product_spec_version = self._settings.get(pge_shortname).get(oc_const.PRODUCT_SPEC_VER)

        rc_params = {
            "product_specification_version": product_spec_version
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_data_validity_start_date(self):
        """Gets the setting for the data_validity_start_date flag from settings.yaml"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_name = self._pge_config.get('pge_name')
        pge_shortname = pge_name[3:].upper()

        logger.info(f'Getting DATA_VALIDITY_START_DATE setting for PGE {pge_shortname}')

        data_validity_start_time = self._settings.get(pge_shortname).get("DATA_VALIDITY_START_DATE")

        rc_params = {
            "data_validity_start_date": data_validity_start_time
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_algorithm_parameters(self):
        """
        Gets the S3 path to the designated algorithm parameters runconfig for use
        with a DISP-S1 job. Takes processing mode into account (forward vs historical)
        to determine the correct parameters to load.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        processing_mode = self._context["processing_mode"]

        # Convert reprocessing mode to forward for sake of selecting a parameter config
        if processing_mode == oc_const.PROCESSING_MODE_REPROCESSING:
            processing_mode = oc_const.PROCESSING_MODE_FORWARD

        s3_bucket = self._pge_config.get(oc_const.GET_DISP_S1_ALGORITHM_PARAMETERS, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DISP_S1_ALGORITHM_PARAMETERS, {}).get(oc_const.S3_KEY)

        # Fill in the processing mode
        s3_key = s3_key.format(processing_mode=processing_mode)

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Algorithm Parameters Template"
        )

        rc_params = {
            oc_const.ALGORITHM_PARAMETERS: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_amplitude_dispersion_files(self):
        """
        Derives the list of S3 paths to the amplitude dispersion files to be
        used with a DISP-S1 job.

        TODO: currently a stub, implement once source of dispersion files is determined
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {
            oc_const.AMPLITUDE_DISPERSION_FILES: list()
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_amplitude_mean_files(self):
        """
        Derives the list of S3 paths to the amplitude mean files to be used with
        a  DISP-S1 job.

        TODO: currently a stub, implement once source of mean files is determined
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {
            oc_const.AMPLITUDE_MEAN_FILES: list()
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_compressed_cslc_files(self):
        """
        Derives the list of S3 paths to the ionosphere files to be used with a
        DISP-S1 job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata = self._context["product_metadata"]["metadata"]

        c_cslc_paths = metadata["product_paths"].get(oc_const.L2_CSLC_S1_COMPRESSED, [])

        rc_params = {
            oc_const.COMPRESSED_CSLC_PATHS: c_cslc_paths
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_dem(self):
        """
        This function downloads a DEM sub-region over the bounding box provided
        in the input product metadata for a DISP-S1 processing job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        # Get the bounding box for the sub-region to select
        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        bbox = metadata.get('bounding_box')

        # Get the s3 location parameters
        s3_bucket = self._pge_config.get(oc_const.GET_DISP_S1_DEM, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DISP_S1_DEM, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = s3_key
        args.outfile = output_filepath
        args.filepath = None
        args.bbox = bbox
        args.tile_code = None
        args.margin = int(self._settings.get("DISP_S1", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged DEM')

        pge_metrics = self.get_opera_ancillary(ancillary_type='DISP-S1 DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_frame_id(self):
        """
        Assigns the frame ID to the RunConfig for DISP-S1 PGE jobs.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]
        frame_id = metadata['frame_id']

        rc_params = {
            oc_const.FRAME_ID: frame_id
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_ionosphere_files(self):
        """
        Derives the list of S3 paths to the ionosphere files to be used with a
        DISP-S1 job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata = self._context["product_metadata"]["metadata"]

        ionosphere_paths = metadata["product_paths"].get("IONOSPHERE_TEC", [])

        rc_params = {
            oc_const.IONOSPHERE_FILES: ionosphere_paths
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_mask_file(self):
        """
        This function downloads a sub-region of the water mask used with DISP-S1
        processing over the bounding box provided in the input product metadata.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        bbox = metadata.get('bounding_box')

        s3_bucket = self._pge_config.get(oc_const.GET_DISP_S1_MASK_FILE, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DISP_S1_MASK_FILE, {}).get(oc_const.S3_KEY)

        ancillary_type = "Water mask"
        output_filepath = os.path.join(working_dir, 'water_mask.vrt')

        # Set up arguments to stage_ancillary_map.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.outfile = output_filepath
        args.s3_bucket = s3_bucket
        args.s3_key = s3_key
        args.bbox = bbox
        args.margin = int(self._settings.get("DISP_S1", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged {ancillary_type}')

        pge_metrics = self.get_opera_ancillary(
            ancillary_type=ancillary_type,
            output_filepath=output_filepath,
            staging_func=stage_ancillary_map,
            staging_func_args=args
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params[oc_const.MASK_FILE] = output_filepath

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_num_workers(self):
        """
        Determines the number of workers/cores to assign to an DISP-S1 job as a
        fraction of the total available.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        available_cores = os.cpu_count()

        # Use all available cores for threads_per_worker
        threads_per_worker = available_cores

        logger.info(f"Allocating {threads_per_worker=} out of {available_cores} available")

        # Use (1/2 + 1) of the available cores for parallel burst processing
        n_parallel_bursts = max(int(round(available_cores / 2)) + 1, 1)

        logger.info(f"Allocating {n_parallel_bursts=} out of {available_cores} available")

        rc_params = {
            "threads_per_worker": str(threads_per_worker),
            "n_parallel_bursts": str(n_parallel_bursts)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_polarization(self):
        """
        Determines the polarization value of the CSLC-S1 products used with a
        DISP-S1 job
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        dataset_type = self._context["dataset_type"]

        product_paths = metadata["product_paths"][dataset_type]

        # Define a regex pattern to match and extract the polarization field from
        # a CSLC-S1 tif product filename
        pattern = re.compile(r".*_(?P<pol>VV|VH|HH|HV)_.*\.h5")

        # Filter out all products to just those with a polarization field in the
        # filename
        polarization_layers = filter(
            lambda path: pattern.match(os.path.basename(path)), product_paths
        )

        # Reduce each product filename to just the polarization field value
        available_polarizations = map(
            lambda path: pattern.match(os.path.basename(path)).groupdict()['pol'],
            list(polarization_layers)
        )

        # Reduce again to just the unique set of polarization fields
        unique_polarizations = set(list(available_polarizations))

        # Make sure we are left with only a single polarization value
        if len(unique_polarizations) == 0:
            raise ValueError('No polarization fields parsed from input CSLC product set')

        if len(unique_polarizations) > 1:
            raise ValueError(f'More than one ({len(unique_polarizations)}) polarization values '
                             f'parsed from set of input CSLC granules')

        rc_params[oc_const.POLARIZATION] = list(unique_polarizations)[0]

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_product_type(self):
        """
        Assigns the product type (forward/historical) to the RunConfig for
        DISP-S1 PGE jobs.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        processing_mode = self._context["processing_mode"]

        rc_params = {
            oc_const.PRODUCT_TYPE: (oc_const.DISP_S1_HISTORICAL
                                    if processing_mode == oc_const.PROCESSING_MODE_HISTORICAL
                                    else oc_const.DISP_S1_FORWARD)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_save_compressed_slc(self):
        """
        Assigns the save_compressed_slc flag based on the value passed to the
        job from the CSLC download job
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        save_compressed_cslc = metadata["save_compressed_cslc"]

        rc_params = {
            "save_compressed_slc": save_compressed_cslc
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_static_layers_files(self):
        """
        Derives the S3 paths to the CSLC static layer files to be used with a
        DISP-S1 job.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata = self._context["product_metadata"]["metadata"]

        static_layers_paths = metadata["product_paths"].get("L2_CSLC_S1_STATIC", [])

        rc_params = {
            oc_const.STATIC_LAYERS_FILES: static_layers_paths
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_disp_s1_troposphere_files(self):
        """
        Derives the S3 paths to the Troposphere (ECMWF) files to be used with a
        DISP-S1 job.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_shortname = oc_const.L3_DISP_S1[3:].upper()

        strict_mode = self._settings.get(pge_shortname).get("STRICT_ANCILLARY_USAGE")

        metadata = self._context["product_metadata"]["metadata"]

        cslc_paths = metadata["product_paths"].get(oc_const.L2_CSLC_S1, [])
        compressed_cslc_paths = metadata["product_paths"].get(oc_const.L2_CSLC_S1_COMPRESSED, [])

        s3_bucket = self._pge_config.get(oc_const.GET_DISP_S1_TROPOSPHERE_FILES, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DISP_S1_TROPOSPHERE_FILES, {}).get(oc_const.S3_KEY)

        acquisition_datetimes = set()

        for cslc_path in cslc_paths + compressed_cslc_paths:
            cslc_native_id = os.path.splitext(os.path.basename(cslc_path))[0]

            if "compressed" in cslc_path.lower():
                _, acquisition_date_str = parse_compressed_cslc_file_name(cslc_native_id)
                acquisition_datetime_str = acquisition_date_str + "T000000Z"
            else:
                _, acquisition_datetime_str = parse_cslc_file_name(cslc_native_id)

            acquisition_datetime = datetime.strptime(
                acquisition_datetime_str, "%Y%m%dT%H%M%SZ"
            )

            acquisition_datetimes.add(acquisition_datetime)

        troposphere_s3_paths = [f"s3://{s3_bucket}/{s3_key}/{ecmwf_key_for_datetime(acquisition_datetime)}"
                                for acquisition_datetime in acquisition_datetimes]

        troposphere_s3_paths = list(set(troposphere_s3_paths))  # Remove any potential duplicates

        if not all(
            check_s3_for_ecmwf(troposphere_s3_path) for troposphere_s3_path in troposphere_s3_paths
        ):
            if strict_mode:
                raise RuntimeError(f"One or more expected ECMWF files is missing from {s3_bucket}/{s3_key}")
            else:
                logger.warning("One or more expected ECMWF files is missing from %s/%s", s3_bucket, s3_key)
                logger.warning("No Tropospheres files will be included for this DISP-S1 job")

                troposphere_s3_paths = list()

        rc_params = {
            oc_const.TROPOSPHERE_FILES: troposphere_s3_paths
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_hls_dem(self):
        """
        This function downloads dems over the bbox provided in the PGE yaml config,
        or derives the appropriate bbox based on the tile code of the product's
        metadata (if available).
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        # get s3_bucket param
        s3_bucket = self._pge_config.get(oc_const.GET_DSWX_HLS_DEM, {}).get(oc_const.S3_BUCKET)

        # get bbox param
        bbox = self._pge_config.get(oc_const.GET_DSWX_HLS_DEM, {}).get(oc_const.BBOX)

        if bbox:
            # Convert to list if we were given a space-delimited string
            if not isinstance(bbox, list):
                bbox = list(map(float, bbox.split()))

            logger.info(f"Got bbox from PGE config: {bbox}")

        # get MGRS tile code, if available from product metadata
        tile_code = get_input_hls_dataset_tile_code(self._context)

        if tile_code:
            logger.info(f'Derived MGRS tile code {tile_code} from product metadata')
        else:
            logger.warning('Could not determine a tile code from product metadata')

        # do checks
        if bbox is None and tile_code is None:
            raise RuntimeError(
                f"Can not determine a region to obtain DEM for.\n"
                f"The product metadata must specify an MGRS tile code, "
                f"or the 'bbox' parameter must be provided in the "
                f"'{oc_const.GET_DSWX_HLS_DEM}' area of the PGE config"
            )

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = ""
        args.outfile = output_filepath
        args.filepath = None
        args.margin = int(self._settings.get("DSWX_HLS", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged DEM')

        # Provide both the bounding box and tile code, stage_dem.py should
        # give preference to the tile code over the bbox if both are provided.
        args.bbox = bbox
        args.tile_code = tile_code

        pge_metrics = self.get_opera_ancillary(ancillary_type='DSWx-HLS DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_hls_input_filepaths(self) -> Dict:
        """Returns a partial RunConfig containing the s3 paths of the published L2_HLS products."""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        work_dir = get_working_dir()
        with open(PurePath(work_dir) / "datasets.json") as fp:
            datasets_json_dict = json.load(fp)
        with open(PurePath(work_dir) / "_job.json") as fp:
            job_json_dict = json.load(fp)
            dataset_type = job_json_dict["params"]["dataset_type"]

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]
        product_paths: List[str] = []
        for file in metadata["Files"]:
            # Example publish location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
            publish_location = str(datasets_json_util.find_publish_location_s3(datasets_json_dict, dataset_type).parent) \
                .removeprefix("s3:/").removeprefix("/")  # handle prefix changed by PurePath
            product_path = f's3://{publish_location}/{self._context["input_dataset_id"]}/{file["FileName"]}'
            product_paths.append(product_path)

        # Used in conjunction with PGE Config YAML's $.localize_groups and its referenced properties in $.runconfig.
        # Compare key names of $.runconfig entries, referenced indirectly via $.localize_groups, with this dict.
        return {"L2_HLS": product_paths}

    def get_dswx_ni_sample_inputs(self):
        """
        Temporary function to stage the "golden" inputs for use with the DSWx-NI
        PGE.
        TODO: this function will eventually be phased out as functions to
              acquire the appropriate input files are implemented with future
              releases
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        s3_bucket = "operasds-dev-pge"
        s3_key = "dswx_ni/dswx_ni_interface_0.1_expected_input.zip"

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="DSWx-S1 Inputs"
        )

        import zipfile
        with zipfile.ZipFile(output_filepath) as myzip:
            zip_contents = myzip.namelist()
            zip_contents = list(filter(lambda x: not x.startswith('__'), zip_contents))
            zip_contents = list(filter(lambda x: not x.endswith('.DS_Store'), zip_contents))
            myzip.extractall(path=working_dir, members=zip_contents)

        rtc_data_dir = os.path.join(working_dir, 'dswx_ni_interface_0.1_expected_input', 'input_dir', 'RTC')
        ancillary_data_dir = os.path.join(working_dir, 'dswx_ni_interface_0.1_expected_input', 'input_dir', 'ancillary_data')

        rtc_files = os.listdir(rtc_data_dir)

        rtc_file_list = [os.path.join(rtc_data_dir, rtc_file) for rtc_file in rtc_files]

        rc_params = {
            'input_file_paths': rtc_file_list,
            'dem_file': os.path.join(ancillary_data_dir, 'dem.tif'),
            'hand_file': os.path.join(ancillary_data_dir, 'hand.tif'),
            'worldcover_file': os.path.join(ancillary_data_dir, 'worldcover.tif'),
            'reference_water_file': os.path.join(ancillary_data_dir, 'reference_water.tif'),
            'algorithm_parameters': os.path.join(ancillary_data_dir, 'algorithm_parameter_ni.yaml'),
            'mgrs_database_file': os.path.join(ancillary_data_dir, 'MGRS_tile.sqlite'),
            'mgrs_collection_database_file': os.path.join(ancillary_data_dir, 'MGRS_collection_db_DSWx-NI_v0.1.sqlite'),
            'input_mgrs_collection_id': "MS_131_19"
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_algorithm_parameters(self):
        """
        Downloads the designated algorithm parameters runconfig from S3 for use
        with a DSWx-S1 job
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        s3_bucket = self._pge_config.get(oc_const.GET_DSWX_S1_ALGORITHM_PARAMETERS, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DSWX_S1_ALGORITHM_PARAMETERS, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Algorithm Parameters Template"
        )

        rc_params = {
            oc_const.ALGORITHM_PARAMETERS: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_dem(self):
        """
        This function downloads a DEM sub-region over the bounding box provided
        in the input product metadata for a DSWx-S1 processing job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        # Get the bounding box for the sub-region to select
        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        bbox = metadata.get('bounding_box')

        # Get the s3 location parameters
        s3_bucket = self._pge_config.get(oc_const.GET_DSWX_S1_DEM, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_DSWX_S1_DEM, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = s3_key
        args.outfile = output_filepath
        args.filepath = None
        args.bbox = bbox
        args.tile_code = None
        args.margin = int(self._settings.get("DSWX_S1", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged DEM')

        pge_metrics = self.get_opera_ancillary(ancillary_type='DSWx-S1 DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_dynamic_ancillary_maps(self):
        """
        Utilizes the stage_ancillary_map.py script to stage the sub-regions for
        each of the ancillary maps used by DSWx-S1 (excluding the DEM).
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        bbox = metadata.get('bounding_box')

        dynamic_ancillary_maps = self._pge_config.get(oc_const.GET_DSWX_S1_DYNAMIC_ANCILLARY_MAPS, {})

        for dynamic_ancillary_map_name in dynamic_ancillary_maps.keys():
            s3_bucket = dynamic_ancillary_maps.get(dynamic_ancillary_map_name, {}).get(oc_const.S3_BUCKET)
            s3_key = dynamic_ancillary_maps.get(dynamic_ancillary_map_name, {}).get(oc_const.S3_KEY)

            ancillary_type = dynamic_ancillary_map_name.replace("_", " ").capitalize()
            output_filepath = os.path.join(working_dir, f'{dynamic_ancillary_map_name}.vrt')

            # Set up arguments to stage_ancillary_map.py
            # Note that since we provide an argparse.Namespace directly,
            # all arguments must be specified, even if it's only with a null value
            args = argparse.Namespace()
            args.outfile = output_filepath
            args.s3_bucket = s3_bucket
            args.s3_key = s3_key
            args.bbox = bbox
            args.margin = int(self._settings.get("DSWX_S1", {}).get("ANCILLARY_MARGIN", 50))  # KM
            args.log_level = LogLevels.INFO.value

            logger.info(f'Using margin value of {args.margin} with staged {ancillary_type}')

            pge_metrics = self.get_opera_ancillary(
                ancillary_type=ancillary_type,
                output_filepath=output_filepath,
                staging_func=stage_ancillary_map,
                staging_func_args=args
            )

            write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

            rc_params[dynamic_ancillary_map_name] = output_filepath

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_inundated_vegetation_enabled(self):
        """
        Determines the setting for the inundated vegetation enabled flag for
        DSWx-S1 processing, based on the set of input RTC granules to be processed.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        dataset_type = self._context["dataset_type"]

        product_paths = metadata["product_paths"][dataset_type]

        # Define a regex pattern to match and extract the polarization field from
        # an RTC-S1 tif product filename
        pattern = re.compile(r".*_(?P<pol>VV|VH|HH|HV).tif")

        # Filter out all products to just those with a polarization field in the
        # filename
        polarization_layers = filter(
            lambda path: pattern.match(os.path.basename(path)), product_paths
        )

        # Reduce each product filename to just the polarization field value
        available_polarizations = map(
            lambda path: pattern.match(os.path.basename(path)).groupdict()['pol'],
            list(polarization_layers)
        )

        # Reduce again to just the unique set of polarization fields
        unique_polarizations = set(list(available_polarizations))

        if len(unique_polarizations) == 0:
            raise ValueError('No polarization fields parsed from input product set')

        # Disable the inundated vegetation check if only a single polarization
        # channel is available
        rc_params[oc_const.INUNDATED_VEGETATION_ENABLED] = len(unique_polarizations) > 1

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_mgrs_collection_id(self):
        """
        Inserts the MGRS collection ID from the job metadata into the RunConfig
        for use with a DSWx-S1 job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        mgrs_set_id = metadata["mgrs_set_id"]

        rc_params[oc_const.INPUT_MGRS_COLLECTION_ID] = mgrs_set_id

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_dswx_s1_num_workers(self):
        """
        Determines the number of workers/cores to assign to an DSWx-S1 job as a
        function of the total available.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        available_cores = os.cpu_count()

        # Use one less than the available cores for standard processing
        num_workers = max(available_cores - 1, 1)

        logger.info(f"Allocating {num_workers} core(s) out of {available_cores} available")

        rc_params = {
            "num_workers": str(num_workers)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_gpu_enabled(self):
        logger.info(
            "Calling {} pre-condition function".format(oc_const.GPU_ENABLED))

        # read in SciFlo work unit json file and extract work directory
        work_unit_file = os.path.abspath("workunit.json")
        with open(work_unit_file) as f:
            work_unit = json.load(f)
        work_dir = os.path.dirname(work_unit["args"][0])

        # extract docker params for the PGE
        docker_params_file = os.path.join(work_dir, "_docker_params.json")
        with open(docker_params_file) as f:
            docker_params = json.load(f)
        container_image_name = self._context["job_specification"]["dependency_images"][
            0
        ]["container_image_name"]
        pge_docker_params = docker_params[container_image_name]

        # detect GPU
        gpu_enabled = (
            True if "gpus" in pge_docker_params.get("runtime_options", {}) else False
        )
        return {oc_const.GPU_ENABLED: gpu_enabled}

    def get_landcover(self):
        """
        Copies the static landcover file configured for use with a DSWx-HLS job
        to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'landcover.tif')

        s3_bucket = self._pge_config.get(oc_const.GET_LANDCOVER, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_LANDCOVER, {}).get(oc_const.S3_KEY)

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Landcover"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.LANDCOVER_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_metadata(self):
        """
        Returns a dict with only the key: value pair for keys in 'keys' from the
        input_context

        :return: dict or raises error if not found
        """
        logger.info("Evaluating precondition {}".format(oc_const.GET_METADATA))
        keys = self._pge_config.get(oc_const.GET_METADATA).get("keys")
        try:
            metadata = self.__get_keys_from_dict(self._context, keys)
            logger.info("Returning the following context metadata to the job_params: {}".format(json.dumps(metadata)))
            return metadata
        except Exception as e:
            logger.error(
                "Could not extract metadata from input "
                "context: {}".format(traceback.format_exc())
            )
            raise RuntimeError(
                "Could not extract metadata from input context: {}".format(e)
            )

    def get_opera_ancillary(self, ancillary_type, output_filepath, staging_func, staging_func_args):
        """
        Handles common operations for obtaining ancillary data used with OPERA
        PGE processing
        """
        pge_metrics = {"download": [], "upload": []}

        loc_t1 = datetime.utcnow()

        try:
            staging_func(staging_func_args)
            logger.info(f"Created {ancillary_type} file : {output_filepath}")
        except Exception as e:
            trace = traceback.format_exc()
            error = str(e)
            raise RuntimeError(
                f"Failed to download {ancillary_type} file, reason: {error}\n{trace}"
            )

        loc_t2 = datetime.utcnow()
        loc_dur = (loc_t2 - loc_t1).total_seconds()
        path_disk_usage = get_disk_usage(output_filepath)

        pge_metrics["download"].append(
            {
                "url": output_filepath,
                "path": output_filepath,
                "disk_usage": path_disk_usage,
                "time_start": loc_t1.isoformat() + "Z",
                "time_end": loc_t2.isoformat() + "Z",
                "duration": loc_dur,
                "transfer_rate": path_disk_usage / loc_dur,
            }
        )
        logger.info(json.dumps(pge_metrics, indent=2))

        return pge_metrics

    # TODO: multiple functions with this name across OPERA PCM, can they be
    #       consolidated?
    def get_product_metadata(self):
        """
        To get the metadata that was extracted from the products generated by the
        previous PGE run

        :return:
        """
        logger.info("Evaluating precondition {}".format(
            oc_const.GET_PRODUCT_METADATA))
        try:
            if self._context.get(oc_const.PRODUCTS_METADATA) is None:
                raise ValueError(
                    "No product metadata key found in the input context")
            keys = self._pge_config.get(
                oc_const.GET_PRODUCT_METADATA).get("keys", [])
            metadata = dict()
            metadata_obj = self._context.get(oc_const.PRODUCTS_METADATA)
            logger.debug("Found Product Metadata: {}".format(json.dumps(metadata_obj)))
            attribute_names = self._pge_config.get(oc_const.GET_PRODUCT_METADATA).get("attribute_names", {})
            if isinstance(metadata_obj, list):
                for product in metadata_obj:
                    metadata.update(self.__get_keys_from_dict(product.get("metadata"), keys, attribute_names))
            else:
                metadata.update(self.__get_keys_from_dict(metadata_obj.get("metadata"), keys, attribute_names))
            logger.info("Returning the following metadata to the job_params: {}".format(json.dumps(metadata)))
            logger.debug(json.dumps(metadata))
            return metadata

        except Exception as e:
            logger.error("Could not extract product metadata: {}".format(traceback))
            raise RuntimeError("Could not extract product metadata: {}".format(e))

    def get_product_version(self):
        """Assigns the product version specified in settings.yaml to PGE RunConfig"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        version_key = self._pge_config.get(oc_const.GET_PRODUCT_VERSION, {}).get(oc_const.VERSION_KEY)

        product_version = self._settings.get(version_key)

        if not product_version:
            raise RuntimeError(
                f"No value set for {version_key} in settings.yaml"
            )

        rc_params = {
            oc_const.PRODUCT_VERSION: product_version
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_rtc_s1_estimated_geometric_accuracy_values(self):
        """
        Returns the estimated geometric accuracy values from settings.yaml
        for inclusion in the instantiated RTC-S1 RunConfig. These values are
        needed for CEOS metadata compliance.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        pge_shortname = oc_const.L2_RTC_S1[3:].upper()

        estimated_geographic_accuracy_values = self._settings.get(pge_shortname).get(
            oc_const.ESTIMATED_GEOMETRIC_ACCURACY)

        rc_params = {
            "estimated_geometric_accuracy_bias_x": estimated_geographic_accuracy_values["BIAS_X"],
            "estimated_geometric_accuracy_bias_y": estimated_geographic_accuracy_values["BIAS_Y"],
            "estimated_geometric_accuracy_stddev_x": estimated_geographic_accuracy_values["STDDEV_X"],
            "estimated_geometric_accuracy_stddev_y": estimated_geographic_accuracy_values["STDDEV_Y"]
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_rtc_s1_num_workers(self):
        """
        Determines the number of workers/cores to assign to an RTC-S1 job as a
        fraction of the total available.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        available_cores = os.cpu_count()

        # Use 3/4th of the available cores for standard processing
        num_workers = max(int(round((available_cores * 3) / 4)), 1)

        logger.info(f"Allocating {num_workers} core(s) out of {available_cores} available")

        rc_params = {
            "num_workers": str(num_workers)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_rtc_s1_static_num_workers(self):
        """
        Determines the number of workers/cores to assign to an RTC-S1-STATIC job
        as a fraction of the total available.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        available_cores = os.cpu_count()

        # Use 1/2 of the available cores for static layer processing
        num_workers = max(int(round(available_cores / 2)), 1)

        logger.info(f"Allocating {num_workers} core(s) out of {available_cores} available")

        rc_params = {
            "num_workers": str(num_workers)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_s3_input_filepaths(self):
        """
        Gets the set of input S3 file paths that comprise the set of products
        to be processed by a DSWx-S1/DISP-S1 PGE job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        dataset_type = self._context["dataset_type"]

        product_paths = metadata["product_paths"][dataset_type]

        # Condense the full set of file paths to just a set of the directories
        # to be localized
        product_set = set(map(lambda path: os.path.dirname(path), product_paths))

        rc_params = {
            oc_const.INPUT_FILE_PATHS: list(product_set)
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_shoreline_shapefiles(self):
        """
        Copies the set of static shoreline shapefiles configured for use with a
        DSWx-HLS job to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        rc_params = {}

        s3_bucket = self._pge_config.get(oc_const.GET_SHORELINE_SHAPEFILES, {}).get(oc_const.S3_BUCKET)
        s3_keys = self._pge_config.get(oc_const.GET_SHORELINE_SHAPEFILES, {}).get(oc_const.S3_KEYS)

        for s3_key in s3_keys:
            output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

            pge_metrics = download_object_from_s3(
                s3_bucket, s3_key, output_filepath, filetype="Shoreline Shapefile"
            )

            write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

            # Set up the main shapefile which is configured in the PGE RunConfig
            if output_filepath.endswith(".shp"):
                rc_params = {
                    oc_const.SHORELINE_SHAPEFILE: output_filepath
                }

        # Make sure the .shp file was included in the set of files localized from S3
        if not rc_params:
            raise RuntimeError("No .shp file included with the localized Shoreline Shapefile dataset.")

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_polarization(self):
        """
        Determines the polarization setting for the CSLC-S1 or RTC-S1 job based
        on the file name of the input SLC granule.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        slc_filename = metadata['FileName']

        slc_regex = "(S1A|S1B)_IW_SLC__1S(?P<pol>SH|SV|DH|DV).*"

        result = re.search(slc_regex, slc_filename)

        if not result:
            raise RuntimeError(
                f'Could not parse Polarization from SLC granule {slc_filename}'
            )

        pol = result.groupdict()['pol']

        logger.info(f'Parsed Polarization mode {pol} from SLC granule {slc_filename}')

        polarization_map = {
            'SH': 'co-pol',
            'SV': 'co-pol',
            'DH': 'dual-pol',
            'DV': 'dual-pol'
        }

        slc_polarization = polarization_map[pol]

        rc_params = {
            oc_const.POLARIZATION: slc_polarization
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_burst_database(self):
        """
        Copies the static burst database file configured for use with an SLC-based
        job to the job's local working area.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'opera_burst_database.sqlite3')

        s3_bucket = self._pge_config.get(oc_const.GET_SLC_S1_BURST_DATABASE, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_SLC_S1_BURST_DATABASE, {}).get(oc_const.S3_KEY)

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="Burst Database"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.BURST_DATABASE_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_dem(self):
        """
        Stages a DEM file corresponding to the region covered by an input
        S1 SLC SAFE archive.

        The manifest.safe file is extracted from the archive and used to
        determine the lat/lon bounding box of the S1 swath. This bbox is then
        used with the stage_dem tool to obtain the appropriate DEM.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        # get the local file path of the input SAFE archive (should have already
        # been downloaded by the get_safe_file precondition function)
        safe_file_path = self._job_params.get(oc_const.SAFE_FILE_PATH)

        # get s3_bucket param
        s3_bucket = self._pge_config.get(oc_const.GET_SLC_S1_DEM, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.GET_SLC_S1_DEM, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, 'dem.vrt')

        logger.info(f"working_dir : {working_dir}")
        logger.info(f"safe_file_path: {safe_file_path}")
        logger.info(f"s3_bucket: {s3_bucket}")
        logger.info(f"s3_key: {s3_key}")
        logger.info(f"output_filepath: {output_filepath}")

        if not safe_file_path:
            raise RuntimeError(f'No value set for {oc_const.SAFE_FILE_PATH} in '
                               f'job parameters. Please ensure the get_safe_file '
                               f'precondition function has been run prior to this one.')

        bbox = bounding_box_from_slc_granule(safe_file_path)

        logger.info(f"Derived DEM bounding box: {bbox}")

        pge_name = self._pge_config.get('pge_name')
        pge_shortname = pge_name[3:].upper()

        logger.info(f'Getting ANCILLARY_MARGIN setting for PGE {pge_shortname}')

        margin = self._settings.get(pge_shortname).get("ANCILLARY_MARGIN")

        logger.info(f'Using margin value of {margin} with staged DEM')

        # Set up arguments to stage_dem.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = s3_key
        args.outfile = output_filepath
        args.filepath = None
        args.margin = margin  # KM
        args.log_level = LogLevels.INFO.value
        args.bbox = bbox
        args.tile_code = None

        pge_metrics = self.get_opera_ancillary(ancillary_type='S1 DEM',
                                               output_filepath=output_filepath,
                                               staging_func=stage_dem,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.DEM_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_orbit_file(self):
        """
        Obtains the S3 location of the orbit file configured for use with a
        CSLC-S1 or RTC-S1 job.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        s3_product_path = self._context['product_path']

        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket_name = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(s3_bucket_name)
        s3_objects = bucket.objects.filter(Prefix=s3_key)

        orbit_file_objects = list(
            filter(lambda s3_object: s3_object.key.endswith('.EOF'), s3_objects)
        )

        if len(orbit_file_objects) < 1:
            raise RuntimeError(
                f'Could not find any orbit files within the S3 location {s3_product_path}'
            )

        s3_orbit_file_paths = [f"s3://{s3_bucket_name}/{orbit_file_object.key}"
                               for orbit_file_object in orbit_file_objects]

        # Assign the s3 location of the orbit file to the chimera config,
        # it will be localized for us automatically
        rc_params = {
            oc_const.ORBIT_FILE_PATH: s3_orbit_file_paths
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_safe_file(self):
        """
        Obtains the input SAFE file for use with an CSLC-S1 or RTC-S1 job.
        This local path is then configured as the value of safe_file_path within the
        interim RunConfig.

        The SAFE file is manually localized here, so it will be available for
        use when obtaining the corresponding DEM.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        metadata: Dict[str, str] = self._context["product_metadata"]["metadata"]

        s3_product_path = f"{self._context['product_path']}/{metadata['FileName']}"
        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        logger.info(f"working_dir : {working_dir}")
        logger.info(f"s3_product_path : {s3_product_path}")
        logger.info(f"s3_bucket: {s3_bucket}")
        logger.info(f"output_filepath: {output_filepath}")

        pge_metrics = download_object_from_s3(
            s3_bucket, s3_key, output_filepath, filetype="SAFE"
        )

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.SAFE_FILE_PATH: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_slc_s1_tec_file(self):
        """
        Stages an Ionosphere Correction (TEC) file for use with a CSLC-S1
        job. The name of the SLC archive to be processed is used to obtain
        the date of the corresponding TEC file to download. The stage_ionosphere_file.py
        script is then used to perform the download.

        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        s3_product_path = self._context['product_path']

        parsed_s3_url = urlparse(s3_product_path)
        s3_path = parsed_s3_url.path

        # Strip leading forward slash from url path
        if s3_path.startswith('/'):
            s3_path = s3_path[1:]

        # Bucket name should be first part of url path, the key is the rest
        s3_bucket_name = s3_path.split('/')[0]
        s3_key = '/'.join(s3_path.split('/')[1:])

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(s3_bucket_name)
        s3_objects = bucket.objects.filter(Prefix=s3_key)

        # Find the available Ionosphere files staged by the download job
        ionosphere_file_objects = []
        for ionosphere_file_type in VALID_IONOSPHERE_TYPES + ['RAP', 'FIN']:
            ionosphere_file_objects.extend(
                list(filter(lambda s3_object: ionosphere_file_type in s3_object.key, s3_objects))
            )
        logger.info(f"{ionosphere_file_objects}=")

        # May not of found any Ionosphere files during download phase, so check now
        if len(ionosphere_file_objects) < 1:
            raise RuntimeError(
                f'Could not find an Ionosphere file within the S3 location {s3_product_path}'
            )

        # There should only have been one file downloaded, but any should
        # work so just take the first
        ionosphere_file_object = ionosphere_file_objects[0]

        s3_ionosphere_file_path = f"s3://{s3_bucket_name}/{ionosphere_file_object.key}"

        # Assign the s3 location of the Ionosphere file to the chimera config,
        # it will be localized for us automatically
        rc_params = {
            oc_const.TEC_FILE: s3_ionosphere_file_path
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_static_ancillary_files(self):
        """
        Gets the S3 paths to the configured static ancillary input files.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        rc_params = {}

        static_ancillary_products = self._pge_config.get(oc_const.GET_STATIC_ANCILLARY_FILES, {})

        for static_ancillary_product in static_ancillary_products.keys():
            s3_bucket = static_ancillary_products.get(static_ancillary_product, {}).get(oc_const.S3_BUCKET)
            s3_key = static_ancillary_products.get(static_ancillary_product, {}).get(oc_const.S3_KEY)

            rc_params[static_ancillary_product] = f"s3://{s3_bucket}/{s3_key}"

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_static_product_version(self):
        """Assigns the static layer product version specified in settings.yaml to PGE RunConfig"""
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        version_key = self._pge_config.get(oc_const.GET_STATIC_PRODUCT_VERSION, {}).get(oc_const.VERSION_KEY)

        product_version = self._settings.get(version_key)

        if not product_version:
            raise RuntimeError(
                f"No value set for {version_key} in settings.yaml"
            )

        rc_params = {
            oc_const.STATIC_PRODUCT_VERSION: product_version
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def get_worldcover(self):
        """
        This function downloads a Worldcover map over the bbox provided in the
        PGE yaml config, or derives the appropriate bbox based on the tile code
        of the product's metadata (if available).
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        output_filepath = os.path.join(working_dir, 'worldcover.vrt')

        # get s3 bucket/key params
        s3_bucket = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.S3_BUCKET)
        worldcover_ver = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.WORLDCOVER_VER)
        worldcover_year = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.WORLDCOVER_YEAR)

        # get bbox param
        bbox = self._pge_config.get(oc_const.GET_WORLDCOVER, {}).get(oc_const.BBOX)

        if bbox:
            # Convert to list if we were given a space-delimited string
            if not isinstance(bbox, list):
                bbox = list(map(float, bbox.split()))

            logger.info(f"Got bbox from PGE config: {bbox}")

        # get MGRS tile code, if available from product metadata
        tile_code = get_input_hls_dataset_tile_code(self._context)

        if tile_code:
            logger.info(f'Derived MGRS tile code {tile_code} from product metadata')
        else:
            logger.warning('Could not determine a tile code from product metadata')

        if bbox is None and tile_code is None:
            raise RuntimeError(
                f"Can not determine a region to obtain a Worldcover map for.\n"
                f"The product metadata must specify an MGRS tile code, "
                f"or the '{oc_const.BBOX}' parameter must be provided in the "
                f"'{oc_const.GET_WORLDCOVER}' area of the PGE config"
            )

        # Set up arguments to stage_worldcover.py
        # Note that since we provide an argparse.Namespace directly,
        # all arguments must be specified, even if it's only with a null value
        args = argparse.Namespace()
        args.s3_bucket = s3_bucket
        args.s3_key = ""
        args.worldcover_ver = worldcover_ver
        args.worldcover_year = worldcover_year
        args.outfile = output_filepath
        args.margin = int(self._settings.get("DSWX_HLS", {}).get("ANCILLARY_MARGIN", 50))  # KM
        args.log_level = LogLevels.INFO.value

        logger.info(f'Using margin value of {args.margin} with staged Worldcover')

        # Provide both the bounding box and tile code, stage_worldcover.py should
        # give preference to the tile code over the bbox if both are provided.
        args.bbox = bbox
        args.tile_code = tile_code

        pge_metrics = self.get_opera_ancillary(ancillary_type='Worldcover',
                                               output_filepath=output_filepath,
                                               staging_func=stage_worldcover,
                                               staging_func_args=args)

        write_pge_metrics(os.path.join(working_dir, "pge_metrics.json"), pge_metrics)

        rc_params = {
            oc_const.WORLDCOVER_FILE: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def instantiate_algorithm_parameters_template(self):
        """
        Downloads a template algorithm parameters yaml file from S3, then
        performs string replacement in memory to instantiate the template.
        String replacement is determined by a pattern mapping associated with
        the chimera configuration for this function.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        logger.info("working_dir : {}".format(working_dir))

        # Get the path to the templated parameters file which should have already been
        # downloaded at this point
        output_filepath = self._job_params[oc_const.ALGORITHM_PARAMETERS]

        with open(output_filepath, 'r') as infile:
            template_contents = infile.read()

        instantiated_contents = template_contents

        # Pull the mapping of parameters names to the template patterns to be
        # replaced with said parameter's value
        template_mappings = self._pge_config.get(
            oc_const.INSTANTIATE_ALGORITHM_PARAMETERS_TEMPLATE, {}).get(oc_const.TEMPLATE_MAPPING)

        # Replace each pattern with a job parameter value
        for parameter, pattern in template_mappings.items():
            try:
                value = self._job_params[parameter]
            except KeyError:
                raise RuntimeError(f'No value for parameter {parameter} in _job_params')

            logger.info(f'Replacing pattern {pattern} with value {value}')
            instantiated_contents = instantiated_contents.replace(pattern, str(value))

        # Strip the .tmpl suffix to derive the instantiated output filename
        output_filepath = output_filepath.replace(".tmpl", "")

        with open(output_filepath, 'w') as outfile:
            outfile.write(instantiated_contents)

        # Return the updated path to the instantiated template, so it can be
        # written to the runconfig used with the PGE job
        rc_params = {
            oc_const.ALGORITHM_PARAMETERS: output_filepath
        }

        logger.info(f"rc_params : {rc_params}")

        return rc_params

    def set_daac_product_type(self):
        """
        Sets the DAAC product type
        :return: a DAAC product type that will use to populate collection value in CNM-S msg
        """
        logger.info("Calling function {} function".format(oc_const.SET_DAAC_PRODUCT_TYPE))
        template = self._pge_config.get(oc_const.SET_DAAC_PRODUCT_TYPE, {}).get(
            "template", None
        )

        if template:
            daac_product_type = template.format(**self._job_params)
            print("daac_product_type: {}".format(daac_product_type))
            return {product_metadata.DAAC_PRODUCT_TYPE: daac_product_type}
        else:
            raise RuntimeError(
                "Must define a 'template' field for the {} function".format(
                    oc_const.SET_DAAC_PRODUCT_TYPE
                )
            )

    def set_extra_pge_output_metadata(self):
        logger.info(
            "Calling {} pre-condition function".format(
                oc_const.SET_EXTRA_PGE_OUTPUT_METADATA
            )
        )
        extra_met = dict()
        for met_key, job_params_key in self._pge_config.get(
                oc_const.SET_EXTRA_PGE_OUTPUT_METADATA
        ).items():
            if job_params_key in self._job_params:
                extra_met[met_key] = self._job_params.get(job_params_key)
            else:
                raise RuntimeError(
                    "Cannot find {} in the job_params dictionary".format(job_params_key)
                )
        return {oc_const.EXTRA_PGE_OUTPUT_METADATA: extra_met}

    def set_sample_product_metadata(self):
        """
        Overwrites the "product_metadata" field of the context dictionary with
        the contents of a JSON file read from S3. This function is only intended
        for use with testing of PGE SCIFLO workflows, and should not be included
        as a precondition function for any PGE's in production.
        """
        logger.info(f"Evaluating precondition {inspect.currentframe().f_code.co_name}")

        # get the working directory
        working_dir = get_working_dir()

        rc_params = {}

        # get s3_bucket param
        s3_bucket = self._pge_config.get(oc_const.SET_SAMPLE_PRODUCT_METADATA, {}).get(oc_const.S3_BUCKET)
        s3_key = self._pge_config.get(oc_const.SET_SAMPLE_PRODUCT_METADATA, {}).get(oc_const.S3_KEY)

        output_filepath = os.path.join(working_dir, os.path.basename(s3_key))

        download_object_from_s3(s3_bucket, s3_key, output_filepath, filetype="Sample product metadata")

        # read the sample product metadata and assign it to the local context
        with open(output_filepath, "r") as infile:
            product_metadata = json.load(infile)

        if not all(key in product_metadata for key in ["dataset", "metadata"]):
            raise RuntimeError(
                "Product metadata file does not contain expected keys (dataset/metadata)."
            )

        logger.info(f"Read product metadata for dataset {product_metadata['dataset']}")

        # assign the read product metadata into the local context, so it can be
        # used by downstream precondition functions
        self._context["product_metadata"] = product_metadata

        return rc_params
