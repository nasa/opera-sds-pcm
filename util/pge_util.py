
"""
===========
pge_util.py
===========

Contains utility functions for executing a PGE, including simulation mode.

"""

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from typing import Dict, List

import backoff
import boto3
from boto3.s3.transfer import TransferConfig, MB

import hysds.utils
from opera_commons.logger import logger
from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as oc_const

DSWX_HLS_BAND_NAMES = ['WTR', 'BWTR', 'CONF', 'DIAG', 'WTR-1',
                       'WTR-2', 'LAND', 'SHAD', 'CLOUD', 'DEM']
"""
List of band identifiers for the multiple tif outputs produced by the DSWx-HLS
PGE.
"""

DSWX_S1_BAND_NAMES = ['WTR', 'BWTR', 'CONF', 'DIAG']
"""
List of band identifiers for the multiple tif outputs produced by the DSWx-S1
PGE.
"""

DIST_S1_BAND_NAMES = ['GEN-DIST-STATUS-ACQ', 'GEN-DIST-STATUS', 'GEN-METRIC', 'GEN-DIST-CONF', 'GEN-DIST-COUNT',
                      'GEN-DIST-DATE', 'GEN-DIST-DUR', 'GEN-DIST-LAST-DATE', 'GEN-DIST-PERC', 'GEN-METRIC-MAX']
"""
List of band identifiers for the multiple tif outputs produced by the DIST-S1
PGE.
"""

DISP_S1_STATIC_BAND_NAMES = ['dem_warped_utm', 'layover_shadow_mask', 'los_enu']
"""
List of band identifiers for the multiple tif outputs produced by the DISP-S1-STATIC
PGE.
"""

CSLC_BURST_IDS = ['T064-135518-IW1', 'T064-135518-IW2', 'T064-135518-IW3',
                  'T064-135519-IW1', 'T064-135519-IW2', 'T064-135519-IW3',
                  'T064-135520-IW1', 'T064-135520-IW2', 'T064-135520-IW3']
"""List of sample burst ID's to simulate CSLC-S1 multi-product output"""

RTC_BURST_IDS = ['T069-147170-IW1', 'T069-147170-IW3', 'T069-147171-IW1',
                 'T069-147171-IW2', 'T069-147171-IW3', 'T069-147172-IW1',
                 'T069-147172-IW2', 'T069-147172-IW3', 'T069-147173-IW1']
"""List of sample burst ID's to simulate RTC-S1 multi-product output"""

CCSLC_BURST_IDS = [
'T041-086865-IW1', 'T041-086865-IW2', 'T041-086865-IW3', 'T041-086866-IW1',
'T041-086866-IW2', 'T041-086866-IW3', 'T041-086867-IW1', 'T041-086867-IW2',
'T041-086867-IW3', 'T041-086868-IW1', 'T041-086868-IW2', 'T041-086868-IW3',
'T041-086869-IW1', 'T041-086869-IW2', 'T041-086869-IW3', 'T041-086870-IW1',
'T041-086870-IW2', 'T041-086870-IW3', 'T041-086871-IW1', 'T041-086871-IW2',
'T041-086871-IW3', 'T041-086872-IW1', 'T041-086872-IW2', 'T041-086872-IW3',
'T041-086873-IW1', 'T041-086873-IW2', 'T041-086873-IW3'
]
"""List of sample burst ID's to simulate multiple Compressed CSLC outputs"""

SIMULATED_MGRS_TILES = ['T18MVA', 'T18MVT', 'T18MVU', 'T18MVV', 'T18MWA', 'T18MWT',
                        'T18MWU', 'T18MWV', 'T18MXA', 'T18MXT', 'T18MXU', 'T18MXV']
"""List of sample MGRS tile ID's to simulate DSWx-S1/NI and DIST-S1 multi-product output"""

S3_CONFIG = TransferConfig(multipart_chunksize=128*MB)
"""Transfer configuration for S3 downloads used to override multipart chunksize to 128MB """

s3 = boto3.resource('s3')

def get_disk_usage(path, follow_symlinks=True):
    """
    Return disk usage size in bytes.

    This function was copied from hysds.util to remove an import dependency that
    was preventing tests from running.
    """

    opts = "-sbL" if follow_symlinks else "-sb"
    size = 0
    try:
        size = int(subprocess.check_output(["du", opts, path]).split()[0])
    except:
        pass
    return size


def get_input_hls_dataset_tile_code(context: Dict) -> str:
    product_metadata = context["product_metadata"]["metadata"]
    tile_code = product_metadata["id"].split('.')[2]  # Example id: "HLS.L30.T54PVQ.2022001T005855.v2.0"

    return tile_code


def get_product_metadata(job_json_dict: Dict) -> Dict:
    params = job_json_dict['job_specification']['params']
    for param in params:
        if param['name'] == 'product_metadata':
            metadata = param['value']

            if isinstance(metadata, dict):
                metadata = metadata['metadata']
            elif isinstance(metadata, str):
                # TODO: kludge to support reading canned metadata from a file stored on S3,
                #       remove when appropriate
                if metadata.startswith("s3://"):
                    bucket, key = metadata.split('/', 2)[-1].split('/', 1)
                    obj = s3.Object(bucket, key)
                    metadata = json.loads(obj.get()['Body'].read())['metadata']
                else:
                    metadata = json.loads(param['value'])['metadata']
            else:
                raise ValueError(f'Unknown product_metadata format: {metadata}')

            return metadata

    raise


PRODUCTION_TIME = None
def get_time_for_filename():
    """
    Creates o a time-tag string suitable for use with PGE output filenames.
    The time-tag string is cached after the first call to this function.
    """
    global PRODUCTION_TIME

    if PRODUCTION_TIME is None:
        PRODUCTION_TIME = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')

    return PRODUCTION_TIME


def check_aws_connection(bucket, key):
    """
    Check connection to the provided S3 bucket by performing a test read
    on the provided bucket/key location.

    Parameters
    ----------
    bucket : str
        Name of the S3 bucket to use with the connection test.
    key : str, optional
        S3 key path to append to the bucket name.

    Raises
    ------
    RuntimeError
        If not connection can be established.

    """
    obj = s3.Object(bucket, key)

    try:
        logger.info(f'Attempting test read of s3://{obj.bucket_name}/{obj.key}')
        obj.get()['Body'].read()
        logger.info('Connection test successful.')
    except Exception:
        errmsg = (f'No access to the {bucket} S3 bucket. '
                  f'Check your AWS credentials and re-run the code.')
        raise RuntimeError(errmsg)


def download_object_from_s3(s3_bucket, s3_key, output_filepath, filetype="Ancillary"):
    """Helper function to download an arbitrary file from S3"""
    if not s3_bucket or not s3_key:
        raise RuntimeError(
            f"Incomplete S3 location for {filetype} file.\n"
            f"Values must be provided for both the '{oc_const.S3_BUCKET}' "
            f"and the '{oc_const.S3_KEY}' fields within the appropriate "
            f"section of the PGE config."
        )

    loc_t1 = datetime.now(timezone.utc)

    try:
        logger.info(f'Downloading {filetype} file s3://{s3_bucket}/{s3_key} to {output_filepath}')
        s3.Object(s3_bucket, s3_key).download_file(output_filepath, Config=S3_CONFIG)
    except Exception as err:
        errmsg = f'Failed to download {filetype} file from S3, reason: {str(err)}'
        raise RuntimeError(errmsg)

    loc_t2 = datetime.now(timezone.utc)
    loc_dur = (loc_t2 - loc_t1).total_seconds()
    path_disk_usage = get_disk_usage(output_filepath)

    pge_metrics = {
        "download" : [
            {
                "url": output_filepath,
                "path": output_filepath,
                "disk_usage": path_disk_usage,
                "time_start": loc_t1.isoformat() + "Z",
                "time_end": loc_t2.isoformat() + "Z",
                "duration": loc_dur,
                "transfer_rate": path_disk_usage / loc_dur,
            }
        ],
        "upload": []
    }

    return pge_metrics


@backoff.on_exception(backoff.expo, exception=Exception, max_tries=10)
def download_file_with_hysds(url, path, cache=False):
    """Helper function to download a file via the Hysds download utility (osaka)"""
    logger.info(f'Downloading file {url} to {path} via Hysds')

    loc_t1 = datetime.now(timezone.utc)
    hysds.utils.download_file(url, path, cache)
    loc_t2 = datetime.now(timezone.utc)

    loc_dur = (loc_t2 - loc_t1).total_seconds()
    path_disk_usage = get_disk_usage(path)

    pge_metrics = {
        "download": [
            {
                "url": url,
                "path": path,
                "disk_usage": path_disk_usage,
                "time_start": loc_t1.isoformat() + "Z",
                "time_end": loc_t2.isoformat() + "Z",
                "duration": loc_dur,
                "transfer_rate": path_disk_usage / loc_dur,
            }
        ],
        "upload": []
    }

    return pge_metrics


def write_pge_metrics(metrics_path, pge_metrics):
    # Merge any existing metrics with the metrics about to be written
    if os.path.exists(metrics_path):
        with open(metrics_path, "r") as infile:
            old_pge_metrics = json.load(infile)

        pge_metrics["download"].extend(old_pge_metrics["download"])
        pge_metrics["upload"].extend(old_pge_metrics["upload"])

    # Commit the new metrics to disk
    with open(metrics_path, "w") as f:
        json.dump(pge_metrics, f, indent=2)


def simulate_run_pge(runconfig: Dict, pge_config: Dict, context: Dict, output_dir: str):
    pge_name: str = pge_config['pge_name']
    input_file_base_name_regexes: List[str] = pge_config['input_file_base_name_regexes']

    input_dataset_id = get_input_dataset_id(context)

    # For PGE's that are triggered off of multiple input datasets (such as
    # DSWx-S1 and DISP-S1) we substitute a single sample dataset ID to pattern
    # match against for the sake of generating dummy output files
    if 'sample_input_dataset_id' in pge_config:
        input_dataset_id = pge_config['sample_input_dataset_id']

    for input_file_base_name_regex in input_file_base_name_regexes:
        pattern = re.compile(input_file_base_name_regex)
        match = pattern.match(input_dataset_id)
        if match:
            break
    else:
        raise RuntimeError(
            f"Could not match dataset ID '{input_dataset_id}' to any "
            f"input file base name regex in the PGE configuration yaml file."
        )

    logger.info('Simulating PGE output generation....')

    output_types = pge_config.get(oc_const.OUTPUT_TYPES)

    for output_type in output_types.keys():
        simulate_output(pge_name, pge_config, match, output_dir, output_types[output_type])


def get_input_dataset_id(context: Dict) -> str:
    params = context['job_specification']['params']
    for param in params:
        if param['name'] == 'input_dataset_id':
            return param['value']
    else:
        return ""


def get_cslc_s1_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates an output filename for simulated CSLC-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    creation_time = get_time_for_filename()

    if extension.endswith('h5') or extension.endswith('iso.xml'):
        for burst_id in CSLC_BURST_IDS:
            base_name = base_name_template.format(
                burst_id=burst_id,
                acquisition_ts=dataset_match.groupdict()['start_ts'],
                creation_ts=creation_time,
                sensor=dataset_match.groupdict()['mission_id'],
                pol='VV',
                product_version='v0.1'
            )
            output_filenames.append(f'{base_name}.{extension}')
    elif extension.endswith('png'):
        for burst_id in CSLC_BURST_IDS:
            base_name = base_name_template.format(
                burst_id=burst_id,
                acquisition_ts=dataset_match.groupdict()['start_ts'],
                creation_ts=creation_time,
                sensor=dataset_match.groupdict()['mission_id'],
                pol='VV',
                product_version='v0.1'
            )
            output_filenames.append(f'{base_name}_BROWSE.{extension}')
    else:
        base_name = ancillary_name_template.format(
            creation_ts=creation_time,
            sensor=dataset_match.groupdict()['mission_id'],
            pol='VV',
            product_version='v0.1',
        )

        output_filenames.append(f'{base_name}.{extension}')

    return output_filenames

def get_cslc_s1_static_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates an output filename for simulated CSLC-S1-STATIC PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    creation_time = get_time_for_filename()

    if extension.endswith('h5') or extension.endswith('iso.xml'):
        for burst_id in CSLC_BURST_IDS:
            static_base_name = base_name_template.format(
                burst_id=burst_id,
                validity_ts='20140403',
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )
            output_filenames.append(f'{static_base_name}.{extension}')
    else:
        base_name = ancillary_name_template.format(
            creation_ts=creation_time,
            sensor=dataset_match.groupdict()['mission_id'],
            pol='VV',
            product_version='v0.1',
        )

        output_filenames.append(f'{base_name}.{extension}')

    return output_filenames


def get_rtc_s1_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates an output filename for simulated RTC-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    creation_time = get_time_for_filename()

    # Primary output image product pattern, includes burst ID, acquisition time
    # and polarization values/static layer name
    if extension.endswith('tiff') or extension.endswith('tif'):
        for burst_id in RTC_BURST_IDS:
            base_name = base_name_template.format(
                burst_id=burst_id,
                acquisition_ts=dataset_match.groupdict()['start_ts'],
                creation_ts=creation_time,
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )

            output_filenames.append(f'{base_name}_VV.{extension}')
            output_filenames.append(f'{base_name}_VH.{extension}')
            output_filenames.append(f'{base_name}_mask.{extension}')
    # Primary metadata product, like image product but no polarization field
    elif extension.endswith('h5') or extension.endswith('iso.xml'):
        for burst_id in RTC_BURST_IDS:
            base_name = base_name_template.format(
                burst_id=burst_id,
                acquisition_ts=dataset_match.groupdict()['start_ts'],
                creation_ts=creation_time,
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )
            output_filenames.append(f'{base_name}.{extension}')
    # PNG browse product, like image product but appended with "_BROWSE"
    elif extension.endswith('png'):
        for burst_id in RTC_BURST_IDS:
            base_name = base_name_template.format(
                burst_id=burst_id,
                acquisition_ts=dataset_match.groupdict()['start_ts'],
                creation_ts=creation_time,
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )
            output_filenames.append(f'{base_name}_BROWSE.{extension}')
    # Ancillary output product pattern, no burst ID, acquisition time or polarization
    else:
        base_name = ancillary_name_template.format(
            creation_ts=creation_time,
            sensor=dataset_match.groupdict()['mission_id'],
            product_version='v0.1',
        )

        output_filenames.append(f'{base_name}.{extension}')

    return output_filenames


def get_rtc_s1_static_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates an output filename for simulated RTC-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    creation_time = get_time_for_filename()

    # Primary output image product pattern, includes burst ID, acquisition time
    # and polarization values/static layer name
    if extension.endswith('tiff') or extension.endswith('tif'):
        for burst_id in RTC_BURST_IDS:
            static_base_name = base_name_template.format(
                burst_id=burst_id,
                validity_ts='20140403',
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )

            output_filenames.append(f'{static_base_name}_incidence_angle.{extension}')
            output_filenames.append(f'{static_base_name}_mask.{extension}')
            output_filenames.append(f'{static_base_name}_local_incidence_angle.{extension}')
            output_filenames.append(f'{static_base_name}_number_of_looks.{extension}')
            output_filenames.append(f'{static_base_name}_rtc_anf_gamma0_to_beta0.{extension}')
            output_filenames.append(f'{static_base_name}_rtc_anf_gamma0_to_sigma0.{extension}')
    # Primary metadata product, like image product but no polarization field
    elif extension.endswith('h5') or extension.endswith('iso.xml'):
        for burst_id in RTC_BURST_IDS:
            static_base_name = base_name_template.format(
                burst_id=burst_id,
                validity_ts='20140403',
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )
            output_filenames.append(f'{static_base_name}.{extension}')
    # PNG browse product, like image product but appended with "_BROWSE"
    elif extension.endswith('png'):
        for burst_id in RTC_BURST_IDS:
            static_base_name = base_name_template.format(
                burst_id=burst_id,
                validity_ts='20140403',
                sensor=dataset_match.groupdict()['mission_id'],
                product_version='v0.1',
            )
            output_filenames.append(f'{static_base_name}_BROWSE.{extension}')
    # Ancillary output product pattern, no burst ID, acquisition time or polarization
    else:
        base_name = ancillary_name_template.format(
            creation_ts=creation_time,
            sensor=dataset_match.groupdict()['mission_id'],
            product_version='v0.1',
        )

        output_filenames.append(f'{base_name}.{extension}')

    return output_filenames


def get_dswx_hls_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DSWx-HLS PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']

    product_shortname = dataset_match.groupdict()['product_shortname']
    if product_shortname == 'HLS.L30':
        sensor = 'L8'
    elif product_shortname == 'HLS.S30':
        sensor = 'S2A'
    else:
        raise RuntimeError(f'Could not determine HLS sensor from product shortname "{product_shortname}"')

    acq_time = datetime.strptime(
        dataset_match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)

    creation_time = get_time_for_filename()

    base_name = base_name_template.format(
        tile_id=dataset_match.groupdict()['tile_id'],
        acquisition_ts=acq_time,
        creation_ts=creation_time,
        sensor=sensor,
        product_version=dataset_match.groupdict()['collection_version']
    )

    # Simulate the multiple output tif files created by this PGE
    if extension.endswith('tiff') or extension.endswith('tif'):
        for band_idx, band_name in enumerate(DSWX_HLS_BAND_NAMES, start=1):
            output_filenames.append(f'{base_name}_B{band_idx:02}_{band_name}.tif')
    elif extension.endswith('png'):
        output_filenames.append(f'{base_name}_BROWSE.png')
        output_filenames.append(f'{base_name}_BROWSE.tif')
    else:
        output_filenames.append(f'{base_name}.{extension}')

    return output_filenames

def get_dswx_s1_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DSWx-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']

    acq_time = dataset_match.groupdict()['acquisition_ts']
    sensor = dataset_match.groupdict()['sensor']

    creation_time = get_time_for_filename()

    for tile_id in SIMULATED_MGRS_TILES:
        base_name = base_name_template.format(
            tile_id=tile_id,
            acquisition_ts=acq_time,
            creation_ts=creation_time,
            sensor=sensor,
            spacing='30',
            product_version=dataset_match.groupdict()['product_version']
        )

        # Simulate the multiple output tif files created by this PGE
        if extension.endswith('tiff') or extension.endswith('tif'):
            for band_idx, band_name in enumerate(DSWX_S1_BAND_NAMES, start=1):
                output_filenames.append(f'{base_name}_B{band_idx:02}_{band_name}.tif')

            output_filenames.append(f'{base_name}_BROWSE.tif')
        elif extension.endswith('png'):
            output_filenames.append(f'{base_name}_BROWSE.png')
        elif extension.endswith('iso.xml'):
            output_filenames.append(f'{base_name}.iso.xml')
        # Ancillary output product pattern, no tile ID or acquisition time
        else:
            base_name = ancillary_name_template.format(
                creation_ts=creation_time,
                sensor=sensor,
                spacing='30',
                product_version=dataset_match.groupdict()['product_version']
            )

            ancillary_file_name = f'{base_name}.{extension}'

            # Should only be one of these files per simulated run
            if ancillary_file_name not in output_filenames:
                output_filenames.append(ancillary_file_name)

    return output_filenames

def get_dswx_ni_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DSWx-NI PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']

    acq_time = get_time_for_filename()
    creation_time = get_time_for_filename()

    for tile_id in SIMULATED_MGRS_TILES:
        base_name = base_name_template.format(
            tile_id=tile_id,
            acquisition_ts=acq_time,
            creation_ts=creation_time,
            sensor='LSAR',
            spacing='30',
            product_version='0.1'
        )

        # Simulate the multiple output tif files created by this PGE
        if extension.endswith('tiff') or extension.endswith('tif'):
            for band_idx, band_name in enumerate(DSWX_S1_BAND_NAMES, start=1):
                output_filenames.append(f'{base_name}_B{band_idx:02}_{band_name}.tif')

            output_filenames.append(f'{base_name}_BROWSE.tif')
        elif extension.endswith('png'):
            output_filenames.append(f'{base_name}_BROWSE.png')
        elif extension.endswith('iso.xml'):
            output_filenames.append(f'{base_name}.iso.xml')
        # Ancillary output product pattern, no tile ID or acquisition time
        else:
            base_name = ancillary_name_template.format(
                creation_ts=creation_time,
                sensor='LSAR',
                spacing='30',
                product_version='0.1'
            )

            ancillary_file_name = f'{base_name}.{extension}'

            # Should only be one of these files per simulated run
            if ancillary_file_name not in output_filenames:
                output_filenames.append(ancillary_file_name)

    return output_filenames


def get_disp_s1_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DISP-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    compressed_cslc_template: str = pge_config['compressed_cslc_name']

    creation_time = get_time_for_filename()

    if extension.endswith('nc') or extension.endswith('iso.xml'):
        base_name = base_name_template.format(
            frame_id="F10859",
            pol="VV",
            ref_datetime="20160705T000000",
            sec_datetime="20160822T000000",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        output_filenames.append(f'{base_name}.{extension}')
    elif extension.endswith('png'):
        base_name = base_name_template.format(
            frame_id="F10859",
            pol="VV",
            ref_datetime="20160705T000000",
            sec_datetime="20160822T000000",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        output_filenames.append(f'{base_name}_BROWSE.{extension}')
    elif extension.endswith('h5'):
        for burst_id in CCSLC_BURST_IDS:
            base_name = compressed_cslc_template.format(
                disp_frame_id="F10859",
                burst_id=burst_id,
                ref_date="20160705",
                first_date="20160822",
                last_date="20160915",
                creation_ts=creation_time,
                pol="VV",
                product_version=dataset_match.groupdict()['product_version']
            )

            output_filenames.append(f'{base_name}.{extension}')
    else:
        base_name = ancillary_name_template.format(
            frame_id="F10859",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        ancillary_file_name = f'{base_name}.{extension}'

        # Should only be one of these files per simulated run
        if ancillary_file_name not in output_filenames:
            output_filenames.append(ancillary_file_name)

    return output_filenames


def get_disp_ni_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DISP-NI PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    compressed_gslc_template: str = pge_config['compressed_gslc_name']

    creation_time = get_time_for_filename()

    if extension.endswith('nc') or extension.endswith('iso.xml'):
        base_name = base_name_template.format(
            track="001",
            direction="A",
            frame_id="150",
            mode="40",
            pol="HH",
            ref_datetime="20060630T061920",
            sec_datetime="20060815T061952",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        output_filenames.append(f'{base_name}.{extension}')
    elif extension.endswith('png'):
        base_name = base_name_template.format(
            track="001",
            direction="A",
            frame_id="150",
            mode="40",
            pol="HH",
            ref_datetime="20060630T061920",
            sec_datetime="20060815T061952",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        output_filenames.append(f'{base_name}_BROWSE.{extension}')
    elif extension.endswith('h5'):
        base_name = compressed_gslc_template.format(
            ref_date="20060630",
            first_date="20060630",
            last_date="20071118",
        )

        output_filenames.append(f'{base_name}.{extension}')
    else:
        base_name = ancillary_name_template.format(
            frame_id="150",
            pol="HH",
            product_version=dataset_match.groupdict()['product_version'],
            creation_ts=creation_time
        )

        ancillary_file_name = f'{base_name}.{extension}'

        # Should only be one of these files per simulated run
        if ancillary_file_name not in output_filenames:
            output_filenames.append(ancillary_file_name)

    return output_filenames


def get_disp_s1_static_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DIST-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']
    creation_time = get_time_for_filename()

    base_name = base_name_template.format(
        frame_id="F10859",
        validity_ts='20250409',
        sensor=dataset_match.groupdict()['sensor'],
        product_version='v0.1'
    )

    ancillary_base_name = ancillary_name_template.format(
        frame_id="F10859",
        product_version='v0.1',
        creation_ts=creation_time
    )

    if extension.endswith('tiff') or extension.endswith('tif'):
        for band in DISP_S1_STATIC_BAND_NAMES:
            output_filenames.append(f'{base_name}_{band}.{extension}')
    elif extension.endswith('png'):
        output_filenames.append(f'{base_name}_BROWSE.{extension}')
    elif extension.endswith('iso.xml'):
        output_filenames.append(f'{base_name}.iso.xml')
    else:
        output_filenames.append(f'{ancillary_base_name}.{extension}')

    return output_filenames

def get_dist_s1_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DIST-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']

    acq_time = get_time_for_filename()
    creation_time = get_time_for_filename()

    for tile_id in SIMULATED_MGRS_TILES:
        base_name = base_name_template.format(
            tile_id=tile_id,
            acquisition_ts=acq_time,
            creation_ts=creation_time,
            sensor='S1',
            spacing='30',
            product_version='0.1',
        )

        if extension.endswith('tiff') or extension.endswith('tif'):
            for band_name in DIST_S1_BAND_NAMES:
                output_filenames.append(f'{base_name}_{band_name}.tif')

            # TODO: Current release doesn't make GeoTIFF browse images
            # output_filenames.append(f'{base_name}_BROWSE.tif')
        elif extension.endswith('png'):
            output_filenames.append(f'{base_name}.png')
        elif extension.endswith('iso.xml'):
            output_filenames.append(f'{base_name}.iso.xml')
        # Ancillary output product pattern, no tile ID or acquisition time
        else:
            base_name = ancillary_name_template.format(
                creation_ts=creation_time,
                sensor='S1',
                spacing='30',
                product_version='0.1'
            )

            ancillary_file_name = f'{base_name}.{extension}'

            # Should only be one of these files per simulated run
            if ancillary_file_name not in output_filenames:
                output_filenames.append(ancillary_file_name)

    return output_filenames


def get_tropo_simulated_output_filenames(dataset_match, pge_config, extension):
    """Generates the output basename for simulated DISP-S1 PGE runs"""
    output_filenames = []

    base_name_template: str = pge_config['output_base_name']
    ancillary_name_template: str = pge_config['ancillary_base_name']

    acq_time = get_time_for_filename()
    creation_time = get_time_for_filename()
    if extension.endswith('nc') or extension.endswith('png') or extension.endswith('iso.xml'):
        base_name = base_name_template.format(
            acquisition_ts=acq_time,
            creation_ts=creation_time,
            model='HRES',
            spacing='0.1',
            product_version='0.1',
        )
    # ancillary has a different output format
    else:
        base_name = ancillary_name_template.format(
            creation_ts=creation_time,
        )

    # Simulate the multiple output tif files created by this PGE
    if extension.endswith('nc'):
        output_filenames.append(f'{base_name}.nc')
    elif extension.endswith('png'):
        output_filenames.append(f'{base_name}.png')
    elif extension.endswith('iso.xml'):
        output_filenames.append(f'{base_name}.iso.xml')
    elif extension.endswith('log'):
        output_filenames.append(f'{base_name}.log')
    elif extension.endswith('qa.log'):
        output_filenames.append(f'{base_name}.qa.log')
    elif extension.endswith('catalog.json'):
        output_filenames.append(f'{base_name}.catalog.json')

    return output_filenames

def simulate_output(pge_name: str, pge_config: dict, dataset_match: re.Match, output_dir: str, extensions: str):
    for extension in extensions:
        # Generate the output file name(s) specific to the PGE to be simulated
        base_name_map = {
            'L2_CSLC_S1': get_cslc_s1_simulated_output_filenames,
            'L2_CSLC_S1_STATIC': get_cslc_s1_static_simulated_output_filenames,
            'L2_RTC_S1': get_rtc_s1_simulated_output_filenames,
            'L2_RTC_S1_STATIC': get_rtc_s1_static_simulated_output_filenames,
            'L3_DSWx_HLS': get_dswx_hls_simulated_output_filenames,
            'L3_DSWx_S1': get_dswx_s1_simulated_output_filenames,
            'L3_DISP_S1': get_disp_s1_simulated_output_filenames,
            'L3_DISP_S1_STATIC': get_disp_s1_static_simulated_output_filenames,
            'L3_DSWx_NI': get_dswx_ni_simulated_output_filenames,
            'L3_DIST_S1': get_dist_s1_simulated_output_filenames,
            'L4_TROPO': get_tropo_simulated_output_filenames,
            'L3_DISP_NI': get_disp_ni_simulated_output_filenames,
        }

        try:
            output_filename_function = base_name_map[pge_name]
        except KeyError as err:
            raise RuntimeError(f'No output filename function available for PGE {str(err)}')

        output_filenames = output_filename_function(dataset_match, pge_config, extension)

        for output_filename in output_filenames:
            output_file = os.path.join(output_dir, output_filename)
            logger.info(f'Simulating output {output_file}')

            # Create a realistic catalog.json file that product2dataset can parse
            if extension.endswith('catalog.json'):
                with open(output_file, 'w') as outfile:
                    json.dump(
                        {
                            "PGE_Version": "sim-pge-0.0.0",
                            "SAS_Version": "sim-sas-0.0.0"
                        },
                        outfile
                    )
            # Create a dummy file containing random data
            else:
                with open(output_file, 'wb') as outfile:
                    outfile.write(os.urandom(1024))
