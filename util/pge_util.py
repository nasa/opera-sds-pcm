from datetime import datetime
import os
import json
import re
from typing import Dict, List

import boto3

from commons.logger import logger
from hysds.utils import get_disk_usage

from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as oc_const

DSWX_BAND_NAMES = ['WTR', 'BWTR', 'CONF', 'DIAG', 'WTR-1',
                   'WTR-2', 'LAND', 'SHAD', 'CLOUD', 'DEM']
"""
List of band identifiers for the multiple tif outputs produced by the DSWx-HLS
PGE.
"""


def download_object_from_s3(s3_bucket, s3_key, output_filepath, filetype="Ancillary"):
    """Helper function to download an arbitrary file from S3"""
    if not s3_bucket or not s3_key:
        raise RuntimeError(
            f"Incomplete S3 location for {filetype} file.\n"
            f"Values must be provided for both the '{oc_const.S3_BUCKET}' "
            f"and the '{oc_const.S3_KEY}' fields within the appropriate "
            f"section of the PGE config."
        )

    s3 = boto3.resource('s3')

    pge_metrics = {"download": [], "upload": []}

    loc_t1 = datetime.utcnow()

    try:
        logger.info(f'Downloading {filetype} file s3://{s3_bucket}/{s3_key} to {output_filepath}')
        s3.Object(s3_bucket, s3_key).download_file(output_filepath)
    except Exception as err:
        errmsg = f'Failed to download {filetype} file from S3, reason: {str(err)}'
        raise RuntimeError(errmsg)

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
    output_base_name: str = pge_config['output_base_name']
    input_file_base_name_regexes: List[str] = pge_config['input_file_base_name_regexes']

    for input_file_base_name_regex in input_file_base_name_regexes:
        pattern = re.compile(input_file_base_name_regex)
        match = pattern.match(get_input_dataset_id(context))
        if match:
            break
    else:
        raise RuntimeError(
            f"Could not match dataset ID '{get_input_dataset_id(context)}' to any "
            f"input file base name regex in the PGE configuration yaml file."
        )

    output_types = pge_config.get(oc_const.OUTPUT_TYPES)

    # Generate the output file base name specific to the PGE to be simulated
    base_name_map = {
        'L2_CSLC_S1': get_cslc_s1_simulated_output_basename,
        'L3_DSWx_HLS': get_dswx_hls_simulated_output_basename
    }

    try:
        output_basename_function = base_name_map[pge_name]
    except KeyError as err:
        raise RuntimeError(f'No basename function available for PGE {str(err)}')

    for output_type in output_types.keys():
        base_name = output_basename_function(match, output_base_name)
        metadata = {}
        simulate_output(pge_name, metadata, base_name, output_dir, output_types[output_type])


def get_input_dataset_id(context: Dict) -> str:
    params = context['job_specification']['params']
    for param in params:
        if param['name'] == 'input_dataset_id':
            return param['value']
    raise


def get_cslc_s1_simulated_output_basename(dataset_match, base_name_template):
    """Generates the output basename for simulated CSLC-S1 PGE runs"""

    base_name = base_name_template.format(
        burst_id='T64-135524-IW2',
        pol='VV',
        acquisition_ts=dataset_match.groupdict()['start_ts'],
        product_version='v0.1',
        creation_ts=dataset_match.groupdict()['stop_ts']
    )

    return base_name


def get_dswx_hls_simulated_output_basename(dataset_match, base_name_template):
    """Generates the output basename for simulated DSWx-HLS PGE runs"""
    product_shortname = dataset_match.groupdict()['product_shortname']
    if product_shortname == 'HLS.L30':
        sensor = 'L8'
    elif product_shortname == 'HLS.S30':
        sensor = 'S2A'
    else:
        raise

    base_name = base_name_template.format(
        tile_id=dataset_match.groupdict()['tile_id'],
        # compare input pattern with entries in settings.yaml, and output pattern with entries in pge_outputs.yaml
        acquisition_ts=datetime.strptime(dataset_match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S'),
        # make creation time a duplicate of the acquisition time for ease of testing
        creation_ts=datetime.strptime(dataset_match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S'),
        sensor=sensor,
        collection_version=dataset_match.groupdict()['collection_version']
    )

    return base_name


def get_input_dataset_tile_code(context: Dict) -> str:
    product_metadata = context["product_metadata"]["metadata"]
    tile_code = product_metadata["id"].split('.')[2]  # Example id: "HLS.L30.T54PVQ.2022001T005855.v2.0"

    return tile_code


def simulate_output(pge_name: str, metadata: Dict, base_name: str, output_dir: str, extensions: str):
    logger.info('Simulating PGE output generation....')

    for extension in extensions:
        if extension.endswith('met'):
            met_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating met {met_file}')
            with open(met_file, 'w') as outfile:
                json.dump(metadata, outfile, indent=2)
        elif extension.endswith('tiff') and pge_name == 'L3_DSWx_HLS':
            # Simulate the multiple output tif files created by this PGE

            for band_idx, band_name in enumerate(DSWX_BAND_NAMES, start=1):
                output_file = os.path.join(output_dir, f'{base_name}_B{band_idx:02}_{band_name}.{extension}')
                logger.info(f'Simulating output {output_file}')
                with open(output_file, 'wb') as f:
                    f.write(os.urandom(1024))
        elif extension.endswith('json'):
            output_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating JSON output {output_file}')
            with open(output_file, 'w') as outfile:
                json.dump({
                    "PGE_Version": "sim-pge-0.0.0",
                    "SAS_Version": "sim-sas-0.0.0"
                }, outfile)
        else:
            output_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating output {output_file}')
            with open(output_file, 'wb') as f:
                f.write(os.urandom(1024))


def get_product_metadata(job_json_dict: Dict) -> Dict:
    params = job_json_dict['job_specification']['params']
    for param in params:
        if param['name'] == 'product_metadata':
            return param['value']['metadata']
    raise