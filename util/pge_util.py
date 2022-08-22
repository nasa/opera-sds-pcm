from datetime import datetime
import os
import json
import re
from typing import Dict, List

from commons.logger import logger

from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as oc_const

DSWX_BAND_NAMES = ['WTR', 'BWTR', 'CONF', 'DIAG', 'WTR-1',
                   'WTR-2', 'LAND', 'SHAD', 'CLOUD', 'DEM']
"""
List of band identifiers for the multiple tif outputs produced by the DSWx-HLS
PGE.
"""


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

    for output_type in output_types.keys():
        product_shortname = match.groupdict()['product_shortname']
        if product_shortname == 'HLS.L30':
            sensor = 'L8'
        elif product_shortname == 'HLS.S30':
            sensor = 'S2A'
        else:
            raise

        base_name = output_base_name.format(
            tile_id=match.groupdict()['tile_id'],
            # compare input pattern with entries in settings.yaml, and output pattern with entries in pge_outputs.yaml
            acquisition_ts=datetime.strptime(match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S'),
            # make creation time a duplicate of the acquisition time for ease of testing
            creation_ts=datetime.strptime(match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S'),
            sensor=sensor,
            collection_version=match.groupdict()['collection_version']
        )
        metadata = {}
        simulate_output(pge_name, metadata, base_name, output_dir, output_types[output_type])


def get_input_dataset_id(context: Dict) -> str:
    params = context['job_specification']['params']
    for param in params:
        if param['name'] == 'input_dataset_id':
            return param['value']
    raise


def get_input_dataset_tile_code(context: Dict) -> str:
    tile_code = None
    product_metadata = context["product_metadata"]["metadata"]

    for band_or_qa, product_path in product_metadata.items():
        if band_or_qa != '@timestamp':
            product_filename = product_path.split('/')[-1]
            tile_code = product_filename.split('.')[2]
            break

    return tile_code


def simulate_output(pge_name: str, metadata: Dict, base_name: str, output_dir: str, extensions: str):
    logger.info('Simulating PGE output generation....')

    for extension in extensions:
        if extension.endswith('met'):
            met_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating met {met_file}')
            with open(met_file, 'w') as outfile:
                json.dump(metadata, outfile, indent=2)
        elif extension.endswith('tiff') and pge_name == 'L3_HLS':
            # Simulate the multiple output tif files created by this PGE

            for band_idx, band_name in enumerate(DSWX_BAND_NAMES, start=1):
                output_file = os.path.join(output_dir, f'{base_name}_B{band_idx:02}_{band_name}.{extension}')
                logger.info(f'Simulating output {output_file}')
                with open(output_file, 'wb') as f:
                    f.write(os.urandom(1024))
        else:
            output_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating output {output_file}')
            with open(output_file, 'wb') as f:
                f.write(os.urandom(1024))
