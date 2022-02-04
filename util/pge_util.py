from datetime import datetime
import os
import json
import re
from typing import Dict, List

from commons.logger import logger

from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as oc_const


def simulate_run_pge(runconfig: Dict, pge_config: Dict, context: Dict, output_dir: str):
    output_base_name: str = runconfig['output_base_name']
    input_file_base_name_regexes: List[str] = runconfig['input_file_base_name_regexes']

    match = None
    for input_file_base_name_regex in input_file_base_name_regexes:
        pattern = re.compile(input_file_base_name_regex)
        match = pattern.match(get_input_dataset_id(context))
        if match:
            break

    output_types = pge_config.get(oc_const.OUTPUT_TYPES)
    for output_type in output_types.keys():
        product_shortname = match.groupdict()['product_shortname']
        if product_shortname == 'HLS.L30':
            sensor = 'Landsat8'
        elif product_shortname == 'HLS.S30':
            sensor = 'Sentinel2'
        else:
            raise

        base_name = output_base_name.format(
            sensor=sensor,
            tile_id=match.groupdict()['tile_id'],
            # compare input pattern with entries in settings.yaml, and output pattern with entries in pge_outputs.yaml
            datetime=datetime.strptime(match.groupdict()['acquisition_ts'], '%Y%jT%H%M%S').strftime('%Y%m%dT%H%M%S')
        )
        metadata = {}
        simulate_output(metadata, base_name, output_dir, output_types[output_type])


def get_input_dataset_id(context: Dict) -> str:
    params = context['job_specification']['params']
    for param in params:
        if param['name'] == 'input_dataset_id':
            return param['value']
    raise


def simulate_output(metadata: Dict, base_name: str, output_dir: str, extensions: str):
    logger.info('Simulating PGE output generation....')

    for extension in extensions:
        if extension.endswith('met'):
            met_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating met {met_file}')
            with open(met_file, 'w') as outfile:
                json.dump(metadata, outfile, indent=2)
        else:
            output_file = os.path.join(output_dir, f'{base_name}.{extension}')
            logger.info(f'Simulating output {output_file}')
            with open(output_file, 'wb') as f:
                f.write(os.urandom(1024))
