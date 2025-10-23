import argparse
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime
from functools import cache
from os.path import join, dirname, basename

import h5py
import yamale
import yaml
from lxml import etree

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DISP_PRODUCT_PATTERN = re.compile(r"(?P<id>(?P<project>OPERA)_(?P<level>L3)_(?P<product_type>DISP)-(?P<source>S1)_"
                                  r"(?P<mode>IW)_(?P<frame_id>F\d{5})_(?P<pol>VV|VH|HH|HV|VV\+VH|HH\+HV)_"
                                  r"(?P<ref_datetime>\d{8}T\d{6}Z)_(?P<sec_datetime>\d{8}T\d{6}Z)_"
                                  r"(?P<product_version>v\d+[.]\d+)_(?P<creation_ts>\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}Z))"
                                  r"[.](?P<ext>nc)")

DISP_ISO_PATTERN = re.compile(r"OPERA_L3_DISP-S1_IW_F\d{5}_(VV|VH|HH|HV|VV\+VH|HH\+HV)_\d{8}T\d{6}Z_\d{8}T\d{6}Z_"
                              r"v\d+[.]\d+_\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}Z[.]iso\.xml")

PROC_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
FILENAME_TIME_FORMAT = '%Y%m%dT%H%M%S'

SCHEMA_PATH = join(dirname(__file__), "update_runconfig_schema.yaml")
SCRIPT_PATH = '/disp-s1/scripts/recompute_perpendicular_baseline.py'


def _validate_config(config):
    schema = yamale.make_schema(SCHEMA_PATH)
    data = yamale.make_data(config)

    yamale.validate(schema, data, strict=True)


def _get_matching_file_in_dir(directory, pattern: re.Pattern):
    matching_file = None

    for file in os.listdir(directory):
        if pattern.match(file):
            matching_file = file
            break

    if matching_file is None:
        raise FileNotFoundError(f"Could not find {pattern} in {directory}")

    return join(directory, matching_file)


@cache
def _get_new_proc_time_and_version(new_product):
    with h5py.File(new_product, 'r') as f:
        new_datetime = f["/identification/processing_start_datetime"][()].decode(
            "utf-8"
        )

        new_version = f["/identification/product_version"][()].decode("utf-8")

    return new_datetime, new_version


def _set_by_xpath(element, ns_map, xpath, new_text):
    elements = element.xpath(xpath, namespaces=ns_map)
    
    if len(elements) == 0:
        raise ValueError(f"No such element {xpath}")
    elif len(elements) > 1:
        msg = f"More than one element {xpath}. Be more specific"
        
        for i, elem in enumerate(elements):
            msg += f'\n  {element.getroottree().getpath(elem)}: {elem.text}'
        raise ValueError(msg)
    
    elements[0].text = new_text
    
    
def _update_iso_xml(
        new_product,
        old_iso_path,
        new_iso_path,
        update_datetime=False,
        update_version=False,
        update_id=False,
):
    new_datetime, new_version = _get_new_proc_time_and_version(new_product)
        
    tree = etree.parse(old_iso_path)
    ns_map = tree.getroot().nsmap

    additional_attributes = tree.xpath('//gmd:contentInfo/gmd:MD_CoverageDescription/gmd:dimension/gmd:MD_Band/'
                                       'gmd:otherProperty/gco:Record/eos:AdditionalAttributes/eos:AdditionalAttribute', 
                                       namespaces=ns_map)

    additional_attributes_map = {
        aa.xpath(
            tree.getpath(aa) + '/eos:reference/eos:EOS_AdditionalAttributeDescription/eos:name/gco:CharacterString',
            namespaces=ns_map
        )[0].text: aa for aa in additional_attributes
    }

    if update_datetime:
        logger.info(f'Updating processing datetime in ISO XML to {new_datetime}')

        # Set new times
        _set_by_xpath(tree, ns_map, '//gmd:dateStamp/gco:DateTime', new_datetime)
        _set_by_xpath(tree, ns_map, '//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/'
                                    'gmd:date/gmd:CI_Date/gmd:date/gco:DateTime', new_datetime)
        _set_by_xpath(tree, ns_map, '//gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/'
                                    'gmd:processStep/gmi:LE_ProcessStep/gmd:dateTime/gco:DateTime', new_datetime)

        _set_by_xpath(
            tree, ns_map,
            f'{tree.getpath(additional_attributes_map["ProcessingDateTime"])}/eos:value/gco:CharacterString',
            new_datetime
        )
        _set_by_xpath(
            tree, ns_map,
            f'{tree.getpath(additional_attributes_map["ProcessingStartDatetime"])}/eos:value/gco:CharacterString',
            new_datetime
        )

    if update_version:
        logger.info(f'Updating product version in ISO XML to {new_version}')

        _set_by_xpath(tree, ns_map, '//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                                    'gmd:CI_Citation/gmd:edition/gco:CharacterString', new_version)
        _set_by_xpath(
            tree, ns_map,
            f'{tree.getpath(additional_attributes_map["ProductVersion"])}/eos:value/gco:CharacterString',
            new_datetime
        )

    if update_id:
        logger.info(f'Updating product ID in ISO XML to {new_iso_path}')

        match_dict = DISP_PRODUCT_PATTERN.match(basename(new_product)).groupdict()

        project = match_dict["project"]
        level = match_dict["level"]
        product_type = match_dict["product_type"]
        source = match_dict["source"]
        mode = match_dict["mode"]
        frame_id = match_dict["frame_id"]
        pol = match_dict["pol"]
        ref_datetime = match_dict["ref_datetime"]
        sec_datetime = match_dict["sec_datetime"]
        version = f'v{new_version}'
        creation_ts = datetime.strptime(new_datetime, PROC_TIME_FORMAT).strftime(FILENAME_TIME_FORMAT) + 'Z'

        new_id = (f'{project}_{level}_{product_type}-{source}_{mode}_{frame_id}_{pol}_{ref_datetime}_{sec_datetime}_'
                  f'{version}_{creation_ts}')

        file_id = f'{project}_{level}_{product_type}-{source}_{mode}_{frame_id}_{version}_{creation_ts}'

        _set_by_xpath(tree, ns_map, '//gmd:fileIdentifier/gco:CharacterString', file_id)
        _set_by_xpath(tree, ns_map, '//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                                    'gmd:CI_Citation/gmd:title/gmx:FileName', new_id)

        identifiers = tree.xpath('//gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/'
                                 'gmd:identifier', namespaces=ns_map)

        for identifier in identifiers:
            identifier_description = identifier.xpath(
                tree.getpath(identifier) + '/gmd:MD_Identifier/gmd:description/gco:CharacterString', namespaces=ns_map
            )[0].text

            if identifier_description == 'ProducerGranuleId':
                identifier.xpath(
                    tree.getpath(identifier) + '/gmd:MD_Identifier/gmd:code/gco:CharacterString',
                    namespaces=ns_map
                )[0].text = new_id

                break

        new_product_filename = new_id + '.nc'
        new_iso_filename = new_id + '.iso.xml'

        shutil.move(new_product, join(dirname(new_product), new_product_filename))
        logger.info(f'Renaming {basename(new_product)} to {new_product_filename}')
        new_iso_path = join(dirname(new_iso_path), new_iso_filename)

    logger.info(f'Writing updated ISO XML to {new_iso_path}')

    with open(new_iso_path, 'w') as f:
        f.write(
            etree.tostring(tree, pretty_print=True, xml_declaration=True, encoding='UTF-8').decode()
        )
        f.write('\n')


def main(args):
    _validate_config(args.file)

    with open(args.file, "r") as f:
        config = yaml.safe_load(f)

    input_product_path = config['RunConfig']['input_product']
    update_params = config['RunConfig']['product_update_params']

    if update_params is None:
        update_params = {}

    subsample = update_params.get('subsample')
    new_version = update_params.get('new_version')
    update_proc_time = update_params.get('update_processed_time', False)
    update_product_id = update_params.get('update_product_id', False)

    output_dir = config['RunConfig']['product_path_group']['product_path']

    input_disp_product = _get_matching_file_in_dir(input_product_path, DISP_PRODUCT_PATTERN)
    input_iso_xml = _get_matching_file_in_dir(input_product_path, DISP_ISO_PATTERN)

    output_path = join(output_dir, basename(input_disp_product))

    cmd = [
        'bash', '-c',
        f'source /usr/local/bin/_activate_current_env.sh; '
        f'micromamba activate base; '
        f'python {SCRIPT_PATH} --input-file {input_disp_product} '
        f'--output-file {output_path}'
    ]

    if subsample is not None:
        cmd[-1] += f' --subsample {subsample}'

    if new_version is not None:
        cmd[-1] += f' --new-version {new_version}'

    if not update_proc_time:
        cmd[-1] += ' --no-update-processing-time'

    logger.info(f'Running command: {cmd}')

    update_result = subprocess.run(cmd)

    logger.info(f'Command exited code: {update_result.returncode}')

    if update_result.returncode != 0:
        raise subprocess.CalledProcessError(update_result.returncode, cmd)

    _update_iso_xml(
        output_path,
        input_iso_xml,
        join(output_dir, basename(input_iso_xml)),
        update_datetime=update_proc_time,
        update_version=new_version is not None,
        update_id=update_product_id
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--file',
        required=True,
        help='YAML config file'
    )

    cli_args = parser.parse_args()

    main(cli_args)
