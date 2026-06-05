#!/usr/bin/env python

import json
import re
from functools import cache
from urllib.parse import urlparse

import backoff
import boto3
import requests

from data_subscriber import es_conn_util
from opera_commons.logger import get_logger
from util.backoff_util import backoff_logger, fatal_code
from util.common_util import backoff_wrapper
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()
s3 = boto3.client('s3')


def get_product(es_conn, product_id):
    query = {
        "query": {
            "bool": {
                "must": [{"term": {"_id": product_id}}]
            }
        }
    }

    logger.info(json.dumps(query, indent=2))

    result = backoff_wrapper(es_conn.search, body=query, index='grq')

    assert result['hits']['total']['value'] == 1

    doc = result['hits']['hits'][0]

    return doc['_index'], doc['_id'], doc['_source']


@cache
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=120,
                      giveup=fatal_code,
                      on_backoff=backoff_logger)
def get_cmr(cmr_catalog_url: str, cmr_doc_urls: dict[str, str]) -> dict:
    try:
        resp = requests.get(cmr_catalog_url)
        resp.raise_for_status()

        return resp.json()
    except Exception as e:
        logger.warning('Failed to get CMR metadata from CMR, attempting to pull from the DAAC-provided document')

        cmr_doc_data = None

        if cmr_doc_urls['s3']:
            logger.info(f'Attempting to use S3 url {cmr_doc_urls["s3"]}')

            parsed_url = urlparse(cmr_doc_urls['s3'])
            bucket = parsed_url.netloc
            key = parsed_url.path.lstrip('/')

            try:
                cmr_doc_data = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8'))
            except Exception as e:
                logger.warning(f'Could not read from S3: {e}. Attempting to fall back to https if it is available')
        elif cmr_doc_urls['https']:
            logger.info(f'Attempting to use S3 url {cmr_doc_urls["s3"]}')
            resp = requests.get(cmr_doc_urls['https'])
            resp.raise_for_status()

            cmr_doc_data = resp.json()

        if cmr_doc_data is None:
            raise RuntimeError('Could not get CMR metadata from any source')

        return cmr_doc_data


def convert_https_to_s3(
        https_url: str,
        full_urls_list: list[str],
        cmr_catalog_url: str,
        cmr_doc_urls: dict[str,str]
) -> str:
    matched_url = None

    # Try searching ES doc first in case it has a mix of S3 and HTTPS
    for url in full_urls_list:
        if url.startswith('s3://') and url.rsplit('/', 1)[-1] == https_url.rsplit('/', 1)[-1]:
            matched_url = url
            break

    # If we can't find in ES, pull the CMR metadata
    if matched_url is None:
        logger.warning(f'Could not find https url in es metadata, pulling CMR entry')

        cmr_urls = get_cmr(cmr_catalog_url, cmr_doc_urls)['RelatedUrls']
        for url_dict in cmr_urls:
            url = url_dict['URL']

            if url.startswith('s3://') and url.rsplit('/', 1)[-1] == https_url.rsplit('/', 1)[-1]:
                matched_url = url
                break

    if not matched_url:
        raise ValueError(f'Could not find matching URL in CMR record')

    return matched_url


def reduce_daac_urls(
        daac_urls: list[str],
        pattern: re.Pattern,
        cmr_catalog_url: str,
        cmr_doc_urls: dict[str, str]
) -> list[str]:
    reduced_daac_urls = set()

    for url in daac_urls:
        filename = url.rsplit('/', 1)[-1]

        if not pattern.fullmatch(filename):
            logger.info(f'Dropping URL {url} as it doesn\'t match the dataset regex')
            continue

        if url.startswith('https://') or url.startswith('http://'):
            try:
                converted_url = convert_https_to_s3(url, daac_urls, cmr_catalog_url, cmr_doc_urls)
                logger.info(f'Converting URL {url} to {converted_url} in reduced URL list')
                reduced_daac_urls.add(converted_url)
            except ValueError as e:
                logger.warning(f'Failed to convert URL {url}')
                # TODO: optionally propagate error here
                reduced_daac_urls.add(url)
        else:
            logger.info(f'Adding S3 url {url} to reduced URL list')
            reduced_daac_urls.add(url)

    logger.info(f'Final reduced URL set: {reduced_daac_urls}')

    return list(reduced_daac_urls)


@exec_wrapper
def main():
    settings = SettingsConf().cfg
    jc = JobContext('_context.json').ctx

    es_conn = es_conn_util.get_es_connection(logger)

    cnm_message = jc['cnm_message']
    product_id = cnm_message['identifier']

    es_index, es_doc_id, es_doc = get_product(es_conn, product_id)

    logger.info(f'Got ES doc: {json.dumps(es_doc, indent=2)}')

    dataset = es_doc['dataset']
    main_file_pattern: re.Pattern = settings['PRODUCT_TYPES'][dataset]['Pattern']

    daac_file_urls: list[str] = es_doc['daac_product_file_urls']

    cmr_doc_urls = {
        'https': None,
        's3': None
    }

    for url in daac_file_urls:
        if url.endswith('.cmr.json'):
            if url.startswith('s3://'):
                cmr_doc_urls['s3'] = url
            elif url.startswith('http://') or url.startswith('https://'):
                cmr_doc_urls['https'] = url

    cmr_catalog_url = es_doc['daac_catalog_url']

    reduced_urls = reduce_daac_urls(
        daac_file_urls, main_file_pattern, cmr_catalog_url, cmr_doc_urls
    )

    update_doc = {
        'doc': {
            'archive_product_urls': reduced_urls
        },
        "doc_as_upsert": True,
    }

    backoff_wrapper(es_conn.es.update, index=es_index, id=es_doc_id, body=update_doc)


if __name__ == '__main__':
    main()
