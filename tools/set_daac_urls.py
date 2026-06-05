import json
import re
from functools import cache

import backoff
import requests

from data_subscriber import es_conn_util
from opera_commons.logger import get_logger
from util.backoff_util import backoff_logger, fatal_code
from util.common_util import backoff_wrapper
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()


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
def get_cmr(cmr_catalog_url: str, cmr_doc_url: str) -> dict:
    try:
        resp = requests.get(cmr_catalog_url)
        resp.raise_for_status()

        return resp.json()
    except Exception as e:
        resp = requests.get(cmr_doc_url)
        resp.raise_for_status()

        return resp.json()


def convert_https_to_s3(https_url: str, cmr_urls: list[dict]) -> str:
    matched_url = None

    for url_dict in cmr_urls:
        url = url_dict['URL']

        if url.startswith('s3://') and url.rsplit('/', 1)[-1] == https_url.rsplit('/', 1)[-1]:
            matched_url = url
            break

    if not matched_url:
        raise ValueError(f'Could not find matching URL in CMR record')

    return matched_url


def reduce_daac_urls(daac_urls: list[str], pattern: re.Pattern, cmr_catalog_url: str, cmr_doc_url: str) -> list[str]:
    cmr_entry = None
    reduced_daac_urls = []

    for url in daac_urls:
        filename = url.rsplit('/', 1)[-1]

        if not pattern.fullmatch(filename):
            logger.info(f'Dropping URL {url} as it doesn\'t match the dataset regex')
            continue

        if url.startswith('https://') or url.startswith('http://'):
            if cmr_entry is None:
                cmr_entry = get_cmr(cmr_catalog_url, cmr_catalog_url)

            try:
                converted_url = convert_https_to_s3(url, cmr_entry['RelatedUrls'])
                logger.info(f'Converting URL {url} to {converted_url} in reduced URL list')
                reduced_daac_urls.append(converted_url)
            except ValueError as e:
                logger.warning(f'Failed to convert URL {url}')
                # TODO: optionally propagate error here
                reduced_daac_urls.append(url)
        else:
            logger.info(f'Adding S3 url {url} to reduced URL list')
            reduced_daac_urls.append(url)

    return reduced_daac_urls


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

    cmr_doc_url = None

    for url in daac_file_urls:
        if url.endswith('.cmr.json'):
            cmr_doc_url = url
            break

    cmr_catalog_url = es_doc['daac_catalog_url']

    reduced_urls = reduce_daac_urls(
        daac_file_urls, main_file_pattern, cmr_catalog_url, cmr_doc_url
    )

    update_doc = {
        'doc': {
            'archive_product_urls': reduced_urls
        },
        "doc_as_upsert": True,
    }

    backoff_wrapper(es_conn.es.update, index=es_index, id=es_doc_id, body=update_doc)


