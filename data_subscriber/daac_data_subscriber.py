#!/usr/bin/env python3

# This is only needed if you want to run this code locally instead of a deployed pcm environment
#import tests.data_subscriber.conftest

import argparse
import sys
from urllib.parse import urlparse

from commons.logger import configure_library_loggers, get_logger
from data_subscriber.asf_cslc_download import AsfDaacCslcDownload
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.asf_slc_download import AsfDaacSlcDownload
from data_subscriber.asf_rtc_for_dist_download import AsfDaacRtcForDistDownload
from data_subscriber.catalog import ProductCatalog
from data_subscriber.cmr import (ProductType, PGEProduct,
                                 Provider, get_cmr_token,
                                 COLLECTION_TO_PROVIDER_TYPE_MAP,
                                 COLLECTION_TO_PRODUCT_TYPE_MAP)
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog, CSLCStaticProductCatalog
from data_subscriber.cslc.cslc_query import CslcCmrQuery
from data_subscriber.cslc.cslc_static_query import CslcStaticCmrQuery
from data_subscriber.gcov.gcov_catalog import NisarGcovProductCatalog
from data_subscriber.gcov.gcov_query import NisarGcovCmrQuery
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.hls.hls_query import HlsCmrQuery
from data_subscriber.lpdaac_download import DaacDownloadLpdaac
from data_subscriber.parser import create_parser, validate_args
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.rtc.rtc_query import RtcCmrQuery
from data_subscriber.rtc_for_dist.rtc_for_dist_catalog import RTCForDistProductCatalog
from data_subscriber.rtc_for_dist.rtc_for_dist_query import RtcForDistCmrQuery
from data_subscriber.slc.slc_catalog import SLCProductCatalog
from data_subscriber.slc.slc_query import SlcCmrQuery
from data_subscriber.survey import run_survey
from util.conf_util import SettingsConf
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id


@exec_wrapper
def main():
    run(sys.argv)


def run(argv: list[str]):
    parser = create_parser()
    args = parser.parse_args(argv[1:])

    validate_args(args)

    logger = get_logger(args.verbose, args.quiet)
    configure_library_loggers()

    es_conn = supply_es_conn(args)

    logger.debug(f"daac_data_subscriber.py invoked with {args=}")

    job_id = supply_job_id()
    logger.debug(f"Using {job_id=}")

    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(args.endpoint, settings)

    results = {}

    if args.subparser_name == "survey":
        run_survey(args, token, cmr, settings)

    if args.subparser_name == "query" or args.subparser_name == "full":
        results["query"] = run_query(args, token, es_conn, cmr, job_id, settings)

    if args.subparser_name == "download" or args.subparser_name == "full":
        netloc = urlparse(f"https://{edl}").netloc

        results["download"] = run_download(args, token, es_conn, netloc, username, password, cmr, job_id)

    logger.info(f"{len(results)=}")
    logger.debug(f"{results=}")
    logger.info("END")

    return results


def run_query(args: argparse.Namespace, token: str, es_conn: ProductCatalog, cmr, job_id, settings):
    product_type = COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection]

    if product_type == ProductType.HLS:
        cmr_query = HlsCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.SLC:
        cmr_query = SlcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.CSLC:
        cmr_query = CslcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.CSLC_STATIC:
        cmr_query = CslcStaticCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.RTC: #RTC input can have multiple product types
        if args.product and args.product == PGEProduct.DIST_1:
            cmr_query = RtcForDistCmrQuery(args, token, es_conn, cmr, job_id, settings)
        else:
            cmr_query = RtcCmrQuery(args, token, es_conn, cmr, job_id, settings)
    elif product_type == ProductType.NISAR_GCOV:
        cmr_query = NisarGcovCmrQuery(args, token, es_conn, cmr, job_id, settings)
    else:
        raise ValueError(f'Unknown collection type "{args.collection}" provided')

    return cmr_query.run_query()

def run_download(args, token, es_conn, netloc, username, password, cmr, job_id):
    provider = (COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                if hasattr(args, "collection") else args.provider)

    if provider == Provider.LPCLOUD:
        downloader = DaacDownloadLpdaac(provider)
    elif provider in (Provider.ASF, Provider.ASF_SLC):
        downloader = AsfDaacSlcDownload(provider)
    elif provider == Provider.ASF_RTC:
        if args.product and  args.product == PGEProduct.DIST_1:
            downloader = AsfDaacRtcForDistDownload(provider)
        else:
            downloader = AsfDaacRtcDownload(provider)
    elif provider == Provider.ASF_CSLC:
        downloader = AsfDaacCslcDownload(provider)
    elif provider == Provider.ASF_CSLC_STATIC:
        raise NotImplementedError("Direct download of CSLC-STATIC products is not supported")
    else:
        raise ValueError(f'Unknown product provider "{provider}"')

    downloader.run_download(args, token, es_conn, netloc, username, password, cmr, job_id)


def supply_es_conn(args):
    logger = get_logger()
    provider = (COLLECTION_TO_PROVIDER_TYPE_MAP[args.collection]
                if hasattr(args, "collection")
                else args.provider)

    if provider == Provider.LPCLOUD:
        es_conn = HLSProductCatalog(logger)
    elif provider in (Provider.ASF, Provider.ASF_SLC):
        es_conn = SLCProductCatalog(logger)
    elif provider == Provider.ASF_RTC: # RTC input can have multiple product types
        if args.product and args.product == PGEProduct.DIST_1:
            es_conn = RTCForDistProductCatalog(logger)
        else:
            es_conn = RTCProductCatalog(logger)
    elif provider == Provider.ASF_CSLC:
        es_conn = CSLCProductCatalog(logger)
    elif provider == Provider.ASF_CSLC_STATIC:
        es_conn = CSLCStaticProductCatalog(logger)
    elif provider == Provider.ASF_NISAR_GCOV:
        es_conn = NisarGcovProductCatalog(logger)
    else:
        raise ValueError(f'Unsupported provider "{provider}"')

    return es_conn


if __name__ == "__main__":
    main()
