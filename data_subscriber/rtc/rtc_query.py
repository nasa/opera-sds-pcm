import asyncio
import logging
import math
import re
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal
import boto3
import json

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, first

from data_subscriber.cmr import COLLECTION_TO_PRODUCT_TYPE_MAP, async_query_cmr, CMR_COLLECTION_TO_PROVIDER_TYPE_MAP
from data_subscriber.rtc import mgrs_bursts_collection_db_client as mbc_client, evaluator
from data_subscriber.rtc.rtc_download_job_submitter import submit_rtc_download_job_submissions_tasks
from data_subscriber.url import form_batch_id, form_batch_id_cslc, _slc_url_to_chunk_id
from data_subscriber.query import CmrQuery
from geo.geo_util import does_bbox_intersect_north_america, does_bbox_intersect_region, _NORTH_AMERICA
from rtc_utils import rtc_granule_regex
from util.conf_util import SettingsConf

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])

#  required constants
MISSION_EPOCH_S1A = dateutil.parser.isoparse("20190101T000000Z")  # set approximate mission start date
MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=12).total_seconds()

class RtcCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.affected_mgrs_set_id_acquisition_ts_cycle_indexes = set()

    def prepare_additional_fields(self, granule, args, granule_id):
        # granule_id looks like this: OPERA_L2_RTC-S1_T074-157286-IW3_20210705T183117Z_20230818T214352Z_S1A_30_v0.4
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        additional_fields["instrument"] = "S1A" if "S1A" in granule_id else "S1B"

        match_product_id = re.match(rtc_granule_regex, granule_id)
        acquisition_dts = match_product_id.group("acquisition_ts")  #e.g. 20210705T183117Z
        burst_id = match_product_id.group("burst_id")  # e.g. T074-157286-IW3

        # Returns up to two mgrs_set_ids. e.g. MS_74_76
        mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
        mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs,
                                                                 mbc_client.product_burst_id_to_mapping_burst_id(
                                                                     burst_id))
        additional_fields["mgrs_set_ids"] = mgrs_burst_set_ids

        # Determine acquisition cycle
        instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B
        acquisition_cycle, acquisition_index = determine_acquisition_cycle(burst_id, acquisition_dts, instrument_epoch)
        additional_fields["acquisition_cycle"] = acquisition_cycle

        update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(acquisition_cycle, acquisition_index,
                                                                          additional_fields, mgrs_burst_set_ids)
        update_affected_mgrs_set_ids(acquisition_cycle, acquisition_index,
                                     self.affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids)

        return additional_fields

    def catalog_granules(self, granules, query_dt):
        granules[:] = filter_granules_rtc(granules)
        super().catalog_granules(granules, query_dt)

    async def refresh_index(self):
        logger.info("performing index refresh")
        self.es_conn.refresh()
        logger.info("performed index refresh")

        logger.info("evaluating available burst sets")
        logger.info(f"{self.affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")
        fully_covered_mgrs_sets, target_covered_mgrs_sets, incomplete_mgrs_sets = await evaluator.main(
            mgrs_set_id_acquisition_ts_cycle_indexes=self.affected_mgrs_set_id_acquisition_ts_cycle_indexes,
            coverage_target=self.settings["DSWX_S1_COVERAGE_TARGET"]
        )

        processable_mgrs_sets = {**incomplete_mgrs_sets, **fully_covered_mgrs_sets}

        # convert to "batch_id" mapping
        batch_id_to_products_map = defaultdict(set)
        for mgrs_set_id, product_burst_sets in processable_mgrs_sets.items():
            for product_burstset in product_burst_sets:
                rtc_granule_id_to_product_docs_map = first(product_burstset)
                first_product_doc_list = first(rtc_granule_id_to_product_docs_map.values())
                first_product_doc = first(first_product_doc_list)
                acquisition_cycle = first_product_doc["acquisition_cycle"]
                batch_id = "{}${}".format(mgrs_set_id, acquisition_cycle)
                batch_id_to_products_map[batch_id] = product_burstset
                if self.args.smoke_run:
                    logger.info(f"{self.args.smoke_run=}. Not processing more burst_sets.")
                    break
            if self.args.smoke_run:
                logger.info(f"{self.args.smoke_run=}. Not processing more sets of burst_sets.")
                break

        return batch_id_to_products_map

def determine_acquisition_cycle(burst_id, acquisition_dts, instrument_epoch):
    """RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
    The cycle restarts periodically with some miniscule drift over time and the life of the mission."""
    burst_identification_number = int(burst_id.split(sep="-")[1])  # e.g. 157286
    seconds_after_mission_epoch = (dateutil.parser.isoparse(acquisition_dts) - instrument_epoch).total_seconds()
    acquisition_index = (
                                seconds_after_mission_epoch - (ACQUISITION_CYCLE_DURATION_SECS * (
                                burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
                        ) / ACQUISITION_CYCLE_DURATION_SECS
    return round(acquisition_index), acquisition_index

def update_affected_mgrs_set_ids(acquisition_cycle, acquisition_index, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids):
    acquisition_index_floor = math.floor(acquisition_index)
    acquisition_index_ceil = math.ceil(acquisition_index)

    # ati looks like this: MS_1_9$151
    # construct filters for evaluation
    if len(mgrs_burst_set_ids) == 1:
        # ati = Acquisition Time Index
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle + 1)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(future_ati)

        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(past_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati)
    elif len(mgrs_burst_set_ids) == 2:
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle + 1)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_a)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_b)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(future_ati)
        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(past_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_a)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_b)
    else:
        raise AssertionError("Unexpected burst overlap: "+ str(mgrs_burst_set_ids))


def update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(acquisition_cycle, acquisition_index, additional_fields, mgrs_burst_set_ids):
    acquisition_index_floor = math.floor(acquisition_index)
    acquisition_index_ceil = math.ceil(acquisition_index)
    # construct filters for evaluation
    if len(mgrs_burst_set_ids) == 1:
        # ati = Acquisition Time Index
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle + 1)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati]

        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati]
    elif len(mgrs_burst_set_ids) == 2:
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle + 1)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati_a, current_ati_b]
        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati_a, current_ati_b]
    else:
        raise AssertionError("Unexpected burst overlap: " + str(mgrs_burst_set_ids))

def filter_granules_rtc(granules):
    filtered_granules = []
    for granule in granules:
        granule_id = granule.get("granule_id")

        match_product_id = re.match(rtc_granule_regex, granule_id)
        burst_id = match_product_id.group("burst_id")

        mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
        mgrs_sets = mbc_client.burst_id_to_mgrs_set_ids(mgrs,
                                                            mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
        if not mgrs_sets:
            logging.debug(f"{burst_id=} not associated with land or land/water data. skipping.")
            continue

        filtered_granules.append(granule)
    return filtered_granules
