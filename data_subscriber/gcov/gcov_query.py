import json
import re
from collections import defaultdict
from datetime import datetime, timedelta

import dateutil.parser
from more_itertools import first

from data_subscriber.gcov.gcov_catalog import GcovGranule, NisarGcovProductCatalog
from data_subscriber.gcov.gcov_granule_util import extract_track_id, extract_frame_id, extract_cycle_number
from data_subscriber.gcov_utils import load_mgrs_track_frame_db, submit_gcov_download_job, \
    join_mgrs_set_id_and_cycle_number, split_mgrs_set_id_and_cycle_number
from data_subscriber.query import BaseQuery
from opera_commons.logger import get_logger
from util.grq_client import get_body

DEFAULT_DSWX_NI_MGRS_TILE_COLLECTION_DB_LOCAL_PATH = "MGRS_collection_db_DSWx-NI_v0.1.sqlite"


class NisarGcovCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, mgrs_track_frame_db_file=None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
        self.logger = get_logger()

        # source track frame db from ancillary bucket or loads local copy
        self.mgrs_track_frame_db = load_mgrs_track_frame_db(mgrs_track_frame_db_file=mgrs_track_frame_db_file)

        self.mgrs_sets_to_process = {}

    def query_cmr(self, timerange, now):
        """
        Query CMR for NISAR L2 GCOV products.

        Args:
            timerange: DateTimeRange object containing start and end dates
            now: Current datetime

        Returns:
            List of granules from CMR
        """
        self.logger.info(f"Query CMR for NISAR L2 GCOV products with timerange: {timerange}")

        cmr_granules = super().query_cmr(timerange, now)
        return cmr_granules

    def _evaluate_mgrs_set_id_cycle_indices(self, grouped_es_docs):
        trigger_mgrs_sets_and_cycle_numbers = []

        min_num_frames = self.args.coverage_target_num
        if not min_num_frames:
            min_num_frames = self.settings["DSWX_NI_MINIMUM_NUMBER_OF_FRAMES_REQUIRED"]
        coverage_target = self.args.coverage_target
        if coverage_target is None:
            coverage_target = self.settings["DSWX_NI_COVERAGE_TARGET"]
        grace_mins = self.args.grace_mins
        if grace_mins is None:
            grace_mins = self.settings["DSWX_NI_COLLECTION_GRACE_PERIOD_MINUTES"]

        if min_num_frames is None and coverage_target is None:
            raise ValueError('Both coverage_target and min_num_frames was specified. Specify one or the other.')
        if min_num_frames is not None and coverage_target is not None:
            raise ValueError('Both coverage_target and min_num_frames were not specified. Specify one or the other.')

        if min_num_frames is not None and min_num_frames <= 0:
            raise ValueError('min_num_frames must be greater than 0.')
        if coverage_target is not None and not (0 <= coverage_target <= 100):
            raise ValueError('coverage_target must be between 0 and 100.')

        if grace_mins is None:
            raise ValueError('grace_mins must be specified.')

        self.logger.info(f'Triggering params: {min_num_frames=} {coverage_target=} {grace_mins=}')  # debug

        for mgrs_set_id_cycle_index in grouped_es_docs:
            self.logger.info(f'Evaluating {mgrs_set_id_cycle_index=}')
            mgrs_set_id, cycle_number = split_mgrs_set_id_and_cycle_number(mgrs_set_id_cycle_index)
            expected_frames: set = self.mgrs_track_frame_db.mgrs_set_id_to_frames(mgrs_set_id)

            es_docs_for_mgrs_set_cycle = grouped_es_docs[mgrs_set_id_cycle_index]
            available_frames = set([doc['_source']['frame_number'] for doc in es_docs_for_mgrs_set_cycle])

            self.logger.info(f'Expected frames for MGRS set: {expected_frames}. Available frames: {available_frames}')

            if not available_frames.issubset(expected_frames):
                raise ValueError(f'{mgrs_set_id_cycle_index=} got frames that were not a subset of expected: '
                                 f'{available_frames=}, {expected_frames=}')

            if expected_frames == available_frames:  # All frames present, so trigger
                self.logger.info(f'Triggering {mgrs_set_id_cycle_index} as it is fully covered')
                trigger_mgrs_sets_and_cycle_numbers.append((mgrs_set_id, cycle_number))
            else:
                if min_num_frames is not None:
                    sufficient_coverage = len(available_frames) >= min_num_frames
                else:
                    sufficient_coverage = ((len(expected_frames) / len(available_frames)) * 100) >= coverage_target

                if sufficient_coverage:  # Evaluate grace period
                    self.logger.info(f'Frame set {mgrs_set_id_cycle_index} has sufficient coverage to trigger')
                    retrieval_dts = {dateutil.parser.parse(doc['_source']['creation_timestamp'])
                                     for doc in es_docs_for_mgrs_set_cycle}

                    if len(retrieval_dts) == 0:
                        continue
                    elif len(retrieval_dts) == 1:
                        max_dt = first(retrieval_dts)
                    else:
                        max_dt = max(*retrieval_dts)

                    eval_time = datetime.now()
                    grace_period_minutes_remaining = timedelta(minutes=grace_mins) - (eval_time - max_dt)
                    if eval_time - max_dt < timedelta(minutes=grace_mins):
                        self.logger.info(f'Frame set still within grace period ({grace_period_minutes_remaining=}) '
                                         f'{mgrs_set_id_cycle_index=}')
                    else:
                        self.logger.info(f'Frame set aged out of grace period {mgrs_set_id_cycle_index=}')
                        trigger_mgrs_sets_and_cycle_numbers.append((mgrs_set_id, cycle_number))

        return trigger_mgrs_sets_and_cycle_numbers

    def determine_download_granules(self, cmr_granules):
        gcov_granules = self._convert_query_result_to_gcov_granules(cmr_granules)
        # set of tuples for uniquely identifying L3 products(mgrs_set_id, cycle_number)
        mgrs_sets_and_cycle_numbers = {(g.mgrs_set_id, g.cycle_number) for g in gcov_granules}
        self.logger.info(f"Found {len(gcov_granules)} GCOV granules")
        self.logger.info(f"Found {len(mgrs_sets_and_cycle_numbers)} unique MGRS sets and cycle numbers")

        # query 1: query for unsubmitted docs (this should include new granules from CMR as they should have been
        # cataloged by now)
        body = get_body(match_all=False)
        body["query"]["bool"]["must_not"].append({"exists": {"field": "download_job_ids"}})
        unsubmitted_docs = self.es_conn.es_util.query(body=body, index=NisarGcovProductCatalog.ES_INDEX_PATTERNS)
        self.logger.info(f"Found {len(unsubmitted_docs)=}")

        # Query 2: Get gcov granules for submitted but not 100%
        body = get_body(match_all=False)
        body["query"]["bool"]["must"].append({"exists": {"field": "download_job_ids"}})
        body["query"]["bool"]["must"].append({"range": {"coverage": {"gte": 0, "lt": 100}}})

        incomplete_docs = self.es_conn.es_util.query(
            body=body,
            index=NisarGcovProductCatalog.ES_INDEX_PATTERNS
        )
        self.logger.info(f"Found {len(incomplete_docs)=}")

        es_docs = unsubmitted_docs + incomplete_docs

        grouped_es_docs = defaultdict(list)
        for es_doc in es_docs:
            grouped_es_docs[es_doc["_source"]['mgrs_set_id_cycle_index']].append(es_doc)

        grouped_es_docs = dict(grouped_es_docs)
        no_new_indices = []

        for mgrs_set_id_cycle_index, gcov_set in grouped_es_docs.items():
            # collect burst sets that have at least 1 new burst since last processed
            if all({
                gcov['_source'].get('downloaded', False)
                for gcov in gcov_set
            }):
                no_new_indices.append(mgrs_set_id_cycle_index)

        for no_new in no_new_indices:
            del grouped_es_docs[no_new]

        trigger_mgrs_sets_and_cycle_numbers = self._evaluate_mgrs_set_id_cycle_indices(grouped_es_docs)

        # return gcov_granules, mgrs_sets_and_cycle_numbers
        return gcov_granules, set(trigger_mgrs_sets_and_cycle_numbers)

    def submit_gcov_download_job_submission_handler(self, mgrs_sets_and_cycle_numbers: list[tuple[str, int]], gcov_granules: list[GcovGranule], docs: list[dict]):
        self.logger.info(f"Triggering GCOV jobs for {len(mgrs_sets_and_cycle_numbers)} unique MGRS sets and cycle numbers to process")
        self.es_conn: NisarGcovProductCatalog

        batch_id_to_job_map = self.trigger_gcov_download_jobs(mgrs_sets_and_cycle_numbers, gcov_granules, docs)
        if self.args.dry_run:
            self.logger.info("dry_run=%s, Skipping marking jobs as downloaded. Producing mock job ID.", str(self.args.dry_run))
            pass
        else:
            batch_id_to_products_map = {}
            for g in gcov_granules:
                batch_id = mgrs_set_and_cycle_number = join_mgrs_set_id_and_cycle_number(g.mgrs_set_id, g.cycle_number)
                batch_id_to_products_map[batch_id] = {self.es_conn._generate_doc_id_by_gcov_granule(g): [g]}
            batch_id_to_docs_map = {}
            for doc in docs:
                batch_id = mgrs_set_and_cycle_number = join_mgrs_set_id_and_cycle_number(doc["mgrs_set_id"], doc["cycle_number"])
                batch_id_to_docs_map[batch_id] = {self.es_conn._generate_doc_id_by_doc(doc): [doc]}

            self.es_conn.mark_products_as_download_job_submitted(batch_id_to_products_map, batch_id_to_job_map, batch_id_to_docs_map)

        return batch_id_to_job_map.values()

    def create_gcov_download_product(self, mgrs_set, cycle_number):
        return {
            "_source": {
                "metadata": {
                    "batch_id": join_mgrs_set_id_and_cycle_number(mgrs_set, cycle_number)
                }
            }
        }

    def trigger_gcov_download_jobs(self, mgrs_sets_and_cycle_numbers: list[tuple[str, int]], gcov_granules: list[GcovGranule], docs: list[dict]):
        mgrs_sets_and_cycle_number_to_job_map = {}
        for mgrs_set, cycle_number in mgrs_sets_and_cycle_numbers:
            job = self.trigger_gcov_download_job(cycle_number, mgrs_set, mgrs_sets_and_cycle_numbers)
            mgrs_set_id_and_cycle_number = join_mgrs_set_id_and_cycle_number(mgrs_set, cycle_number)
            mgrs_sets_and_cycle_number_to_job_map[mgrs_set_id_and_cycle_number] = job
        return mgrs_sets_and_cycle_number_to_job_map

    def trigger_gcov_download_job(self, cycle_number, mgrs_set, mgrs_sets_and_cycle_numbers):
        product = self.create_gcov_download_product(mgrs_set, cycle_number)
        job = submit_gcov_download_job(
            params=self.create_gcov_download_job_params(
                self.args,
                product=product,
                batch_ids=[
                    join_mgrs_set_id_and_cycle_number(mgrs_set, cycle_number)
                    # for mgrs_set, cycle_number in
                    # mgrs_sets_and_cycle_numbers
                ],
                release_version=self.args.release_version
            ),
            product=product, job_queue="opera-job_worker-gcov_download",
            job_name=f"job-WF-gcov_download",
            release_version=self.args.release_version or self.settings["RELEASE_VERSION"])
        return job

    def _catalog_granules(self, granules, query_dt):
        for granule in granules:
            self.logger.info(f"Cataloging GCOV granule: {granule.native_id}")
            self.es_conn.update_granule_index(granule, self.job_id, query_dt)

        self.logger.info(f'Cataloged: {len(granules):,} GCOV granules')

        self.logger.info("Performing index refresh")
        self.es_conn.refresh()
        self.logger.info("Performed index refresh")

        self.es_conn: "NisarGcovProductCatalog"

        # query 1: query for unsubmitted docs
        from util.grq_client import get_body
        body = get_body(match_all=False)
        body["query"]["bool"]["must_not"].append({"exists": {"field": "download_job_ids"}})
        body["sort"] = {"creation_timestamp": {"order": "desc"}}
        unsubmitted_docs = self.es_conn.es_util.query(body=body, index=NisarGcovProductCatalog.ES_INDEX_PATTERNS)
        self.logger.info(f"Found {len(unsubmitted_docs)=}")

        return unsubmitted_docs

    def _convert_query_result_to_gcov_granules(self, granules: list) -> list[GcovGranule]:
        """
        Convert a list of CMR granule dicts to a list of GcovGranule objects.
        """
        gcov_granules = []
        for granule in granules:
            granule_id = granule.get("granule_id")
            native_id = granule_id

            # Find s3_download_url
            s3_download_url = None
            # matches s3://*001.h5
            # input example: s3://sds-n-cumulus-test-nisar-products/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"
            s3_regex = r'^s3:\/\/.*\d\d\d\.h5$'
            for url in granule.get("related_urls", []):
                if re.match(s3_regex, url):
                    s3_download_url = url
                    break

            # Track, frame and cycle number
            track_number = extract_track_id(granule)
            frame_number = extract_frame_id(granule)
            cycle_number = extract_cycle_number(granule)


            # MGRS set id: use DB lookup
            mgrs_set_id = None
            try:
                mgrs_sets = self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets({(frame_number, track_number)})
            except Exception:
                self.logger.error(f"Error getting MGRS set ID for granule {granule_id}. If needed, report to ADT and update the DB.")
                mgrs_sets = {}

            for mgrs_set_id in mgrs_sets.keys():
                # Acquisition times
                revision_dt = datetime.fromisoformat(granule.get("revision_date").replace("Z", "+00:00"))
                acquisition_start_time = datetime.fromisoformat(granule.get("temporal_extent_beginning_datetime").replace("Z", "+00:00"))

                gcov_granules.append(GcovGranule(
                    native_id=native_id,
                    granule_id=granule_id,
                    s3_download_url=s3_download_url,
                    track_number=track_number,
                    frame_number=frame_number,
                    cycle_number=cycle_number,
                    mgrs_set_id=mgrs_set_id,
                    mgrs_set_ids=list(mgrs_sets.keys()),
                    mgrs_set_id_cycle_index=join_mgrs_set_id_and_cycle_number(mgrs_set_id, cycle_number),
                    revision_dt=revision_dt,
                    acquisition_start_time=acquisition_start_time,
                ))
        return gcov_granules

    def _convert_db_docs_to_gcov_granules(self, docs: list) -> list[GcovGranule]:
        """
        Convert a list of CMR granule docs to a list of GcovGranule objects.
        """
        gcov_granules = []
        granules = [doc for doc in docs]
        for granule in granules:
            granule_id = granule.get("granule_id")
            native_id = granule_id

            # Find s3_download_url
            # matches s3://*001.h5
            # input example: s3://sds-n-cumulus-test-nisar-products/NISAR_L2_GCOV_BETA_V1/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001/NISAR_L2_PR_GCOV_015_156_A_011_2005_DVDV_A_20230619T000817_20230619T000835_T00406_M_P_J_001.h5"
            s3_download_url = granule["s3_download_url"]


            # Track, frame and cycle number
            track_number = extract_track_id(granule)
            frame_number = extract_frame_id(granule)
            cycle_number = extract_cycle_number(granule)


            # MGRS set id: use DB lookup
            mgrs_set_id = None
            try:
                mgrs_sets = self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets({(frame_number, track_number)})
            except Exception:
                self.logger.error(f"Error getting MGRS set ID for granule {granule_id}. If needed, report to ADT and update the DB.")
                mgrs_sets = {}

            for mgrs_set_id in mgrs_sets.keys():
                # Acquisition times
                revision_dt = datetime.fromisoformat(granule.get("revision_dt").replace("Z", "+00:00"))
                acquisition_start_time = datetime.fromisoformat(granule.get("acquisition_start_time").replace("Z", "+00:00"))

                gcov_granules.append(GcovGranule(
                    native_id=native_id,
                    granule_id=granule_id,
                    s3_download_url=s3_download_url,
                    track_number=track_number,
                    frame_number=frame_number,
                    cycle_number=cycle_number,
                    mgrs_set_id=mgrs_set_id,
                    mgrs_set_ids=list(mgrs_sets.keys()),
                    mgrs_set_id_cycle_index=join_mgrs_set_id_and_cycle_number(mgrs_set_id, cycle_number),
                    revision_dt=revision_dt,
                    acquisition_start_time=acquisition_start_time,
                ))
        return gcov_granules

    def create_gcov_download_job_params(self, args=None, product=None, batch_ids=None, release_version: str = None):
        return [
            {
                "name": "batch_ids",
                "value": "--batch-ids " + " ".join(batch_ids) if batch_ids else "",
                "from": "value"
            },
            {
                "name": "smoke_run",
                "value": "--smoke-run" if args.smoke_run else "",
                "from": "value"
            },
            {
                "name": "dry_run",
                "value": "--dry-run" if args.dry_run else "",
                "from": "value"
            },
            {
                "name": "endpoint",
                "value": f"--endpoint={args.endpoint}",
                "from": "value"
            },
            {
                "name": "transfer_protocol",
                "value": f"--transfer-protocol={args.transfer_protocol}",
                "from": "value"
            },
            {
                "name": "proc_mode",
                "value": f"--processing-mode={args.proc_mode}",
                "from": "value"
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": json.dumps(product["_source"])
            },
            {
                "name": "dataset_type",
                "from": "value",
                "type": "text",
                "value": "L2_NISAR_GCOV"
            },
            {
                "name": "input_dataset_id",
                "type": "text",
                "from": "value",
                "value": product["_source"]["metadata"]["batch_id"]
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": product["_source"]
            },
            {
                "name": "dswx_ni_job_release",
                "from": "value",
                "type": "text",
                "value": f"--release-version={release_version}"
            }
        ]