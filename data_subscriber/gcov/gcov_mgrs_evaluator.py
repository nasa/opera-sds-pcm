"""
DSWx-NI MGRS Evaluator

Triggered by NISAR GCOV ingest or on-demand re-evaluation. Queries ES for all GCOV products matching
the MGRS tile sets covered by the input, creating a DSWX-NI State Config for each.
"""
import os
import shutil
from datetime import datetime, timedelta, timezone

from data_subscriber import es_conn_util
from data_subscriber.cslc.disp_s1_state_config import find_state_config
from data_subscriber.gcov import gcov_state_config_constants as c
from data_subscriber.gcov.gcov_granule_util import (extract_track_id, extract_frame_id, extract_cycle_number,
                                                    extract_acquisition_time_range)
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.logger import get_logger
from util.common_util import backoff_wrapper, create_state_config_dataset
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()


# TODO: Need to determine if cycle is reliable. I think there are some sets covered by different cycles
#  eg. MS_1_173[1_1, 173_176] & MS_1_174[1_1, 1_2, 173_176]. In current DB, these are both over water
#  so we can safely ignore them, but we may want to plan a way to handle this scenario


class GcovMgrsEvaluator:
    """Evaluates tile set completeness"""

    def __init__(self, es_conn):
        self.mgrs_track_frame_db = load_mgrs_track_frame_db()
        self.es_conn = es_conn
        self.settings = SettingsConf().cfg
        self.msgs = []
        self.msg_details = ""

    def _msg(self, short, detail=""):
        """Append a terse message for Figaro and an optional detail line."""
        self.msgs.append(short)
        if detail:
            self.msg_details += detail + "\n"

    def evaluate(self, input_dataset_id, metadata, dataset_type, force_publish=False):

        if dataset_type == c.MGRS_SET_STATE_CONFIG:
            mgrs_set_id = metadata.get(c.MGRS_SET_ID)
            sensing_date = metadata.get(c.SENSING_DATE)
            cycle_number = metadata.get(c.CYCLE_NUMBER)

            logger.info(f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}, {sensing_date=}')
            self._msg(
                f're-eval {mgrs_set_id} {sensing_date}',
                f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}, {sensing_date=}'
            )

            self._evaluate_mgrs_tile_set(mgrs_set_id, cycle_number, sensing_date, force_publish=force_publish)
        else:
            native_id = input_dataset_id

            track_id = extract_track_id(native_id)
            frame_id = extract_frame_id(native_id)
            cycle_number = extract_cycle_number(native_id)

            acquisition_start_dts, _ = extract_acquisition_time_range(native_id)
            sensing_date = acquisition_start_dts.strftime("%Y%m%d")

            logger.info(f'Evaluating GCOV {native_id}, {track_id=}, {frame_id=}, {sensing_date=}')

            mgrs_set_ids = list(self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets({(frame_id, track_id)}).keys())
            mgrs_set_ids = [mgrs_set_id for mgrs_set_id in mgrs_set_ids
                            if self.mgrs_track_frame_db.get_lof_for_mgrs_set_id(mgrs_set_id) != 'water']

            self._msg(
                f'evaluating {len(mgrs_set_ids)} tile sets',
                f'Track-frame {track_id}_{frame_id} belongs to {len(mgrs_set_ids)}: {mgrs_set_ids}'
            )

            if len(mgrs_set_ids) == 0:
                logger.info(f'Track-frame {track_id}_{frame_id} belongs to no tile sets with '
                            f'land coverage and will be skipped')
                self._msg(
                    f'no land coverage for {track_id}_{frame_id}. Skipping',
                    f'Track-frame {track_id}_{frame_id} belongs to no tile sets with land coverage and will be skipped'
                )
                return

            for mgrs_set_id in mgrs_set_ids:
                self._evaluate_mgrs_tile_set(mgrs_set_id, cycle_number, sensing_date, force_publish=force_publish)

    def _evaluate_mgrs_tile_set(self, mgrs_set_id, cycle_number, sensing_date, force_publish=False):
        sc_id = self._get_sc_id(mgrs_set_id, cycle_number)
        expected_track_frames = self.mgrs_track_frame_db.mgrs_set_id_to_track_frames(mgrs_set_id)

        logger.info(f'Evaluating state config {sc_id}')

        existing_state_config, sc_index = find_state_config(self.es_conn, sc_id, c.MGRS_SET_STATE_CONFIG)

        if not force_publish:
            if existing_state_config.get(c.IS_COMPLETE, False):
                logger.info(f'State config {sc_id} is already complete and will be skipped')
                self._msg(
                    f'{mgrs_set_id}${cycle_number} already complete',
                    f'State config {sc_id} is already complete and will be skipped'
                )
                return

        existing_found_track_frames = set(existing_state_config.get(c.FOUND_TRACK_FRAMES, []))

        found_track_frames, excluded_track_frames, gcov_product_paths, start_time, end_time = self._query_gcov(
            expected_track_frames, cycle_number, sensing_date
        )

        geojson = self.mgrs_track_frame_db.get_geojson_for_mgrs_set_id(mgrs_set_id)

        if existing_found_track_frames != set(found_track_frames):
            # Create or update SC
            expired = False
            self._create_sc(mgrs_set_id, cycle_number, sensing_date, expected_track_frames, found_track_frames,
                            excluded_track_frames, gcov_product_paths, start_time, end_time, geojson=geojson)
        else:
            expiration_time = self._get_state_config_expiration_time(sc_id)
            now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            if now >= expiration_time:
                self._expire_sc(existing_state_config, sc_index, start_time, end_time, geojson=geojson)
                expired = True
            else:
                expired = False

        n_found = len(found_track_frames)
        n_excluded = len(excluded_track_frames)
        n_expected = len(expected_track_frames)
        missing = sorted(set(expected_track_frames) - (set(found_track_frames) | set(excluded_track_frames)))

        excluded_msg_str = f' (+ {n_excluded}/{n_expected} excluded)' if n_excluded > 0 else ' '

        if n_found == n_expected:
            short_msg = f"{mgrs_set_id}${cycle_number} complete {n_found}/{n_expected}{excluded_msg_str}"
            detail_msg = (f"Tile set {mgrs_set_id}${cycle_number}: complete {n_found}/{n_expected}{excluded_msg_str} "
                          f"track-frames")
        elif not expired:
            short_msg = f"{mgrs_set_id}${cycle_number} incomplete {n_found}/{n_expected}{excluded_msg_str}"
            detail_msg = (f"Tile set {mgrs_set_id}${cycle_number}: incomplete {n_found}/{n_expected}"
                          f"{excluded_msg_str} track-frames, missing: {missing}")
        else:
            short_msg = f"{mgrs_set_id}${cycle_number} expired with {n_found}/{n_expected}{excluded_msg_str}"
            detail_msg = (f"Tile set {mgrs_set_id}${cycle_number}: expired with {n_found}/{n_expected}"
                          f"{excluded_msg_str} track-frames, missing: {missing}")

        self._msg(short_msg, detail_msg)

    def _query_gcov(
            self,
            expected_track_frames,
            cycle_number,
            sensing_date
    ) -> tuple[list[str], list[str], dict[str, list[str]], str, str]:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"dataset_type.keyword": "L2_GCOV_NI"}},
                        {"terms": {"metadata.track_frame.keyword": expected_track_frames}},
                    ],
                    "filter": [
                        {"term": {"metadata.acquisition_cycle": cycle_number}},
                    ]
                }
            },
            "size": len(expected_track_frames) * 2,
        }

        results = backoff_wrapper(
            self.es_conn.query,
            body=body,
            index="grq_*_l2_gcov_ni-*"
        )

        found_track_frames = set()
        excluded_track_frames = set()
        product_paths = {'https': [], 's3': []}

        start_times = []
        end_times = []

        if results:
            for hit in results:
                source = hit.get("_source", {})
                meta = source.get("metadata", {})
                track_frame = meta.get("track_frame")
                if track_frame and track_frame in expected_track_frames:
                    if meta['polarization'] not in c.VALID_POLS:
                        excluded_track_frames.add(track_frame)
                        continue
                    if meta['bandwidth_mode'] not in c.VALID_MODES:
                        excluded_track_frames.add(track_frame)
                        continue
                    found_track_frames.add(track_frame)
                    # Get the ASF S3 path to the .h5 file (not the HySDS dataset dir URL)
                    product_paths['https'].extend(meta['product_https_paths'])
                    product_paths['s3'].extend(meta['product_s3_paths'])
                start_times.append(source['starttime'])
                end_times.append(source['endtime'])

        found_track_frames = list(found_track_frames)
        found_track_frames.sort()

        excluded_track_frames = list(excluded_track_frames)
        excluded_track_frames.sort()

        return found_track_frames, excluded_track_frames, product_paths, min(start_times), max(end_times)

    def _create_sc(self, tile_set_id, cycle_number, sensing_date, expected_track_frames, found_track_frames,
                   excluded_track_frames, product_paths, start_time, end_time, geojson=None):
        sc_id = self._get_sc_id(tile_set_id, cycle_number)

        grace_period = self.settings['DSWX_NI_COLLECTION_GRACE_PERIOD_MINUTES']
        new_expiration_time = (datetime.now(tz=timezone.utc) + timedelta(minutes=grace_period))
        new_expiration_date = new_expiration_time.strftime("%Y%m%d")
        new_expiration_time = new_expiration_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        expected = sorted(expected_track_frames)
        found = sorted(found_track_frames)
        excluded = sorted(excluded_track_frames)
        missing = sorted(set(expected) - (set(found) | set(excluded)))
        coverage_actual = len(found) + len(excluded)
        coverage_expected = len(expected)

        is_complete = len(missing) == 0
        is_skipped = len(excluded) == len(expected)

        if is_complete:
            completeness_reason = f"complete: {coverage_actual}/{coverage_expected} track-frames"
        else:
            completeness_reason = (f"incomplete: {coverage_actual}/{coverage_expected} "
                                   f"track-frames, missing {len(missing)}")

        if len(excluded) > 0:
            completeness_reason += f', excluded {len(excluded)}'

        metadata = {
            "id": sc_id,
            c.STATE_CONFIG_TYPE: c.STATE_CONFIG_TYPE,
            c.MGRS_SET_ID: tile_set_id,
            c.CYCLE_NUMBER: cycle_number,
            c.SENSING_DATE: sensing_date,
            c.EXPECTED_TRACK_FRAMES: expected,
            c.FOUND_TRACK_FRAMES: found,
            c.EXCLUDED_TRACK_FRAMES: excluded,
            c.MISSING_TRACK_FRAMES: missing,
            c.LAND_OCEAN_FLAG: self.mgrs_track_frame_db.get_lof_for_mgrs_set_id(tile_set_id),
            c.BOUNDING_BOX: self.mgrs_track_frame_db.get_bounding_box_for_mgrs_set_id(tile_set_id),
            c.GCOV_HTTPS_PRODUCT_PATHS: product_paths['https'],
            c.GCOV_S3_PRODUCT_PATHS: product_paths['s3'],
            c.COVERAGE_ACTUAL: coverage_actual,
            c.COVERAGE_EXPECTED: coverage_expected,
            c.IS_COMPLETE: is_complete,
            c.COMPLETENESS_REASON: completeness_reason,
            c.EXPIRATION_DATE: new_expiration_date,
            c.IS_EXPIRED: False,
            c.IS_SKIPPED: is_skipped,
        }

        # Remove existing dataset dir if present (will be recreated)
        if os.path.isdir(sc_id):
            shutil.rmtree(sc_id)

        logger.info(f"Creating state config: {sc_id} (coverage={coverage_actual}/{coverage_expected}, {is_complete=})")

        create_state_config_dataset(
            dataset_name=sc_id,
            metadata=metadata,
            start_time=start_time,
            end_time=end_time,
            expiration_time=new_expiration_time,
            dataset_type=c.MGRS_SET_STATE_CONFIG,
            geojson=geojson,
        )

        return sc_id, metadata

    def _expire_sc(self, state_config, sc_index, start_time, end_time, geojson=None):
        mgrs_set_id = state_config.get(c.MGRS_SET_ID)
        cycle_number = state_config.get(c.CYCLE_NUMBER)

        sc_id = self._get_sc_id(mgrs_set_id, cycle_number)
        expired_sc_id = self._get_sc_id(mgrs_set_id, cycle_number, expired=True)

        metadata = state_config
        metadata[c.IS_EXPIRED] = True
        metadata[c.IS_SKIPPED] = len(metadata[c.FOUND_TRACK_FRAMES]) == 0

        # Remove existing dataset dir if present (will be recreated)
        if os.path.isdir(sc_id):
            shutil.rmtree(sc_id)
        if os.path.isdir(expired_sc_id):
            shutil.rmtree(expired_sc_id)

        logger.info(f"Expiring state config: {sc_id}")

        # Directly update the doc instead of republishing to avoid double-triggering the partial SCIFLO rule
        self.es_conn.update_document(
            index=sc_index,
            id=sc_id,
            script={
                "source": "ctx._source.metadata.is_expired = true; ctx._source.metadata.is_skipped = params.skipped",
                "lang": "painless",
                "params": {
                    "skipped": metadata[c.IS_SKIPPED]
                }
            },
            # body={
            #     "script": {
            #         "source": "ctx._source.metadata.is_expired = true; ctx._source.metadata.is_skipped = params.skipped",
            #         "lang": "painless",
            #         "params": {
            #             "skipped": metadata[c.IS_SKIPPED]
            #         }
            #     },
            #     "query": {
            #         "bool": {
            #             "must": [
            #                 {"match": {"id.keyword": sc_id}}
            #             ]
            #         }
            #     }
            # },
            refresh=True
        )

        # create_state_config_dataset(
        #     dataset_name=sc_id,
        #     metadata=metadata,
        #     start_time=start_time,
        #     end_time=end_time,
        #     dataset_type=c.MGRS_SET_STATE_CONFIG,
        #     geojson=geojson,
        # )

        metadata['id'] = expired_sc_id

        create_state_config_dataset(
            dataset_name=expired_sc_id,
            metadata=metadata,
            start_time=start_time,
            end_time=end_time,
            dataset_type=c.MGRS_SET_EXPIRED_STATE_CONFIG,
            geojson=geojson,
        )

        return sc_id, metadata

    def _get_state_config_expiration_time(self, sc_id):
        existing_document = backoff_wrapper(
            self.es_conn.search_by_id,
            id=sc_id,
            index=c.MGRS_SET_STATE_CONFIG_ES_PATTERN,
            ignore=[404]
        )

        if existing_document.get("found", False):
            return existing_document.get('_source', {}).get('expiration_time')
        return None

    @staticmethod
    def _get_sc_id(mgrs_set_id, cycle_number, expired=False):
        if not expired:
            return f'dswx_ni_set{mgrs_set_id}_cycle{cycle_number}-state-config'
        else:
            return f'dswx_ni_set{mgrs_set_id}_cycle{cycle_number}-expired-state-config'


@exec_wrapper
def evaluate():
    """HySDS job entry point."""
    jc = JobContext("_context.json")
    job_context = jc.ctx

    product_metadata = job_context.get("product_metadata", {})
    metadata = product_metadata.get("metadata", {})
    input_dataset_id = job_context.get("input_dataset_id")
    dataset_type = job_context.get("dataset_type", "")
    force_publish = job_context.get("force_publish", False)

    es_conn = es_conn_util.get_es_connection(logger)

    evaluator = GcovMgrsEvaluator(es_conn)
    evaluator.evaluate(input_dataset_id, metadata, dataset_type,
                       force_publish=force_publish)


if __name__ == "__main__":
    evaluate()
