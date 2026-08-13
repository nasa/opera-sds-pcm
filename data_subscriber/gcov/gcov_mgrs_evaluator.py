"""
DSWx-NI MGRS Evaluator

Triggered by NISAR GCOV ingest or on-demand re-evaluation. Queries ES for all GCOV products matching
the MGRS tile sets covered by the input, creating a DSWX-NI State Config for each.
"""
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Union

from shapely import from_geojson
from shapely.geometry import Polygon, MultiPolygon

from data_subscriber import es_conn_util
from data_subscriber.cslc.disp_s1_state_config import find_state_config
from data_subscriber.gcov import gcov_state_config_constants as c
from data_subscriber.gcov.gcov_granule_util import (extract_track_id, extract_frame_id, extract_cycle_number,
                                                    extract_acquisition_time_range)
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.logger import get_logger
from util.common_util import backoff_wrapper, create_info_message_files, create_state_config_dataset
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.geo_util import area_from_polygon

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

    def _refresh_index(self, index_pattern=None):
        """
        Attempt to refresh an ES index/index pattern, defaulting to the MGRS state config GRQ index pattern.

        The refresh call is wrapped in an exponential backoff to reduce transient failures. If failures persist,
        a warning is logged.

        Args:
            index_pattern: Index name or pattern to refresh.
        """
        if index_pattern is None:
            index_pattern = c.MGRS_SET_STATE_CONFIG_ES_PATTERN

        try:
            logger.info(f'Attempting refresh on {index_pattern}')
            backoff_wrapper(
                self.es_conn.es.indices.refresh,
                index=index_pattern,
                ignore_unavailable=True,
                allow_no_indices=True,
                expand_wildcards="open",
            )
        except Exception as e:
            logger.warning(f'Failed index refresh: {e}. Newly created documents (<1s) in {index_pattern} may not '
                           f'yet be indexed')

    def evaluate(self, input_dataset_id, metadata, dataset_type, force_publish=False):
        """
        Run the DSWx-NI state config evaluation logic on a given input dataset. The dataset can be an existing state
        config, a GCOV granule, or a batch of GCOV granules. The former will evaluate for state config expiration, the
        latter two will create/update existing state configs.

        Args:
            input_dataset_id: The identifier of the input dataset
            metadata: The product metadata of the input dataset
            dataset_type: The HySDS dataset id of the input dataset type (dswx_ni-state-config, L2_GCOV_NI or
                          L2_GCOV_NI_BATCH)
            force_publish: If true, republish the state config, even if it is already marked as complete or expired.
        """
        sc_datasets = []

        if dataset_type == c.MGRS_SET_STATE_CONFIG:
            mgrs_set_id = metadata.get(c.MGRS_SET_ID)
            cycle_number = metadata.get(c.CYCLE_NUMBER)

            logger.info(f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}')
            self._msg(
                f're-eval {mgrs_set_id}',
                f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}'
            )

            sc = self._evaluate_mgrs_tile_set(mgrs_set_id, cycle_number, force_publish=force_publish)

            if sc:
                sc_datasets.append(sc)
        else:
            if dataset_type == c.GCOV_BATCH:
                logger.info(f'Evaluating GCOV batch {input_dataset_id} ({metadata["count"]:,} GCOV inputs)')
                input_gcovs = [granule['id'] for granule in metadata['granules']]
            else:
                logger.info(f'Evaluating single GCOV {input_dataset_id}')
                input_gcovs = [input_dataset_id]

            for native_id in input_gcovs:
                track_id = extract_track_id(native_id)
                frame_id = extract_frame_id(native_id)
                cycle_number = extract_cycle_number(native_id)

                acquisition_start_dts, _ = extract_acquisition_time_range(native_id)
                sensing_time = acquisition_start_dts.strftime("%Y-%m-%dT%H:%M:%H")

                logger.info(f'Evaluating GCOV {native_id}, {track_id=}, {frame_id=}, {sensing_time=}')

                mgrs_set_ids = list(self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets({(frame_id, track_id)}).keys())
                mgrs_set_ids = [mgrs_set_id for mgrs_set_id in mgrs_set_ids
                                if self.mgrs_track_frame_db.get_lof_for_mgrs_set_id(mgrs_set_id) != 'water']

                if len(mgrs_set_ids) == 0:
                    logger.info(f'Track-frame {track_id}_{frame_id} belongs to no tile sets with '
                                f'land coverage and will be skipped')
                    self._msg(
                        f'no land coverage for {track_id}_{frame_id}. Skipping',
                        f'Track-frame {track_id}_{frame_id} belongs to no tile sets '
                        f'with land coverage and will be skipped'
                    )
                else:
                    self._msg(
                        f'evaluating {len(mgrs_set_ids)} tile sets',
                        f'Track-frame {track_id}_{frame_id} belongs to {len(mgrs_set_ids)}: {mgrs_set_ids}'
                    )
                    for mgrs_set_id in mgrs_set_ids:
                        sc = self._evaluate_mgrs_tile_set(
                            mgrs_set_id, cycle_number, force_publish=force_publish
                        )

                        if sc:
                            sc_datasets.append(sc)

        logger.info('Finished state config evaluation(s)')

        if len(sc_datasets) > 0:
            logger.info('Confirming state config datasets should be published (ie, they weren\'t already published '
                        'by a parallel evaluator)')
            self._confirm_state_config_publications(sc_datasets)
        else:
            logger.info('No new or updated non-expired state configs to publish')

        create_info_message_files(self.msgs, self.msg_details)

    def _confirm_state_config_publications(self, sc_ids: list[str]):
        """
        Iterate over created state config datasets, deleting them if they've already been marked as complete (which
        can happen from parallel evaluator jobs)

        Args:
            sc_ids: List of state config IDs to check
        """
        for sc_id in sc_ids:
            complete, expired, skipped = self._get_state_config_state(sc_id)

            if complete and not expired and not skipped:
                logger.warning(f'State config {sc_id} has already been published as complete. Removing it from this '
                               f'evaluator\'s pub list to avoid double triggering')
                self._msg(
                    f'dedup publising of {sc_id}',
                    f'State config {sc_id} has already been published as complete in a parallel worker so it '
                    f'will be removed from this job\'s results'
                )
                if os.path.isdir(sc_id):
                    shutil.rmtree(sc_id)
            else:
                logger.info(f'State config {sc_id} confirmed for publication')

    def _evaluate_mgrs_tile_set(self, mgrs_set_id, cycle_number, force_publish=False):
        """
        Evaluate the completeness of a given MGRS tile set for a given cycle.

        Args:
            mgrs_set_id: The MGRS set ID to evaluate
            cycle_number: The cycle number to evaluate
            force_publish: Whether to publish state configs even if they have already been published

        Returns:
            The ID of the state config that was created/updated, None if nothing was created or the state config was
            expired
        """
        sc_id = self._get_sc_id(mgrs_set_id, cycle_number)
        expected_track_frames = self.mgrs_track_frame_db.mgrs_set_id_to_track_frames(mgrs_set_id)

        logger.info(f'Evaluating state config {sc_id}')

        self._refresh_index()
        existing_state_config, sc_index = find_state_config(self.es_conn, sc_id, c.MGRS_SET_STATE_CONFIG)

        if not force_publish and existing_state_config.get(c.IS_COMPLETE, False):
            logger.info(f'State config {sc_id} is already complete and will be skipped')
            self._msg(
                f'{mgrs_set_id}${cycle_number} already complete',
                f'State config {sc_id} is already complete and will be skipped'
            )
            return None

        existing_found_track_frames = set(existing_state_config.get(c.FOUND_TRACK_FRAMES, []))
        existing_excluded_track_frames = set(existing_state_config.get(c.EXCLUDED_TRACK_FRAMES, []))

        (
            found_track_frames,
            excluded_track_frames,
            gcov_product_paths,
            polygons,
            start_time,
            end_time
        ) = self._query_gcov(expected_track_frames, cycle_number)

        state_config_updated = ((existing_found_track_frames != set(found_track_frames)) or
                                (existing_excluded_track_frames != set(excluded_track_frames)))

        if not (force_publish or state_config_updated) and existing_state_config.get(c.IS_EXPIRED, False):
            logger.info(f'State config {sc_id} is expired and will be skipped')
            self._msg(
                f'{mgrs_set_id}${cycle_number} expired',
                f'State config {sc_id} is expired and will be skipped'
            )
            return None

        geojson = self.mgrs_track_frame_db.get_geojson_for_mgrs_set_id(mgrs_set_id)

        coverage_area = 0

        for polygon in polygons:
            if isinstance(polygon, MultiPolygon):
                for geom in polygon.geoms:
                    intersection = self.mgrs_track_frame_db.get_polygon_intersection_for_mgrs_set_id(mgrs_set_id,
                                                                                                     geom)

                    if not intersection.is_empty():
                        intersection_area = area_from_polygon(intersection, units='km2')
                        coverage_area += intersection_area
            elif isinstance(polygon, Polygon):
                intersection = self.mgrs_track_frame_db.get_polygon_intersection_for_mgrs_set_id(mgrs_set_id, polygon)

                if not intersection.is_empty():
                    intersection_area = area_from_polygon(intersection, units='km2')
                    coverage_area += intersection_area
            else:
                raise ValueError(f'Unexpected polygon type: {type(polygon)}')

        logger.info(f'Coverage area for state config {sc_id}: {coverage_area:,} km^2')

        if state_config_updated:
            # Create or update SC
            logger.info(f'State config {sc_id} is new or has been updated')
            expired = False
            new_sc, _ = self._create_sc(mgrs_set_id, cycle_number, expected_track_frames,
                                        found_track_frames, excluded_track_frames, gcov_product_paths,
                                        start_time, end_time, coverage_area, geojson=geojson)
        else:
            expiration_time = self._get_state_config_expiration_time(sc_id)
            now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            if expiration_time is not None and now >= expiration_time:
                logger.info(f'State config {sc_id} is expired and will be triggered')
                self._expire_sc(existing_state_config, sc_index, start_time, end_time, geojson=geojson)
                expired = True
            else:
                logger.info(f'State config {sc_id} has not changed and is not yet expired')
                expired = False
            new_sc = None

        n_found = len(found_track_frames)
        n_excluded = len(excluded_track_frames)
        n_expected = len(expected_track_frames)
        missing = sorted(set(expected_track_frames) - (set(found_track_frames) | set(excluded_track_frames)))

        excluded_msg_str = f' (+ {n_excluded}/{n_expected} excluded)' if n_excluded > 0 else ' '

        if expired:
            state = 'expired'
        elif n_found > 0 and (n_found + n_excluded) == n_expected:
            state = 'completed'
        elif n_excluded == n_expected:
            state = 'skipped'
        else:
            state = 'incomplete'

        short_msg = f"{mgrs_set_id}${cycle_number} {state} with {n_found}/{n_expected}{excluded_msg_str}"
        detail_msg = (f"Tile set {mgrs_set_id}${cycle_number}: {state} with {n_found}/{n_expected}{excluded_msg_str} "
                      f"track-frames")

        if missing:
            detail_msg += f', missing: {missing}'

        self._msg(short_msg, detail_msg)

        return new_sc

    def _query_gcov(
            self,
            expected_track_frames,
            cycle_number
    ) -> tuple[list[str], list[str], dict[str, list[str]], list[Union[Polygon, MultiPolygon]], str, str]:
        """
        Query GRQ for GCOVs with a set of track-frames for a given cycle number, filter for valid modes and polarities,
        and gather URLs.

        Args:
            expected_track_frames: List of track-frames (<trk>_<frm>) to query for
            cycle_number: Acquisition cycle to query for

        Returns:
            A tuple consisting of [List of valid track-frames found by the query, list of track-frames excluded
            (invalid mode or polarization), a dictionary mapping https/s3 to the URLs of the valid GCOVs, a list of
            valid GCOV geometries, the acquisition start time of the earliest matching GCOV (valid or not), the
            acquisition end time of the latest matching GCOV (valid or not)
        """
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

        self._refresh_index(c.GCOV_DATASET_ES_PATTERN)

        results = backoff_wrapper(
            self.es_conn.query,
            body=body,
            index=c.GCOV_DATASET_ES_PATTERN
        )

        found_track_frames = []
        excluded_track_frames = set()
        product_paths = {'https': [], 's3': []}
        polygons = []

        # TODO: Temporary (hopefully) kludge to correct and then parse the GRQ location field
        #  should be replaced with a simple from_geojson(json.dumps(location)) once GRQ records
        #  the values correctly
        def _parse_geojson_from_grq_location(location: dict) -> Union[Polygon, MultiPolygon]:
            corrected_type = {
                'point': 'Point',
                'linestring': 'LineString',
                'polygon': 'Polygon',
                'multipoint': 'MultiPoint',
                'multilinestring': 'MultiLineString',
                'multipolygon': 'MultiPolygon',
                'geometrycollection': 'GeometryCollection',
            }.get(location['type'], location['type'])

            logger.info(f'Temp: corrected location GeoJSON type {location["type"]} -> {corrected_type}')
            location['type'] = corrected_type

            return from_geojson(json.dumps(location))

        start_times = []
        end_times = []

        if results:
            for hit in results:
                source = hit.get("_source", {})

                meta = source.get("metadata", {})
                track_frame = meta.get("track_frame")
                if track_frame and track_frame in expected_track_frames:
                    start_times.append(source['starttime'])
                    end_times.append(source['endtime'])
                    if meta['polarization'] not in c.VALID_POLS:
                        excluded_track_frames.add(track_frame)
                        continue
                    if meta['bandwidth_mode'] not in c.VALID_MODES:
                        excluded_track_frames.add(track_frame)
                        continue
                    if track_frame not in found_track_frames:
                        found_track_frames.append(track_frame)
                        # Get the ASF S3 path to the .h5 file (not the HySDS dataset dir URL)
                        product_paths['https'].extend(meta['product_https_paths'])
                        product_paths['s3'].extend(meta['product_s3_paths'])
                        polygons.append(_parse_geojson_from_grq_location(meta['location']))
                    else:
                        logger.warning(f'Ignoring repeated granule for track frame {track_frame}: {source["id"]}')
                else:
                    logger.warning(f'Unexpected track frame: {track_frame} for query {body}')

        found_track_frames.sort()

        excluded_track_frames = list(excluded_track_frames)
        excluded_track_frames.sort()

        return (
            found_track_frames,
            excluded_track_frames,
            product_paths,
            polygons,
            min(start_times),
            max(end_times)
        )

    def _create_sc(self, tile_set_id, cycle_number, expected_track_frames, found_track_frames,
                   excluded_track_frames, product_paths, start_time, end_time, coverage_area, geojson=None):
        """Creates or updates a state config"""
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

        minimum_coverage_area = float(self.settings['DSWX_NI']['MIN_COVERAGE_AREA'])
        sufficient_area = coverage_area >= minimum_coverage_area

        is_complete = len(missing) == 0
        is_skipped = len(excluded) == len(expected) or (is_complete and not sufficient_area)

        if is_complete:
            completeness_reason = f"complete: {coverage_actual}/{coverage_expected} track-frames"
        else:
            completeness_reason = (f"incomplete: {coverage_actual}/{coverage_expected} "
                                   f"track-frames, missing {len(missing)}")

        if len(excluded) > 0:
            completeness_reason += f', excluded {len(excluded)}'

        if is_skipped:
            if len(excluded) == len(expected):
                skipped_reason = 'no valid inputs'
            elif not sufficient_area:
                skipped_reason = f'insufficient coverage area: {coverage_area:,} km^2 vs {minimum_coverage_area:,} km^2'
            else:
                skipped_reason = 'unknown'
        else:
            skipped_reason = ''

        metadata = {
            "id": sc_id,
            c.STATE_CONFIG_TYPE: c.STATE_CONFIG_TYPE,
            c.MGRS_SET_ID: tile_set_id,
            c.CYCLE_NUMBER: cycle_number,
            c.EXPECTED_TRACK_FRAMES: expected,
            c.FOUND_TRACK_FRAMES: found,
            c.EXCLUDED_TRACK_FRAMES: excluded,
            c.MISSING_TRACK_FRAMES: missing,
            c.LAND_OCEAN_FLAG: self.mgrs_track_frame_db.get_lof_for_mgrs_set_id(tile_set_id),
            c.BOUNDING_BOX: self.mgrs_track_frame_db.get_bounding_box_for_mgrs_set_id(tile_set_id),
            c.COVERAGE_AREA: coverage_area,
            c.GCOV_HTTPS_PRODUCT_PATHS: product_paths['https'],
            c.GCOV_S3_PRODUCT_PATHS: product_paths['s3'],
            c.COVERAGE_ACTUAL: coverage_actual,
            c.COVERAGE_EXPECTED: coverage_expected,
            c.IS_COMPLETE: is_complete,
            c.COMPLETENESS_REASON: completeness_reason,
            c.EXPIRATION_DATE: new_expiration_date,
            c.IS_EXPIRED: False,
            c.IS_SKIPPED: is_skipped,
            c.SKIPPED_REASON: skipped_reason,
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
        """
        Expires a state config.

        This sets the is_expired field to true, and sets the is_skipped field to true if no valid GCOVs have been found,
        it also copies the state config to the expired state config collection.
        """
        mgrs_set_id = state_config.get(c.MGRS_SET_ID)
        cycle_number = state_config.get(c.CYCLE_NUMBER)

        sc_id = self._get_sc_id(mgrs_set_id, cycle_number)
        expired_sc_id = self._get_sc_id(mgrs_set_id, cycle_number, expired=True)

        metadata = state_config

        minimum_coverage_area = float(self.settings['DSWX_NI']['MIN_COVERAGE_AREA'])
        sufficient_area = metadata[c.COVERAGE_AREA] >= minimum_coverage_area

        metadata[c.IS_EXPIRED] = True
        metadata[c.IS_SKIPPED] = len(metadata[c.FOUND_TRACK_FRAMES]) == 0 or not sufficient_area

        if metadata[c.IS_SKIPPED]:
            if len(metadata[c.FOUND_TRACK_FRAMES]) == 0:
                skipped_reason = 'no valid inputs'
            elif not sufficient_area:
                skipped_reason = (f'insufficient coverage area: {metadata[c.COVERAGE_AREA]:,} km^2 '
                                  f'vs {minimum_coverage_area:,} km^2')
            else:
                skipped_reason = 'unknown'
            metadata[c.SKIPPED_REASON] = skipped_reason

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
            body={
                "script": {
                    "source": "ctx._source.metadata.is_expired = true; "
                              "ctx._source.metadata.is_skipped = params.skipped",
                    "lang": "painless",
                    "params": {
                        "skipped": metadata[c.IS_SKIPPED]
                    }
                },
            },
            refresh=True
        )

        logger.info(f'Marked state config {sc_id} as expired')

        metadata['id'] = expired_sc_id

        create_state_config_dataset(
            dataset_name=expired_sc_id,
            metadata=metadata,
            start_time=start_time,
            end_time=end_time,
            dataset_type=c.MGRS_SET_EXPIRED_STATE_CONFIG,
            geojson=geojson,
        )

        logger.info(f'Copied expired state config to expired index as {expired_sc_id}')

        return sc_id, metadata

    def _get_state_config_expiration_time(self, sc_id):
        """
        Convenience method to get the expiration time of a state config.

        Args:
            sc_id: The ID of the state config

        Returns:
            The expiration time of the state config, or None if it does not exist
        """

        self._refresh_index()

        existing_document = backoff_wrapper(
            self.es_conn.search_by_id,
            id=sc_id,
            index=c.MGRS_SET_STATE_CONFIG_ES_PATTERN,
            ignore=[404]
        )

        if existing_document.get("found", False):
            return existing_document.get('_source', {}).get('expiration_time')
        return None

    def _get_state_config_state(self, sc_id):
        """
        Convenience method to get the state flags of a state config.

        Args:
            sc_id: The ID of the state config

        Returns:
            A tuple of the is_complete, is_expired, is_skipped flags of the state config
        """
        self._refresh_index()

        existing_document = backoff_wrapper(
            self.es_conn.search_by_id,
            id=sc_id,
            index=c.MGRS_SET_STATE_CONFIG_ES_PATTERN,
            ignore=[404]
        )

        if existing_document.get("found", False):
            metadata = existing_document.get('_source', {}).get('metadata', {})
            return (metadata.get(c.IS_COMPLETE, False),
                    metadata.get(c.IS_EXPIRED, False),
                    metadata.get(c.IS_SKIPPED, False))
        return False, False, False

    @staticmethod
    def _get_sc_id(mgrs_set_id, cycle_number, expired=False):
        """Determine the state config ID associated with a given MGRS Set ID and acquisition cycle."""
        if not expired:
            return f'dswx_ni_{mgrs_set_id}-{cycle_number}-state-config'
        else:
            return f'dswx_ni_{mgrs_set_id}-{cycle_number}-expired-state-config'


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
