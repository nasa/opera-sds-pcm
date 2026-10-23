"""
DSWx-S1 MGRS Evaluator

Triggered by RTC production or manual batch ingest or on-demand re-evaluation.

Queries ES for all RTC products matching the MGRS tile sets covered by the input,
creating a DSWX-S1 State Config for each.
"""
import json
import math
import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Union

from dateutil.parser import parse as dt_parse
from opensearchpy.helpers import scan
from shapely import from_geojson
from shapely.geometry import Polygon, MultiPolygon
from shapely.io import to_geojson

from data_subscriber import es_conn_util
from data_subscriber.cslc.disp_s1_state_config import find_state_config
from data_subscriber.rtc import rtc_state_config_constants as c
from data_subscriber.rtc.mgrs_bursts_collection_db_client import (cached_load_mgrs_burst_db,
                                                                  burst_id_to_mgrs_set_ids,
                                                                  mgrs_set_id_to_expected_bursts,
                                                                  get_polygon_for_mgrs_set_id,
                                                                  intersect_polygon_with_mgrs_set,
                                                                  get_bounding_box_for_mgrs_set_id,
                                                                  get_lof)
from data_subscriber.rtc.mgrs_bursts_collection_db_client import mapping_burst_id_to_product_burst_id as burst_id_upper
from opera_commons.logger import get_logger
from rtc_utils import (dedupe_grq_rtc_docs, determine_acquisition_cycle_for_rtc_granule, granule_id_to_burst_id,
                       granule_id_to_acquisition_timestamp, granule_id_to_sensor)
from util.common_util import backoff_wrapper, create_info_message_files, create_state_config_dataset
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.geo_util import area_from_polygon
from util.job_util import is_running_outside_verdi_worker_context as is_not_verdi
from util.job_util import supply_job_id

logger = get_logger()


class RTCMgrsEvaluator:
    """Evaluates tile set completeness"""

    def __init__(self, es_conn):
        self.mgrs_db = cached_load_mgrs_burst_db(filter_land=False)
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
            index_pattern = c.DSWX_S1_STATE_CONFIG_ES_PATTERN

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
        Run the DSWx-S1 state config evaluation logic on a given input dataset. The dataset can be an existing state
        config, or a batch of RTC-S1 granules. The former will evaluate for state config expiration, the
        latter will create/update existing state configs.

        Args:
            input_dataset_id: The identifier of the input dataset
            metadata: The product metadata of the input dataset
            dataset_type: The HySDS dataset id of the input dataset type (dswx_s1-state-config or L2_RTC_S1_BATCH)
            force_publish: If true, republish the state config, even if it is already marked as complete or expired.
        """
        sc_datasets = []

        if dataset_type == c.DSWX_S1_STATE_CONFIG:
            mgrs_set_id = metadata.get(c.MGRS_SET_ID)
            cycle_number = metadata.get(c.CYCLE_NUMBER)
            sensor = metadata.get(c.SENSOR)

            logger.info(f'DSWx-S1 MGRS set re-evaluation triggered: {mgrs_set_id=}')
            self._msg(
                f're-eval {mgrs_set_id}',
                f'DSWx-S1 MGRS set re-evaluation triggered: {mgrs_set_id=}'
            )

            sc = self._evaluate_mgrs_tile_set(mgrs_set_id, cycle_number, sensor, force_publish=force_publish)

            if sc:
                sc_datasets.append(sc)
        else:
            if dataset_type == c.RTC_BATCH:
                if (metadata.get(c.PROCESSING_MODE) == 'reprocessing' and
                        not self.settings.get('DSWX_S1', {}).get('ENABLE_REPROCESSING_FROM_RTC', False)):
                    logger.info(f'RTC batch {input_dataset_id} is from reprocessing mode and DSWx reprocessing is not '
                                f'enabled. Set setting DSWX_S1.REPROCESSING_FROM_RTC to enable reprocessing.')
                    self._msg(
                        'Skipping reprocessed RTC batch',
                        f'RTC batch {input_dataset_id} is from reprocessing mode and DSWx reprocessing is not '
                        f'enabled. Set setting DSWX_S1.REPROCESSING_FROM_RTC to enable reprocessing.'
                    )
                    input_rtcs = None
                    filter_mgrs_set_ids = None
                else:
                    logger.info(f'Evaluating RTC batch {input_dataset_id} ({metadata["count"]:,} RTC inputs)')
                    input_rtcs = metadata[c.RTC_IDS]
                    filter_mgrs_set_ids = metadata.get(c.MGRS_SET_IDS)
            elif dataset_type == c.RTC_DS:
                logger.info(f'Evaluating single RTC {input_dataset_id}')
                input_rtcs = [input_dataset_id]
                filter_mgrs_set_ids = None
            else:
                raise TypeError(f'Unknown dataset type: {dataset_type}')

            if input_rtcs:
                matched_state_configs = set()

                for native_id in input_rtcs:
                    burst_id = granule_id_to_burst_id(native_id)
                    sensor = granule_id_to_sensor(native_id)
                    acquisition_time = granule_id_to_acquisition_timestamp(native_id)

                    mgrs_set_ids = burst_id_to_mgrs_set_ids(self.mgrs_db, burst_id)
                    matched = False

                    logger.info(f'Determining state configs for {native_id}, '
                                f'{burst_id=} contained in {len(mgrs_set_ids)} MGRS sets, {sensor=}, {acquisition_time=}')

                    for mgrs_set_id in mgrs_set_ids:
                        if filter_mgrs_set_ids and mgrs_set_id not in filter_mgrs_set_ids:
                            logger.info(f'Skipping MGRS set {mgrs_set_id} as it is not in the configured filter set')
                            continue

                        if get_lof(self.mgrs_db, mgrs_set_id) == 'water':
                            logger.info(f'MGRS set {mgrs_set_id} is over water and will be skipped')
                            continue

                        cycle_number = determine_acquisition_cycle_for_rtc_granule(native_id)

                        expected_tracks = self.mgrs_db[self.mgrs_db['mgrs_set_id'] == mgrs_set_id].iloc[0]['orbits']

                        if len(expected_tracks) > 1 and (max(expected_tracks) - min(expected_tracks)) > 1:
                            if burst_id.split('-')[0] == 'T001':
                                # Just incremented cycles within the tile set, use pre-increment cycle number
                                cycle_number = cycle_number - 1

                        matched_state_configs.add((mgrs_set_id, cycle_number, sensor))
                        matched = True

                    if not matched:
                        logger.info(f'Burst {burst_id} belongs to no tile sets with land coverage and will be skipped')
                        self._msg(
                            f'no land coverage for {burst_id}. Skipping',
                            f'Burst {burst_id} belongs to no tile sets with land coverage and will be skipped'
                        )

                logger.info(f'Identified {len(matched_state_configs):,} state configs to evaluate')
                self._msg(
                    f'evaluating {len(matched_state_configs):,} tile sets',
                    f'Identified {len(matched_state_configs):,} state configs to evaluate'
                )

                for mgrs_set_id, cycle_number, sensor in matched_state_configs:
                    sc = self._evaluate_mgrs_tile_set(mgrs_set_id, cycle_number, sensor, force_publish=force_publish)

                    if sc:
                        sc_datasets.append(sc)

        logger.info('Finished state config evaluation(s)')

        if len(sc_datasets) > 0:
            logger.info('Confirming state config datasets should be published (ie, they weren\'t already published '
                        'by a parallel evaluator)')
            self._confirm_state_config_publications(sc_datasets)
        else:
            logger.info('No new or updated non-expired state configs to publish')

        if dataset_type == c.RTC_BATCH and not is_not_verdi():
            job_id = supply_job_id()
            logger.info(f'Marking RTC batch {input_dataset_id} as evaluated by job {job_id}')

            result = backoff_wrapper(
                self.es_conn.es.update_by_query,
                index=c.RTC_BATCH_DATASET_ES_PATTERN,
                body={
                    "script": {
                        "source": "ctx._source.metadata.evaluator_job_id = params['job_id']",
                        "params": {
                            "job_id": str(job_id),
                        },
                        "lang": "painless"
                    },
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "_id": input_dataset_id
                                    }
                                }
                            ]
                        }
                    }
                },
                refresh=True
            )

            if result["updated"] == 0:
                logger.error(f"Failed to mark RTC batch as evaluated: {result}")
                self._msg(
                    'could not mark RTC batch as evaluated',
                    f'Failed to mark RTC batch as evaluated: {result}'
                )
            else:
                logger.info(f"Successfully marked RTC batch as evaluated by job {job_id}")

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

    def _evaluate_mgrs_tile_set(self, mgrs_set_id, cycle_number, sensor, force_publish=False):
        """
        Evaluate the completeness of a given MGRS tile set for a given cycle.

        Args:
            mgrs_set_id: The MGRS set ID to evaluate
            cycle_number: The cycle number to evaluate
            sensor: The sensor to evaluate
            force_publish: Whether to publish state configs even if they have already been published

        Returns:
            The ID of the state config that was created/updated, None if nothing was created or the state config was
            expired
        """
        sc_id = self._get_sc_id(mgrs_set_id, cycle_number, sensor)
        expected_bursts = [
            burst_id_upper(b) for b in mgrs_set_id_to_expected_bursts(self.mgrs_db, mgrs_set_id)
        ]

        logger.info(f'Evaluating state config {sc_id}')

        self._refresh_index()
        existing_state_config, sc_index = find_state_config(self.es_conn, sc_id, c.DSWX_S1_STATE_CONFIG)

        if not force_publish and existing_state_config.get(c.IS_COMPLETE, False):
            logger.info(f'State config {sc_id} is already complete and will be skipped')
            self._msg(
                f'{mgrs_set_id}${cycle_number}${sensor} already complete',
                f'State config {sc_id} is already complete and will be skipped'
            )
            return None

        existing_found_bursts = set(existing_state_config.get(c.FOUND_BURSTS, []))
        existing_excluded_bursts = set(existing_state_config.get(c.EXCLUDED_BURSTS, []))
        existing_jobs = existing_state_config.get(c.TRIGGERED_JOBS, [])

        expected_tracks = self.mgrs_db[self.mgrs_db['mgrs_set_id'] == mgrs_set_id].iloc[0]['orbits']

        if not(len(expected_tracks) > 1 and (max(expected_tracks) - min(expected_tracks)) > 1):
            (
                found_bursts,
                excluded_bursts,
                product_paths,
                polygons,
                start_time,
                end_time
            ) = self._query_rtc(expected_bursts, cycle_number, sensor)

            if not found_bursts:
                raise RuntimeError(f'RTC query returned no bursts for {mgrs_set_id}. This should not happen and '
                                   f'indicates an unexpected state for GRQ.')
        else:
            # We're in a special case where the acquisition cycle increments within the tile set's coverage, so we'll
            #  have the last track with cycle n and track 1 with cycle n+1. The SC will be pinned at the lower cycle
            #  number, so we have to explicitly query track 1 bursts with cycle number n+1

            found_bursts = []
            excluded_bursts = []
            product_paths = {}
            polygons = []
            start_times = []
            end_times = []

            if len(expected_tracks) > 2:
                raise ValueError(f'Unexpected: tile set {mgrs_set_id} has '
                                 f'more than 2 expected tracks: {expected_tracks}')

            for track in expected_tracks:
                if track == max(expected_tracks):
                    query_cycle = cycle_number
                else:
                    query_cycle = cycle_number + 1

                (
                    query_found_bursts,
                    query_excluded_bursts,
                    query_product_paths,
                    query_polygons,
                    query_start_time,
                    query_end_time
                ) = self._query_rtc(
                    [b for b in expected_bursts if b.startswith(f'T{track:03d}')],
                    query_cycle,
                    sensor
                )

                if query_found_bursts:
                    found_bursts.extend(query_found_bursts)
                    excluded_bursts.extend(query_excluded_bursts)
                    product_paths.update(query_product_paths)
                    polygons.extend(query_polygons)
                    start_times.append(query_start_time)
                    end_times.append(query_end_time)
                else:
                    logger.info(f'No bursts found for track {track}')

            if not found_bursts:
                raise RuntimeError(f'RTC queries returned no bursts for {mgrs_set_id}. This should not happen and '
                                   f'indicates an unexpected state for GRQ.')

            start_time = min(start_times)
            end_time = max(end_times)

            # Verify the tile set is covered by temporally close RTCs. If the cycle tweak doesn't work, this check
            # should fail eventually when a set is covered by trk 1 RTCs 12 days earlier than the others
            # TODO: Remove once this is validated
            if end_time - start_time > timedelta(days=1):
                logger.critical(f'Tile set coverage not temporally close:\n{json.dumps({
                    'found_bursts': found_bursts,
                    'excluded_bursts': excluded_bursts,
                    'product_paths': product_paths,
                    "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "end_time": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }, indent=2)}')
                raise Exception('Tile set coverage not temporally close (see logs)')

        state_config_updated = ((existing_found_bursts != set(found_bursts)) or
                                (existing_excluded_bursts != set(excluded_bursts)))

        if not (force_publish or state_config_updated) and existing_state_config.get(c.IS_EXPIRED, False):
            logger.info(f'State config {sc_id} is expired and will be skipped')
            self._msg(
                f'{mgrs_set_id}${cycle_number}${sensor} expired',
                f'State config {sc_id} is expired and will be skipped'
            )
            return None

        geojson = json.loads(to_geojson(get_polygon_for_mgrs_set_id(self.mgrs_db, mgrs_set_id)))

        coverage_area = 0

        for polygon in polygons:
            if polygon is None:
                logger.warning('Unable to compute coverage as one or more RTCs does not have geo metadata')
                coverage_area = float('nan')
                break
            elif isinstance(polygon, MultiPolygon):
                for geom in polygon.geoms:
                    intersection = intersect_polygon_with_mgrs_set(geom, self.mgrs_db, mgrs_set_id)

                    if not intersection.is_empty:
                        intersection_area = area_from_polygon(intersection, units='km2')
                        coverage_area += intersection_area
            elif isinstance(polygon, Polygon):
                intersection = intersect_polygon_with_mgrs_set(polygon, self.mgrs_db, mgrs_set_id)

                if not intersection.is_empty:
                    intersection_area = area_from_polygon(intersection, units='km2')
                    coverage_area += intersection_area
            else:
                raise ValueError(f'Unexpected polygon type: {type(polygon)}')

        coverage_area = round(coverage_area, 3)

        logger.info(f'Coverage area for state config {sc_id}: {coverage_area:,} km^2')

        if state_config_updated:
            if existing_jobs and set(existing_jobs[-1].get('triggered_bursts', [])) == set(found_bursts):
                logger.info(f'State config {sc_id} appears to have already been triggered with this burst set')
                self._msg(
                    f'SC {sc_id} deduped trigger',
                    f'State config {sc_id} appears to have already been triggered with this burst set'
                )
                return None

            # Create or update SC
            logger.info(f'State config {sc_id} is new or has been updated')
            expired = False
            new_sc, new_sc_meta = self._create_sc(mgrs_set_id, cycle_number, sensor, expected_bursts,
                                                  found_bursts, excluded_bursts, product_paths,
                                                  start_time, end_time, coverage_area, geojson=geojson,
                                                  triggered_jobs=existing_jobs)
        else:
            expiration_time = self._get_state_config_expiration_time(sc_id)
            now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            if expiration_time is not None and now >= expiration_time:
                logger.info(f'State config {sc_id} is expired and will be triggered')
                self._expire_sc(existing_state_config, sc_index, sensor, start_time, end_time, geojson=geojson)
                expired = True
            else:
                logger.info(f'State config {sc_id} has not changed and is not yet expired')
                expired = False
            new_sc = None
            new_sc_meta = None

        n_found = len(found_bursts)
        n_excluded = len(excluded_bursts)
        n_expected = len(expected_bursts)
        missing = sorted(set(expected_bursts) - (set(found_bursts) | set(excluded_bursts)))

        excluded_msg_str = f' (+ {n_excluded}/{n_expected} excluded)' if n_excluded > 0 else ' '

        if expired:
            state = 'expired'
        elif n_found > 0 and (n_found + n_excluded) == n_expected:
            state = 'completed'
        elif n_excluded == n_expected:
            state = 'skipped'
        else:
            state = 'incomplete'
            if new_sc_meta and new_sc_meta.get(c.IS_RUNNABLE):
                state += ' but runnable'

        short_msg = (f"{mgrs_set_id}${cycle_number}${sensor} {state} with {n_found}/{n_expected} "
                     f"[{coverage_area:,} km^2]{excluded_msg_str}")
        detail_msg = (f"Tile set {mgrs_set_id}${cycle_number}${sensor}: {state} with {n_found}/{n_expected}"
                      f"{excluded_msg_str} bursts [{coverage_area:,} km^2]")

        if missing:
            detail_msg += f', missing: {missing}'

        self._msg(short_msg, detail_msg)

        return new_sc

    def _query_rtc(
            self,
            expected_burst_ids,
            cycle_number,
            sensor
    ) -> tuple[list[str], list[str], dict[str, list[str]], list[Union[Polygon, MultiPolygon]], datetime, datetime]:
        """
        Query GRQ for RTCs with a set of burst IDs for a given cycle number, filter for valid modes and polarities,
        and gather URLs.

        Args:
            expected_burst_ids: List of burst IDs to query for
            cycle_number: Acquisition cycle to query for
            sensor: Sensor to query for

        Returns:
            A tuple consisting of [List of valid bursts found by the query, list of bursts excluded
            (invalid mode or polarization), a dictionary mapping burst IDs to the S3 URLs of the valid RTCs, a list of
            valid RTC geometries, the acquisition start time of the earliest matching RTC (valid or not), the
            acquisition end time of the latest matching RTC (valid or not)
        """
        body = {
            "query": {
                "bool": {
                    "must": [
                        # Dataset must be RTC
                        {"term": {"dataset_type.keyword": "L2_RTC_S1"}},
                        # Either met.Files[*].burst_id in expected OR met.burst_id in expected
                        {"bool": {
                            "should": [
                                {"terms": {"metadata.burst_id.keyword": expected_burst_ids}},
                                {"terms": {"metadata.Files.burst_id.keyword": expected_burst_ids}},
                            ],
                            "minimum_should_match": 1
                        }},
                        # Either met.Files[*].sensor = sensor OR met.sensor = sensor
                        {"bool": {
                            "should": [
                                {"term": {"metadata.sensor.keyword": sensor}},
                                {"term": {"metadata.Files.sensor.keyword": sensor}},
                            ],
                            "minimum_should_match": 1
                        }},
                    ],
                    # "filter": [
                    #     {"term": {"metadata.acquisition_cycle": cycle_number}},
                    # ]

                    # ACQ cycle may not exist in older/backfilled records, so we have to get docs without
                    #  a cycle and filter manually. #TODO: Ensure all these meta fields get into backfilled docs
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    # met.acq_cyc = cycle
                                    {"term": {"metadata.acquisition_cycle": cycle_number}},
                                    # met.acq_cyc not defined, doc will need to be manually filtered
                                    {
                                        "bool": {
                                            "must_not": {
                                                "exists": {"field": "metadata.acquisition_cycle"}
                                            }
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            },
            "size": len(expected_burst_ids) * 2,
        }

        self._refresh_index(c.RTC_DATASET_ES_PATTERN)

        results = []

        for hit in scan(self.es_conn.es, query=body, index=c.RTC_DATASET_ES_PATTERN):
            if 'acquisition_cycle' in hit.get('_source', {}).get('metadata', {}):
                if hit['_source']['metadata']['acquisition_cycle'] != cycle_number:
                    logger.error(f'Returned doc {hit["_id"]} despite having mismatched acq cycle. '
                                 f'Expected: {cycle_number} got {hit["_source"]["metadata"]["acquisition_cycle"]}. '
                                 f'THIS SHOULD NOT HAPPEN. Doc will be dropped')
                    continue
                else:
                    results.append(hit)
            else:
                if determine_acquisition_cycle_for_rtc_granule(hit['_source']['id']) == cycle_number:
                    results.append(hit)

        results = dedupe_grq_rtc_docs(results)

        found_bursts = set()
        excluded_bursts = set()  # This is no longer used and can be removed in the future
        product_paths = {}
        polygons = []

        # TODO: Temporary (hopefully) kludge to correct and then parse the GRQ location field
        #  should be replaced with a simple from_geojson(json.dumps(location)) once GRQ records
        #  the values correctly (HC-644)
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

            geom = from_geojson(json.dumps(location))

            if not isinstance(geom, (Polygon, MultiPolygon)):
                raise TypeError(type(geom))
            return geom

        times = []

        if results:
            for hit in sorted(
                    results,
                    key=lambda h: h.get("_source", {}).get("metadata", {}).get("burst_id") or
                                  granule_id_to_burst_id(h.get("_source", {})['id'])
            ):
                source = hit.get("_source", {})

                meta = source.get("metadata", {})
                burst_id = meta.get("burst_id") or granule_id_to_burst_id(source['id'])

                if burst_id and burst_id in expected_burst_ids:
                    times.append(dt_parse(meta.get('acquisition_ts') or
                                          granule_id_to_acquisition_timestamp(source['id'])))

                    if burst_id not in found_bursts:
                        found_bursts.add(burst_id)

                        urls = source.get('daac_product_file_urls') or meta['product_s3_paths']

                        product_paths[burst_id] = [
                            url for url in urls if url.endswith('.tif') or url.endswith('.h5')
                        ]
                        if 'location' in source:
                            polygons.append(_parse_geojson_from_grq_location(source['location']))
                        else:
                            polygons.append(None)
                    else:
                        logger.warning(f'Ignoring repeated granule for burst {burst_id}: {source["id"]}')
                else:
                    logger.warning(f'Unexpected burst ID {burst_id} for query {body}')

        return (
            list(sorted(found_bursts)),
            list(sorted(excluded_bursts)),
            product_paths,
            polygons,
            min(times) if times else None,
            max(times) if times else None,
        )

    def _create_sc(self, tile_set_id, cycle_number, sensor, expected_bursts, found_bursts, excluded_bursts,
                   product_paths, start_time, end_time, coverage_area, geojson=None, triggered_jobs=None):
        """Creates or updates a state config"""
        sc_id = self._get_sc_id(tile_set_id, cycle_number, sensor)

        grace_period = self.settings['DSWX_S1_COLLECTION_GRACE_PERIOD_MINUTES']
        new_expiration_time = (datetime.now(tz=timezone.utc) + timedelta(minutes=grace_period))
        new_expiration_date = new_expiration_time.strftime("%Y%m%d")
        new_expiration_time = new_expiration_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        if triggered_jobs is None:
            triggered_jobs = []

        expected = sorted(expected_bursts)
        found = sorted(found_bursts)
        excluded = sorted(excluded_bursts)
        missing = sorted(set(expected) - (set(found) | set(excluded)))
        coverage_actual = len(found) + len(excluded)
        coverage_expected = len(expected)

        # Compute the minimum coverage bursts from settings
        coverage_target = self.settings.get('DSWX_S1_COVERAGE_TARGET')
        coverage_minimum_setting = self.settings.get('DSWX_S1_MINIMUM_NUMBER_OF_BURSTS_REQUIRED')

        try:
            if coverage_target is not None and coverage_minimum_setting is not None:
                raise ValueError('Cannot set both DSWX_S1_COVERAGE_TARGET and '
                                 'DSWX_S1_MINIMUM_NUMBER_OF_BURSTS_REQUIRED')

            if coverage_target is not None:
                coverage_target = int(coverage_target)
                if not(0 <= coverage_target <= 100):
                    raise ValueError('DSWX_S1_COVERAGE_TARGET must be between 0 and 100')

                minimum_burst_count = int(math.ceil(coverage_expected * (float(coverage_target) / 100.0)))
            elif coverage_minimum_setting is not None:
                if coverage_minimum_setting < 1:
                    raise ValueError('DSWX_S1_MINIMUM_NUMBER_OF_BURSTS_REQUIRED must be >= 1')
                minimum_burst_count = int(coverage_minimum_setting)
            else:
                minimum_burst_count = coverage_expected
        except Exception as e:
            logger.error(f"Failed to determine minimum burst count ({e}). Using full set as minimum.")
            minimum_burst_count = coverage_expected

        logger.info(f'Computed minimum burst count for tile set {tile_set_id}: {minimum_burst_count}')

        minimum_coverage_area = float(self.settings.get('DSWX_S1', {}).get('MIN_COVERAGE_AREA', 0))
        if not math.isnan(coverage_area):
            sufficient_area = coverage_area >= minimum_coverage_area
        else:
            logger.warning('SC coverage area is NaN, so skipping area check')
            sufficient_area = True

        lof = get_lof(self.mgrs_db, tile_set_id)

        is_runnable = coverage_actual >= minimum_burst_count and sufficient_area

        is_complete = len(missing) == 0
        is_skipped = len(excluded) == len(expected) or (is_complete and not sufficient_area) or lof == 'water'

        if is_complete:
            completeness_reason = f"complete: {coverage_actual}/{coverage_expected} bursts"
        else:
            completeness_reason = (f"incomplete: {coverage_actual}/{coverage_expected} "
                                   f"bursts, missing {len(missing)}")

        if len(excluded) > 0:
            completeness_reason += f', excluded {len(excluded)}'

        if is_skipped:
            if len(excluded) == len(expected):
                skipped_reason = 'no valid inputs'
            elif lof == 'water':
                skipped_reason = 'tile set is over water'
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
            c.SENSOR: sensor,
            c.EXPECTED_BURSTS: expected,
            c.FOUND_BURSTS: found,
            c.EXCLUDED_BURSTS: excluded,
            c.MISSING_BURSTS: missing,
            c.LAND_OCEAN_FLAG: lof,
            c.BOUNDING_BOX: get_bounding_box_for_mgrs_set_id(self.mgrs_db, tile_set_id),
            c.COVERAGE_AREA: coverage_area,
            c.RTC_S3_PRODUCT_PATHS: product_paths,
            c.COVERAGE_ACTUAL: coverage_actual,
            c.COVERAGE_EXPECTED: coverage_expected,
            c.IS_COMPLETE: is_complete,
            c.COMPLETENESS_REASON: completeness_reason,
            c.IS_RUNNABLE: is_runnable,
            c.EXPIRATION_DATE: new_expiration_date,
            c.IS_EXPIRED: False,
            c.IS_SKIPPED: is_skipped,
            c.SKIPPED_REASON: skipped_reason,
            c.TRIGGERED_COUNT: len(triggered_jobs),
            c.TRIGGERED_JOBS: triggered_jobs,
        }

        # Remove existing dataset dir if present (will be recreated)
        if os.path.isdir(sc_id):
            shutil.rmtree(sc_id)

        logger.info(f"Creating state config: {sc_id} (coverage={coverage_actual}/{coverage_expected}, {is_complete=})")

        create_state_config_dataset(
            dataset_name=sc_id,
            metadata=metadata,
            start_time=start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time=end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            expiration_time=new_expiration_time,
            dataset_type=c.DSWX_S1_STATE_CONFIG,
            geojson=geojson,
        )

        return sc_id, metadata

    def _expire_sc(self, state_config, sc_index, sensor, start_time, end_time, geojson=None):
        """
        Expires a state config.

        This sets the is_expired field to true, and sets the is_skipped field to true if no valid RTCs have been found,
        it also copies the state config to the expired state config collection.
        """
        mgrs_set_id = state_config.get(c.MGRS_SET_ID)
        cycle_number = state_config.get(c.CYCLE_NUMBER)

        sc_id = self._get_sc_id(mgrs_set_id, cycle_number, sensor)
        expired_sc_id = self._get_sc_id(mgrs_set_id, cycle_number, sensor, expired=True)

        metadata = state_config

        minimum_coverage_area = float(self.settings.get('DSWX_S1', {}).get('MIN_COVERAGE_AREA', 0))
        if not math.isnan(metadata[c.COVERAGE_AREA]):
            sufficient_area = metadata[c.COVERAGE_AREA] >= minimum_coverage_area
        else:
            logger.warning('SC coverage area is NaN, so skipping area check')
            sufficient_area = True

        lof = metadata[c.LAND_OCEAN_FLAG]

        metadata[c.IS_EXPIRED] = True
        metadata[c.IS_SKIPPED] = len(metadata[c.FOUND_BURSTS]) == 0 or not sufficient_area or lof == 'water'

        if metadata[c.IS_SKIPPED]:
            if len(metadata[c.FOUND_BURSTS]) == 0:
                skipped_reason = 'no valid inputs'
            elif lof == 'water':
                skipped_reason = 'tile set is over water'
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
        result = backoff_wrapper(
            self.es_conn.update_document,
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

        logger.info(f'Marked state config {sc_id} as expired: {result}')

        metadata['id'] = expired_sc_id

        create_state_config_dataset(
            dataset_name=expired_sc_id,
            metadata=metadata,
            start_time=start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time=end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            dataset_type=c.DSWX_S1_EXPIRED_STATE_CONFIG,
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
            index=c.DSWX_S1_STATE_CONFIG_ES_PATTERN,
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
            index=c.DSWX_S1_STATE_CONFIG_ES_PATTERN,
            ignore=[404]
        )

        if existing_document.get("found", False):
            metadata = existing_document.get('_source', {}).get('metadata', {})
            return (
                metadata.get(c.IS_COMPLETE, False),
                metadata.get(c.IS_EXPIRED, False),
                metadata.get(c.IS_SKIPPED, False)
            )
        return False, False, False

    @staticmethod
    def _get_sc_id(mgrs_set_id, cycle_number, sensor, expired=False):
        """Determine the state config ID associated with a given MGRS Set ID, acquisition cycle and sensor."""
        if not expired:
            return f'dswx_s1_{mgrs_set_id}-{cycle_number}-{sensor}-state-config'
        else:
            return f'dswx_s1_{mgrs_set_id}-{cycle_number}-{sensor}-expired-state-config'


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

    evaluator = RTCMgrsEvaluator(es_conn)
    evaluator.evaluate(input_dataset_id, metadata, dataset_type, force_publish=force_publish)


if __name__ == "__main__":
    evaluate()
