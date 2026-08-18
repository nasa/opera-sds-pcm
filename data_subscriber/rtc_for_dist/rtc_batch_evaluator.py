import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from os.path import basename
from typing import Union

import dateutil
from more_itertools import one, first

from data_subscriber.cmr import Provider
from data_subscriber.cslc_utils import save_blocked_download_job, parse_r2_product_file_name
from data_subscriber.dist_s1_utils import (PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD)
from dist_s1.dataset_util import create_dataset, create_ds_dataset_json, write_ds_dataset_json, write_ds_met_json
from dist_s1.forward_state_config_dao import fix_batch_id
from dist_s1.state_config_service import state_configs_by_batch_id
from rtc_utils import rtc_product_file_regex
from util.job_submitter import try_submit_mozart_job


@dataclass
class Args:
    # used directly in RtcBatchEvaluator
    job_queue: str = "opera-job_worker-rtc_for_dist_data_download"
    proc_mode: str = "forward"

    # used for download job submission


    # required download job params
    endpoint: str = "OPS"
    transfer_protocol: str = "s3"
    # proc_mode: str
    provider: str = Provider.ASF_RTC

    # download job params with fallbacks. i.e. optional
    smoke_run: bool = False
    dry_run: bool = False
    use_temporal: bool = False
    chunk_size: int = 1
    release_version: str = None

class RtcBatchEvaluator:
    def __init__(
            self,
            logger=None,
            download_batch_id_to_k_granules=None,
            batch_id_to_current_granules=None,
            download_batch_id_to_job_submittable=None,
            args: Args = Args(),
            dist_dependency=None,
            es_conn=None,
            settings=None,
    ):
        self.logger = logger
        self.download_batch_id_to_k_granules = download_batch_id_to_k_granules
        self.batch_id_to_current_granules = batch_id_to_current_granules
        self.download_batch_id_to_job_submittable = download_batch_id_to_job_submittable
        self.args = args
        self.dist_dependency = dist_dependency
        self.es_conn = es_conn
        self.settings = settings

        # state objects / result of evaluation
        self.usable_batch_id_to_current_urls_map = {}
        self._unusable_batch_id_to_current_urls_map = {}
        self.submittable_batch_id_to_current_urls_map = {}
        self._unsubmittable_batch_id_to_current_urls_map = {}

        self.download_job_submitter = DownloadJobSubmitter(logger=logger, args=args, dist_dependency=dist_dependency, es_conn=es_conn, settings=settings)

    def evaluate(self, total_granules, query_timerange):
        # return self.rtc_batch_evaluator.evaluate(total_granules, query_timerange)
        def add_filtered_urls(granule, filtered_urls: list, polarization_preference: Union[set, None] =None):
            """
            Updates the filtered_urls list, using the given granule.

            :param granule: the granule whose filtered_urls should be filtered.
            :param filtered_urls: the output list.
            :param polarization_preference: the polarizations preference.

            :return: either the polarizations preference, or the most common polarizations detected. Never returns single polarizations.
            """
            self.logger.debug(f'add_filtered_urls: {polarization_preference=} {granule["granule_id"]}')
            if not granule.get("filtered_urls"):
                self.logger.error(f'No URLs to filter. {granule.get("filtered_urls")}')
                return None

            # ignore single polarizations. declared polarization
            if len(polarization := granule.get("polarization", [])) == 1:
                self.logger.info(f'single polarization detected. Ignoring. {polarization=}')
                return None

            # ignore single polarizations. effective polarization
            polarizations = set()
            for filter_url in granule.get("filtered_urls"):
                if filter_url.endswith("VV.tif"):
                    polarizations.add("VV")
                if filter_url.endswith("VH.tif"):
                    polarizations.add("VH")
                if filter_url.endswith("HH.tif"):
                    polarizations.add("HH")
                if filter_url.endswith("HV.tif"):
                    polarizations.add("HV")
            if len(polarizations) == 1:
                self.logger.info(f'single effective polarization detected. Ignoring. {polarizations=}')
                return None

            polarizations = []
            for filter_url in granule.get("filtered_urls"):
                if filter_url.endswith("VV.tif"):
                    polarizations.append(frozenset({"VV", "VH"}))
                if filter_url.endswith("HH.tif"):
                    polarizations.append(frozenset({"HH", "HV"}))

            most_common_polarization = Counter(polarizations).most_common(1)
            self.logger.debug(f'most_common_polarization={most_common_polarization[0][0]}')

            if polarization_preference and most_common_polarization:
                if polarization_preference != most_common_polarization[0][0]:
                    self.logger.info(f"Polarization switch detected. {polarization_preference=} detected_polarization={most_common_polarization[0][0]}")

            self.logger.info(f"Pre-filter {len(filtered_urls)=}.")

            # if a preference is preferred (i.e. for CURRENT granules), filter by that
            detected_polarizations = None
            if polarization_preference:
                if polarization_preference == {"VV", "VH"}:
                    self.logger.debug('Filtering to pol pref VV/VH')
                    for filter_url in granule.get("filtered_urls"):
                        # NOTE: If we want to enable https downloads in the download worker, we need to change this
                        if not filter_url.startswith("s3://"):
                            continue

                        if any(filter_url.endswith(s) for s in ["VV.tif", "VH.tif"]):
                            filtered_urls.append(filter_url)
                    detected_polarizations = frozenset({"VV", "VH"})
                elif polarization_preference == {"HH", "HV"}:
                    self.logger.debug('Filtering to pol pref HH/HV')
                    for filter_url in granule.get("filtered_urls"):
                        # NOTE: If we want to enable https downloads in the download worker, we need to change this
                        if not filter_url.startswith("s3://"):
                            continue

                        if any(filter_url.endswith(s) for s in ["HH.tif", "HV.tif"]):
                            filtered_urls.append(filter_url)
                    detected_polarizations = frozenset({"HH", "HV"})
                self.logger.info(f"Post- polarization preference filter {len(filtered_urls)=}.")
                return detected_polarizations

            detected_polarizations = None
            if most_common_polarization and most_common_polarization[0][0] == {"VV", "VH"}:
                self.logger.debug('Filtering to common pol VV/VH')
                for filter_url in granule.get("filtered_urls"):
                    # NOTE: If we want to enable https downloads in the download worker, we need to change this
                    if not filter_url.startswith("s3://"):
                        continue

                    if any(filter_url.endswith(s) for s in ["VV.tif", "VH.tif"]):
                        filtered_urls.append(filter_url)
                detected_polarizations = frozenset({"VV", "VH"})
            elif most_common_polarization and most_common_polarization[0][0] == {"HH", "HV"}:
                self.logger.debug('Filtering to common pol HH/HV')
                for filter_url in granule.get("filtered_urls"):
                    # NOTE: If we want to enable https downloads in the download worker, we need to change this
                    if not filter_url.startswith("s3://"):
                        continue

                    if any(filter_url.endswith(s) for s in ["HH.tif", "HV.tif"]):
                        filtered_urls.append(filter_url)
                detected_polarizations = frozenset({"HH", "HV"})
            else:
                self.logger.error(f"Unexpected polarization {most_common_polarization=}. Falling back to regular filtering.")
                for filter_url in granule.get("filtered_urls"):
                    # Get rid of .h and mask.tif files that aren't used
                    # NOTE: If we want to enable https downloads in the download worker, we need to change this
                    if "s3://" in filter_url and (filter_url[-6:] in ["VV.tif", "VH.tif", "HH.tif", "HV.tif"]):
                        filtered_urls.append(filter_url)
            self.logger.info(f"Post- most common polarization filter {len(filtered_urls)=}.")

            return detected_polarizations

        self.logger.info(f"{len(total_granules)=}")

        # batch_id_to_granules = defaultdict(list)
        # for granule in total_granules:
        #     batch_id_to_granules[granule["download_batch_id"]].append(granule)

        # TODO chrisjrd: unused
        # group current + baseline granules
        # rtc_prefix_to_granules_map = defaultdict(set)
        # self.logger.info("grouping current granules")
        # for granule in total_granules:
        #     rtc_prefix_to_granules_map[get_rtc_burst_prefix(granule["granule_id"])].add(granule["granule_id"])
        # self.logger.info("grouping baseline granules")
        # for granule in chain.from_iterable(self.download_batch_id_to_k_granules.values()):
        #     rtc_prefix_to_granules_map[get_rtc_burst_prefix(granule["granule_id"])].add(granule["granule_id"])

        # TODO chrisjrd: unused
        # rtc_prefix_to_rsorted_granules_map = {}
        # for k, granules in rtc_prefix_to_granules_map.items():
        #     rtc_prefix_to_rsorted_granules_map[k] = sorted(granules, key=get_unique_rtc_id_for_dist, reverse=True)
        # self.logger.info(f"{rtc_prefix_to_rsorted_granules_map=}")

        # TODO chrisjrd: consider moving out of this function,
        #  and performing right after `self.batch_id_to_current_granules` is finalized
        def init_batch_id_to_polarizations(batch_id_to_current_granules_map, logger):
            download_batch_id_to_current_granules = defaultdict(list)
            logger.debug(f"{list(batch_id_to_current_granules_map.keys())[:1]=}")
            for batch_id, current_granules in batch_id_to_current_granules_map.items():
                for g in current_granules:
                    download_batch_id_to_current_granules[g["download_batch_id"]].append(g)
            batch_id_to_polarizations = create_batch_id_to_polarizations_map(download_batch_id_to_current_granules)

            logger.info(f"{batch_id_to_polarizations=}")
            return batch_id_to_polarizations
        # determine the polarization used in the (current) granules
        batch_id_to_polarizations = init_batch_id_to_polarizations(self.batch_id_to_current_granules, self.logger)

        batch_id_to_current_urls_map = defaultdict(list)
        burst_to_pol = {}
        for granule in total_granules:
            # prefer to filter granules based on this "base" polarization
            # pol_pref = RtcForDistCmrQuery.supply_cbs_polarizations(batch_id_to_polarizations, granule["download_batch_id"])
            g_polarizations = batch_id_to_polarizations.get(granule["download_batch_id"])  # e.g. { {"VV", "VH"} }
            if not g_polarizations:
                self.logger.warning(f'No polarization detected for {granule["download_batch_id"]}. Skipping.')
                continue
            elif len(g_polarizations) == 1:
                pol_pref = one(g_polarizations)
                if len(one(g_polarizations)) == 1:
                    self.logger.info(f'Single polarization {set(pol_pref)} detected in current granules for {granule["download_batch_id"]}. A download job will not be submitted.')
            elif len(g_polarizations) > 1:
                pol_pref = g_polarizations
                self.logger.info(f'Multiple polarizations {set(pol_pref)} detected in current granules for {granule["download_batch_id"]}. A download job will not be submitted.')
            else:
                continue
            burst_id = granule['granule_id'].split('_')[3]
            # burst_to_pol[burst_id] = pol_pref
            burst_to_pol[burst_id] = add_filtered_urls(granule, batch_id_to_current_urls_map[granule["download_batch_id"]], polarization_preference=pol_pref)
        self.batch_id_to_current_urls_map = batch_id_to_current_urls_map

        batch_id_to_baseline_urls = defaultdict(list)

        # TODO chrisjrd: consider moving out of this function,
        #  and performing right after `self.batch_id_to_current_granules` is finalized
        def init_product_id_to_polarization_map(batch_id_to_current_granules_map, logger):
            product_id_to_polarization_map = {}
            """The product ID of "the current granules". This is shared in common with the baseline granules."""
            for batch_id, current_granules in batch_id_to_current_granules_map.items():
                if current_granules:
                    g = current_granules[0]
                    product_id = g["product_id"]
                    product_id_to_polarization_map[product_id] = polarizations_for_granules(current_granules)

            logger.info(f"{product_id_to_polarization_map=}")
            return product_id_to_polarization_map
        product_id_to_polarization_map = init_product_id_to_polarization_map(self.batch_id_to_current_granules, self.logger)

        for download_batch_id, granules in self.download_batch_id_to_k_granules.items():
            self.logger.info(f"Processing baseline granules. {download_batch_id=} {len(granules)=}")
            if not granules:
                self.logger.info(f"No granules to filter baseline URLs from. {download_batch_id=}. Skipping.")
                continue

            for granule in granules:
                # prefer to filter granules based on this "base" polarization
                #self.logger.info(download_batch_id)
                #self.logger.info(granule["download_batch_id"])
                pol_pref = first(product_id_to_polarization_map.get(granule["product_id"]))  # use "current" granule polarization
                burst_id = granule['granule_id'].split('_')[3]
                pol_pref = burst_to_pol.get(burst_id, pol_pref)
                #print(download_batch_id, granule["download_batch_id"])
                add_filtered_urls(granule, batch_id_to_baseline_urls[download_batch_id], polarization_preference=pol_pref)
        self._restrict_batch_urls_by_common_bursts(batch_id_to_current_urls_map, batch_id_to_baseline_urls)
        self.batch_id_to_baseline_urls_map = batch_id_to_baseline_urls

        usable_batch_id_to_current_urls_map = {}
        _unusable_batch_id_to_current_urls_map = {}
        for batch_id, current_urls in batch_id_to_current_urls_map.items():
            if not self.download_batch_id_to_job_submittable.get(batch_id):
                self.logger.info(f"{batch_id=} is marked as not submittable (baseline bursts missing). Skipping job submission.")
                _unusable_batch_id_to_current_urls_map[batch_id] = "NO_BASELINE_GRANULES"
                continue

            # If the length of urls is 0, we can't submit this. Skip.
            if len(current_urls) == 0:
                self.logger.error(f"No current URLs found for {batch_id}. Cannot submit download job.")
                _unusable_batch_id_to_current_urls_map[batch_id] = "NO_CURRENT_GRANULE_URLS"
                continue

            if batch_id not in batch_id_to_baseline_urls:
                self.logger.warning(f"Cannot find baseline URLs for {batch_id}. Cannot submit download job.")
                _unusable_batch_id_to_current_urls_map[batch_id] = "NO_BASELINE_GRANULE_URLS"
                continue

            usable_batch_id_to_current_urls_map[batch_id] = current_urls
        self.usable_batch_id_to_current_urls_map.update(usable_batch_id_to_current_urls_map)
        self._unusable_batch_id_to_current_urls_map.update(_unusable_batch_id_to_current_urls_map)

        # NOTE: this is another output of evaluation: marking a state-config as skippable
        if self.args.proc_mode == "historical":
            for batch_id, current_urls in batch_id_to_current_urls_map.items():
                if len(current_urls) == 0:
                        self.write_state_config_skippable(batch_id)

        submittable_batch_id_to_current_urls_map = {}
        _unsubmittable_batch_id_to_current_urls_map = {}
        for batch_id, current_urls in usable_batch_id_to_current_urls_map.items():
            chunk_batch_ids = [batch_id]

            # If the previous run for this tile has not been processed, submit as a pending job
            # previous_tile_product_file_paths can be None or a list of file paths

            # From  "https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0_VH.tif" ...
            # To: OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0
            one_rtc_granule = current_urls[0].split("/")[-1][:-7]
            _, acquisition_dts = parse_r2_product_file_name(one_rtc_granule, "L2_RTC_S1")
            acquisition_ts = dateutil.parser.isoparse(acquisition_dts[:-1])

            should_wait, previous_tile_product_file_paths, previous_tile_job_id = self.dist_dependency.should_wait_previous_run(fix_batch_id(batch_id), acquisition_ts)
            if should_wait:
                product_metadata = {
                    "current_s3_paths": sorted(current_urls),
                    "baseline_s3_paths": sorted(batch_id_to_baseline_urls[batch_id])
                }
                self.download_job_submitter.populate_product_metadata(product_metadata, previous_tile_product_file_paths)
                add_attributes = {
                    "previous_tile_job_id": previous_tile_job_id,
                    "download_batch_id": batch_id,
                    "acquisition_ts": acquisition_ts
                }

                product_type = "rtc_for_dist"
                job_name = f"job-WF-{product_type}_download-{chunk_batch_ids[0]}"

                self.logger.info(
                    f"We will wait for the previous run for the job {previous_tile_job_id} to complete before submitting the download job.")
                params = self.download_job_submitter._create_download_job_params(query_timerange, chunk_batch_ids, product_metadata, for_pending_job=True)
                save_blocked_download_job(self.es_conn.es_util, PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD, self.settings["RELEASE_VERSION"],
                                                           product_type, params, self.args.job_queue, job_name, add_attributes)

                _unsubmittable_batch_id_to_current_urls_map[batch_id] = "WAITING"
                continue
            submittable_batch_id_to_current_urls_map[batch_id] = current_urls
        self.submittable_batch_id_to_current_urls_map.update(submittable_batch_id_to_current_urls_map)
        self._unsubmittable_batch_id_to_current_urls_map.update(_unsubmittable_batch_id_to_current_urls_map)

        if not self.args.proc_mode == "historical":
            return []

        job_submission_tasks = self.download_job_submitter.submit_download_jobs(submittable_batch_id_to_current_urls_map, batch_id_to_baseline_urls, query_timerange)
        return job_submission_tasks

    def _restrict_batch_urls_by_common_bursts(self, batch_to_current, batch_to_baseline):
        self.logger.info(f'Restricting URLs to common bursts')

        def url_to_burst_id(url):
            match = re.match(rtc_product_file_regex, basename(url))

            if not match:
                raise ValueError(f'Could not determine burst ID from {url=}')

            burst_id = match.groupdict()['burst_id']
            return burst_id

        for batch_id in batch_to_current:
            self.logger.debug(f'{batch_id=}')

            batch_current_urls = batch_to_current[batch_id]
            batch_baseline_urls = batch_to_baseline[batch_id]

            self.logger.debug(f'{batch_current_urls=}')
            self.logger.debug(f'{batch_baseline_urls=}')

            current_burst_set = set([url_to_burst_id(url) for url in batch_current_urls])
            baseline_burst_set = set([url_to_burst_id(url) for url in batch_baseline_urls])

            self.logger.info(f'{current_burst_set=}')
            self.logger.info(f'{baseline_burst_set=}')

            if current_burst_set != baseline_burst_set:
                common_burst_set = current_burst_set & baseline_burst_set
                self.logger.warning(f'Detected a mismatch in bursts between the current and baseline RTC sets. '
                                    f'The common bursts are: {common_burst_set}')
                self.logger.info('Checking current set for extra bursts')

                extra_bursts = current_burst_set - baseline_burst_set

                if extra_bursts:
                    self.logger.warning(f'Found {len(extra_bursts)} bursts to remove '
                                        f'from the current set: {extra_bursts}')

                    batch_to_current[batch_id] = [url for url in batch_current_urls
                                                  if url_to_burst_id(url) in common_burst_set]

                    self.logger.info(f'Reduced baseline URL set for {batch_id=} from {len(batch_current_urls)} to '
                                     f'{len(batch_to_current[batch_id])}')

                self.logger.info('Checking current set for extra bursts')

                extra_bursts = baseline_burst_set - current_burst_set

                if extra_bursts:
                    self.logger.warning(f'Found {len(extra_bursts)} bursts to remove '
                                        f'from the baseline set: {extra_bursts}')

                    batch_to_baseline[batch_id] = [url for url in batch_baseline_urls
                                                   if url_to_burst_id(url) in common_burst_set]

                    self.logger.info(f'Reduced baseline URL set for {batch_id=} from {len(batch_baseline_urls)} to '
                                     f'{len(batch_to_baseline[batch_id])}')
            else:
                self.logger.info(f'Baseline and current sets for {batch_id=} contain no extra bursts')

        return batch_to_current, batch_to_baseline

    def write_state_config_skippable(self, batch_id):
        self.logger.info(f"Historical mode detected. Scheduling for publication a state-config marked as complete and skipped.")
        # NOTE: this will override any existing doc

        state_config_batch_id = batch_id.removeprefix("p").replace("_a", "_")

        existing_state_config = one(state_configs_by_batch_id(batch_id=state_config_batch_id))["_source"]["metadata"]
        ds_met_json = existing_state_config
        ds_met_json.update({
            "status": "complete",  # DEV: marking as complete to enable "skipping" this product-id-time in the chain
            "is_complete": True,
            "was_skipped": True,  # NOTE: added to distinguish from normal completion
        })

        # create state-config
        dataset_id = ds_met_json["id"]
        batch_id = ds_met_json["batch_id"]

        ds_dataset_json = create_ds_dataset_json(version="1.0")
        ds_dataset_json_path = write_ds_dataset_json(ds_dataset_json, dataset_id)
        ds_met_json_path = write_ds_met_json(ds_met_json, dataset_id)
        dataset_dir = create_dataset(dataset_id=dataset_id, ds_dataset_json=ds_dataset_json_path, ds_met_json=ds_met_json_path, dataset_type="DIST_S1-STATE-CONFIG")

class DownloadJobSubmitter:
    def __init__(
            self,
            logger=None,
            args: Args = Args(),
            dist_dependency=None,
            es_conn=None,
            settings=None,  # TODO chrisjrd: check usage
    ):
        self.logger = logger
        self.args = args
        self.dist_dependency = dist_dependency
        self.es_conn = es_conn
        self.settings = settings

    def submit_download_jobs(self, submittable_batch_id_to_current_urls_map,batch_id_to_baseline_urls, query_timerange ):
        job_submission_tasks = []
        for batch_id, current_urls in submittable_batch_id_to_current_urls_map.items():
            self.logger.info(f"Submitting download job for {batch_id=}")
            self.logger.debug(f"{current_urls=}")
            chunk_batch_ids = [batch_id]

            # If the previous run for this tile has not been processed, submit as a pending job
            # previous_tile_product_file_paths can be None or a list of file paths

            # From  "https://datapool.asf.alaska.edu/RTC/OPERA-S1/OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0_VH.tif" ...
            # To: OPERA_L2_RTC-S1_T047-100732-IW2_20250706T231126Z_20250712T063114Z_S1A_30_v1.0
            one_rtc_granule = current_urls[0].split("/")[-1][:-7]
            burst_id, acquisition_dts = parse_r2_product_file_name(one_rtc_granule, "L2_RTC_S1")
            acquisition_ts = dateutil.parser.isoparse(acquisition_dts[:-1])

            _, previous_tile_product_file_paths, __ = self.dist_dependency.should_wait_previous_run(fix_batch_id(batch_id), acquisition_ts)

            product_metadata = {
                "current_s3_paths": sorted(current_urls),
                "baseline_s3_paths": sorted(batch_id_to_baseline_urls[batch_id])
            }
            self.populate_product_metadata(product_metadata, previous_tile_product_file_paths)

            product_type = "rtc_for_dist"
            job_name = f"job-WF-{product_type}_download-{chunk_batch_ids[0]}"

            params = self._create_download_job_params(query_timerange, chunk_batch_ids, product_metadata)
            download_job_id = try_submit_mozart_job(
                product={},
                params=params,
                job_queue=self.args.job_queue,
                rule_name=f"trigger-{product_type}_download",
                job_spec=f"job-{product_type}_download:{self.settings['RELEASE_VERSION']}",
                job_type=f"{product_type}_download",
                job_name=job_name
            )

            # Record download job id in ES
            self.es_conn.mark_download_job_id(batch_id, download_job_id)

            job_submission_tasks.append(download_job_id)

        return job_submission_tasks

    def populate_product_metadata(self, product_metadata, previous_tile_product_file_paths):
        # Append the S3 prefix to the previous_tile_product_file_paths
        # from:
        # "OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1_GEN-DIST-STATUS.tif"
        # to:
        # "s3://self.settings["DATASET_BUCKET"]/products/DIST_S1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1/OPERA_L3_DIST-ALERT-S1_T11SLT_20250614T015028Z_20250715T153855Z_S1_30_v0.1_GEN-DIST-STATUS.tif

        #s3_rs_bucket = self.settings["DATASET_BUCKET"]
        #s3_rs_prefix = "s3://" + s3_rs_bucket + "/products/DIST_S1/"
        #if previous_tile_product_file_paths:
        #    previous_tile_product_file_paths = [s3_rs_prefix + f for f in previous_tile_product_file_paths]

        self.logger.info(f"Previous tile product file paths: {previous_tile_product_file_paths}")
        product_metadata["previous_tile_product_file_paths"] = previous_tile_product_file_paths

    def _create_download_job_params(self, query_timerange, chunk_batch_ids, product_metadata, for_pending_job=False):
        params = self.create_download_job_params(query_timerange, chunk_batch_ids)
        params.append({
            "name": "product_metadata",
            "from": "value",
            "type": "object",
            "value": json.dumps(product_metadata) if for_pending_job else product_metadata # Pending jobs goes into ES as a string
        })
        return params

    def create_download_job_params(self, query_timerange, chunk_batch_ids):
        args = self.args
        download_job_params = [
            {
                "name": "batch_ids",
                "value": "--batch-ids " + " ".join(chunk_batch_ids) if chunk_batch_ids else "",
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
            # TODO chrisjrd: resolve after testing
            # {
            #     "name": "start_datetime",
            #     "value": f"--start-date={query_timerange.start_date}",
            #     "from": "value"
            # },
            # {
            #     "name": "end_datetime",
            #     "value": f"--end-date={query_timerange.end_date}",
            #     "from": "value"
            # },
            {
                "name": "use_temporal",
                "value": "--use-temporal" if args.use_temporal else "",
                "from": "value"
            },
            {
                "name": "chunk_size",
                "value": f"--chunk-size={args.chunk_size}" if args.chunk_size else "",
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
                "name": "provider",
                "value": f"--provider={args.provider}",
                "from": "value"
            },
            {
                "name": "release_version",
                "value": f"--release-version={args.release_version}" if args.release_version else "",
                "from": "value"
            }
        ]
        self.logger.debug(f"{download_job_params=}")
        return download_job_params


def create_batch_id_to_polarizations_map(batch_to_granules_map):
    return {
        batch_id: polarizations_for_granules(granules)
        for batch_id, granules in batch_to_granules_map.items()
    }


def supply_cbs_polarizations(batch_id_to_polarizations, batch_id):
    """Determine polarization for the current burst set (CBS). None if indeterminate."""
    batch_polarization = batch_id_to_polarizations.get(batch_id, None)
    if batch_polarization is None:
        polarization_preference = None
    else:
        if len(batch_polarization) == 1:
            polarization_preference = next(iter(batch_polarization))
        else:  # multiple polarizations / indeterminate
            polarization_preference = None
    return polarization_preference


def polarizations_for_granules(granules) -> set[frozenset]:
    """
    Given a list of granules, return a (unique) set of all polarizations detected.
    In most cases, this will look like singleton set like { {"VV", "VH"} }
    But it is theoretically possible to have a longer set like { {"VV", "VH"}, {"HH, "HV"}, {"VV"}, {"HH"}, ... }
    Note: granules may have single polarizations, or multiple.
    """
    return {frozenset(g["polarization"]) for g in granules if g.get("polarization")}
