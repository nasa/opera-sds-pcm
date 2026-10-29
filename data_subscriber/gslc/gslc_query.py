#!/usr/bin/env python3

"""Historical query of NISAR GSLCs for one DISP-NI k-set.

run_disp_ni_historical_processing.py submits one gslc_query_hist job per frame and k-set, with a
window that covers exactly the k sensing dates the DISP-NI frame database lists for it. This job
finds those GSLCs in CMR, catalogs them, and submits a single download job for all k dates.
"""

from collections import defaultdict
from datetime import datetime
from fnmatch import fnmatchcase
from pathlib import Path

from data_subscriber.cmr import CMR_TIME_FORMAT
from data_subscriber.cslc_utils import split_download_batch_id
from data_subscriber.gslc.gslc_utils import (build_gslc_native_id_patterns,
                                             gslc_download_batch_id,
                                             gslc_unique_id,
                                             localize_disp_ni_frame_db,
                                             parse_gslc_granule_id,
                                             process_disp_ni_frame_db)
from data_subscriber.query import BaseQuery

GSLC_DOWNLOAD_PRODUCT_TYPE = "gslc"


class GslcCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, disp_ni_frame_db_file=None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if disp_ni_frame_db_file is None:
            self.frame_db = localize_disp_ni_frame_db()
        else:
            self.frame_db = process_disp_ni_frame_db(disp_ni_frame_db_file)

    def validate_args(self):
        if self.proc_mode != "historical":
            raise AssertionError("GSLC query supports historical mode only.")
        if self.args.frame_id is None:
            raise AssertionError("Historical mode requires frame id to be specified.")
        if self.args.start_date is None or self.args.end_date is None:
            raise AssertionError("Historical mode requires start and end date to be specified.")
        if self.args.native_id is not None:
            raise AssertionError("Historical mode does not support native_id.")
        if self.args.k is None or self.args.k < 1:
            raise AssertionError("k parameter must be greater than 0.")
        if self.args.m is None or self.args.m < 1:
            raise AssertionError("m parameter must be greater than 0.")

    @property
    def frame_id(self) -> int:
        return int(self.args.frame_id)

    def query_cmr(self, timerange, now: datetime):
        frame = self.frame_db.get(self.frame_id)
        if frame is None:
            err = RuntimeError("Frame not in DISP-NI frame DB")
            err.add_note(f"frame_id={self.frame_id} is not in the DISP-NI frame database, "
                         f"so OPERA does not process it for DISP-NI.")
            raise err

        self.args.native_id_patterns = build_gslc_native_id_patterns(frame)
        self.args.use_temporal = True
        granules = super().query_cmr(timerange, now)

        frame_dates = {dt.date() for dt in frame.sensing_datetimes}
        frame_granules = []
        for granule in granules:
            try:
                gslc = parse_gslc_granule_id(granule["granule_id"])
            except ValueError:
                self.logger.warning("Skipping %s because it is not a NISAR GSLC granule id", granule["granule_id"])
                continue

            # The native-id patterns already restrict the query to the frame's NISAR frames. A date the
            # frame database leaves out was excluded deliberately, so it is not part of any k-set.
            if not frame.covers(gslc.nisar_frame) or gslc.sensing_start.date() not in frame_dates:
                self.logger.info("Skipping %s because it is not on a sensing date of DISP-NI frame %d",
                                 granule["granule_id"], self.frame_id)
                continue

            # The frame database picks the GSLC mode and coverage the frame is processed in
            if gslc.mode != frame.mode or gslc.coverage != frame.coverage:
                self.logger.info("Skipping %s because DISP-NI frame %d is processed in mode %s, coverage %s",
                                 granule["granule_id"], self.frame_id, frame.mode, frame.coverage)
                continue

            # CMR lists the GSLC's QA_STATS.h5 next to the GSLC itself. Both would land in the same
            # catalog doc, the QA file's URLs overwriting the GSLC's, so keep only the GSLC.
            granule["filtered_urls"] = [url for url in granule["filtered_urls"]
                                        if Path(url).name == f"{granule['granule_id']}.h5"]

            download_batch_id = gslc_download_batch_id(self.frame_id, gslc.cycle)
            granule.update({
                "frame_id": self.frame_id,
                "nisar_frame": gslc.nisar_frame,
                "acquisition_cycle": gslc.cycle,
                "acquisition_ts": gslc.sensing_start,
                "mode": gslc.mode,
                "polarization": gslc.polarization,
                "coverage": gslc.coverage,
                "download_batch_id": download_batch_id,
                "unique_id": gslc_unique_id(download_batch_id, gslc.nisar_frame),
            })
            frame_granules.append(granule)

        return frame_granules

    def eliminate_duplicate_granules(self, granules):
        """Keep one GSLC per NISAR frame and date.

        query_cmr already kept only the frame's mode and coverage, which is all the frame database
        selects on. Granule ids of one NISAR frame and cycle then differ by their CRID and counter, so
        the highest id is the newest one. A pass acquired in two polarizations (36 frame-dates of the
        2026-10-27 database, mostly DHDH with QPDH) also gives two GSLCs; the highest id is kept there
        too, with a warning."""

        latest = {}
        for granule in granules:
            key = (granule["nisar_frame"], granule["acquisition_cycle"])
            current = latest.get(key)
            if current is not None and current["polarization"] != granule["polarization"]:
                self.logger.warning("Frame %d has %s and %s GSLCs of %s on cycle %d; keeping the highest granule id",
                                    self.frame_id, current["polarization"], granule["polarization"], key[0], key[1])
            if current is None or granule["granule_id"] > current["granule_id"]:
                latest[key] = granule

        granules = list(latest.values())
        self.validate_historical_query_result(granules)

        return granules

    def validate_historical_query_result(self, granules):
        """Every frame database date in the window must be found, with every NISAR frame of the frame.

        The window comes from run_disp_ni_historical_processing.py and covers exactly the k dates of
        one k-set, so anything else is a CMR gap, usually transient. Failing lets the job be retried
        rather than downloading an incomplete k-set."""

        frame = self.frame_db[self.frame_id]
        start = datetime.strptime(self.args.start_date, CMR_TIME_FORMAT)
        end = datetime.strptime(self.args.end_date, CMR_TIME_FORMAT)
        expected_dates = {dt.date() for dt in frame.sensing_datetimes if start <= dt <= end}

        nisar_frames_by_date = defaultdict(set)
        for g in granules:
            nisar_frames_by_date[g["acquisition_ts"].date()].add(g["nisar_frame"])

        if set(nisar_frames_by_date) != expected_dates:
            err = RuntimeError("GSLC dates don't match frame DB")
            err.add_note(f"frame_id={self.frame_id}: expected {sorted(map(str, expected_dates))}, "
                         f"found {sorted(map(str, nisar_frames_by_date))}. This is usually a transient CMR "
                         f"error, so try this job again.")
            raise err

        for date, nisar_frames in sorted(nisar_frames_by_date.items()):
            missing = {pattern for pattern in frame.nisar_frames
                       if not any(fnmatchcase(nisar_frame, pattern) for nisar_frame in nisar_frames)}
            if missing:
                err = RuntimeError("GSLC missing a NISAR frame")
                err.add_note(f"frame_id={self.frame_id} on {date}: missing {sorted(missing)}. This is usually a "
                             f"transient CMR error, so try this job again.")
                raise err

    def prepare_additional_fields(self, granule, args, granule_id):
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        # bounding_box is the GSLC's CMR footprint, which the download job turns into the frame's DEM/mask bounds
        for f in ["frame_id", "nisar_frame", "acquisition_cycle", "acquisition_ts", "mode", "polarization", "coverage",
                  "download_batch_id", "unique_id", "bounding_box"]:
            additional_fields[f] = granule[f]
        additional_fields["k"] = args.k
        additional_fields["m"] = args.m

        return additional_fields

    def download_job_submission_handler(self, granules, query_timerange, mark_docs_in_parallel=False):
        """Submit one download job covering all k dates of the k-set.

        The download job (OPERA-2673) receives every batch id of the k-set and submits the DISP-NI PGE job."""

        batch_ids = sorted({g["download_batch_id"] for g in granules},
                           key=lambda batch_id: split_download_batch_id(batch_id)[1])
        if not batch_ids:
            self.logger.info("No GSLC granules to download for frame %d", self.frame_id)
            return []

        if len(batch_ids) != self.args.k:
            err = RuntimeError("GSLC batch count is not k")
            err.add_note(f"frame_id={self.frame_id}: {len(batch_ids)} batches {batch_ids} for k={self.args.k}")
            raise err

        # As for CSLC, the download job handles the whole k-set at once
        self.args.chunk_size = self.args.k
        params = self.create_download_job_params(query_timerange, list(batch_ids))

        first_cycle = split_download_batch_id(batch_ids[0])[1]
        last_cycle = split_download_batch_id(batch_ids[-1])[1]
        job_name = (f"job-WF-{GSLC_DOWNLOAD_PRODUCT_TYPE}_download-frame-{self.frame_id}"
                    f"-cycles-{first_cycle}-to-{last_cycle}")

        job_id = self._submit_and_mark_one_job(
            [(batch_id, None) for batch_id in batch_ids],
            release_version=self.args.release_version or self.settings["RELEASE_VERSION"],
            product_type=GSLC_DOWNLOAD_PRODUCT_TYPE,
            params=params,
            job_queue=self.args.job_queue,
            job_name=job_name,
            payload_hash=None,
        )

        return [job_id]
