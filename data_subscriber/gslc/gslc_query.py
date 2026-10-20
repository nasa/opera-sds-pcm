#!/usr/bin/env python3

"""Historical query of NISAR GSLCs for one DISP-NI k-set.

run_disp_ni_historical_processing.py submits one gslc_query_hist job per frame and k-set, with a
window that covers exactly the k sensing dates the DISP-NI frame database lists for it. This job
finds those GSLCs in CMR, catalogs them, and submits a single download job for all k dates.
"""

from collections import defaultdict
from datetime import datetime

from data_subscriber.cmr import CMR_TIME_FORMAT
from data_subscriber.cslc_utils import split_download_batch_id
from data_subscriber.gslc.gslc_utils import (GSLC_MODE_POLARIZATION_SETTINGS_FIELD,
                                             build_gslc_native_id_patterns,
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

        self.mode_polarization_preference = settings["DISP_NI"][GSLC_MODE_POLARIZATION_SETTINGS_FIELD]

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
            if gslc.nisar_frame not in frame.nisar_frames or gslc.sensing_start.date() not in frame_dates:
                self.logger.info("Skipping %s because it is not on a sensing date of DISP-NI frame %d",
                                 granule["granule_id"], self.frame_id)
                continue

            download_batch_id = gslc_download_batch_id(self.frame_id, gslc.cycle)
            granule.update({
                "frame_id": self.frame_id,
                "nisar_frame": gslc.nisar_frame,
                "acquisition_cycle": gslc.cycle,
                "acquisition_ts": gslc.sensing_start,
                "mode": gslc.mode,
                "polarization": gslc.polarization,
                "download_batch_id": download_batch_id,
                "unique_id": gslc_unique_id(download_batch_id, gslc.nisar_frame),
            })
            frame_granules.append(granule)

        return frame_granules

    def eliminate_duplicate_granules(self, granules):
        """Keep one GSLC per NISAR frame and date, all in the same mode and polarization.

        Within one NISAR frame, cycle, mode and polarization, granule ids differ only by their
        CRID and counter, so the highest id is the newest one. The k-set then gets the first
        preferred mode/polarization that every one of its NISAR frames and dates has, because
        ICS 2.6.1 requires a single frequency and polarization per job and a frame can be
        acquired in several modes on one date."""

        latest = {}
        for granule in granules:
            key = (granule["nisar_frame"], granule["acquisition_cycle"], granule["mode"], granule["polarization"])
            if key not in latest or granule["granule_id"] > latest[key]["granule_id"]:
                latest[key] = granule

        granules = self._select_mode_polarization(list(latest.values()))
        self.validate_historical_query_result(granules)

        return granules

    def _select_mode_polarization(self, granules):
        if not granules:
            return granules

        acquisitions = {(g["nisar_frame"], g["acquisition_cycle"]) for g in granules}
        acquisitions_by_mode = defaultdict(set)
        for g in granules:
            acquisitions_by_mode[f'{g["mode"]}_{g["polarization"]}'].add((g["nisar_frame"], g["acquisition_cycle"]))

        for mode_polarization in self.mode_polarization_preference:
            if acquisitions_by_mode.get(mode_polarization) == acquisitions:
                self.logger.info("Using GSLC mode/polarization %s for frame %d", mode_polarization, self.frame_id)
                return [g for g in granules if f'{g["mode"]}_{g["polarization"]}' == mode_polarization]

        found = {mode: sorted(cycles) for mode, cycles in sorted(acquisitions_by_mode.items())}
        err = RuntimeError("No common GSLC mode/pol")
        err.add_note(f"frame_id={self.frame_id}: none of {list(self.mode_polarization_preference)} is available on "
                     f"every NISAR frame and cycle {sorted(acquisitions)}. Found (NISAR frame, cycle) per "
                     f"mode/polarization: {found}")
        raise err

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
            missing = frame.nisar_frames - nisar_frames
            if missing:
                err = RuntimeError("GSLC missing a NISAR frame")
                err.add_note(f"frame_id={self.frame_id} on {date}: missing {sorted(missing)}. This is usually a "
                             f"transient CMR error, so try this job again.")
                raise err

    def prepare_additional_fields(self, granule, args, granule_id):
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["frame_id", "nisar_frame", "acquisition_cycle", "acquisition_ts", "mode", "polarization",
                  "download_batch_id", "unique_id"]:
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
