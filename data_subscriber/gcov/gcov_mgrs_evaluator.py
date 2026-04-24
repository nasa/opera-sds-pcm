"""
DSWx-NI MGRS Evaluator

Triggered by NISAR GCOV ingest or on-demand re-evaluation. Queries ES for all GCOV products matching
the MGRS tile sets covered by the input, creating a DSWX-NI State Config for each.
"""

from data_subscriber import es_conn_util
from data_subscriber.cslc.disp_s1_state_config import find_state_config
from data_subscriber.gcov import gcov_state_config_constants as c
from data_subscriber.gcov.gcov_granule_util import extract_track_id, extract_frame_id, extract_cycle_number, extract_acquisition_time_range
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.logger import get_logger
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()


# TODO: Need to determine if cycle is reliable. I think there are some sets covered by different cycles
#  eg. MS_1_173[1_1, 173_176] & MS_1_174[1_1, 1_2, 173_176]. Thought: MS_1* w/ T173 -> T173 cyc -= 1;
#      MS_173* w/ T1 -> T1 cyc += 1... Need to ask more about this


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

            logger.info(f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}, {sensing_date=}')
            self._msg(
                f're-eval {mgrs_set_id} {sensing_date}',
                f'DSWx-NI MGRS set re-evaluation triggered: {mgrs_set_id=}, {sensing_date=}'
            )

            self._evaluate_mgrs_tile_set(mgrs_set_id, sensing_date, force_publish=force_publish)
        else:
            native_id = input_dataset_id

            track_id = extract_track_id(native_id)
            frame_id = extract_frame_id(native_id)

            acquisition_start_dts, _ = extract_acquisition_time_range(native_id)
            sensing_date = acquisition_start_dts.strftime("%Y%m%d")

            logger.info(f'Evaluating GCOV {native_id}, {track_id=}, {frame_id=}, {sensing_date=}')

            mgrs_set_ids = list(self.mgrs_track_frame_db.frame_and_track_to_mgrs_sets((frame_id, track_id)).keys())

            self._msg(
                f'evaluating {len(mgrs_set_ids)} tile sets',
                f'Track-frame {track_id}_{frame_id} belongs to {len(mgrs_set_ids)}: {mgrs_set_ids}'
            )

            for mgrs_set_id in mgrs_set_ids:
                self._evaluate_mgrs_tile_set(mgrs_set_id, sensing_date, force_publish=force_publish)

    def _evaluate_mgrs_tile_set(self, mgrs_set_id, sensing_date, force_publish=False):
        sc_id = f'mgrs_set-{mgrs_set_id}${sensing_date}-state-config'
        expected_track_frames = self.mgrs_track_frame_db.mgrs_set_id_to_track_frames(mgrs_set_id)

        logger.info(f'Evaluating state config {sc_id}')

        existing_state_config, _ = find_state_config(self.es_conn, sc_id, c.MGRS_SET_STATE_CONFIG)

        if not force_publish:
            if existing_state_config.get(c.IS_COMPLETE, False):
                logger.info(f'State config {sc_id} is already complete and will be skipped')
                self._msg(
                    f'{mgrs_set_id}${sensing_date} already complete',
                    f'State config {sc_id} is already complete and will be skipped'
                )
                return

        prev_found_track_frames = set(existing_state_config.get(c.FOUND_TRACK_FRAMES, []))

        # TODO: Get current track frames. If sets are identical, do nothing; else, update metadata & reset
        #       expiration time & compute coverage


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
