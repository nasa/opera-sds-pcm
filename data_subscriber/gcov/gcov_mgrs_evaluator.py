"""
DSWx-NI MGRS Evaluator

Triggered by NISAR GCOV ingest or on-demand re-evaluation. Queries ES for all GCOV products matching
the MGRS tile sets covered by the input, creating a DSWX-NI State Config for each.
"""

from data_subscriber import es_conn_util
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.logger import get_logger
from util.conf_util import SettingsConf
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()


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
        logger.info('TEST')
        self._msg('test', detail='test')


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
