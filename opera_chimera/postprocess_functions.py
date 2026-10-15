"""
Class that contains the post process steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""
from datetime import timezone, datetime
import logging
import os
from typing import Dict

from chimera.commons.constants import ChimeraConstants as chimera_consts
from chimera.postprocess_functions import PostProcessFunctions

from opera_chimera.accountability import OperaAccountability
from opera_commons.es_connection import get_grq_es, get_mozart_es

from util.common_util import backoff_wrapper

logger = logging.getLogger(__name__)

grq_es = get_grq_es(logger)
mozart_es = get_mozart_es(logger)


class OperaPostProcessFunctions(PostProcessFunctions):
    def __init__(self, context: Dict, pge_config: Dict, settings, job_result):
        PostProcessFunctions.__init__(
            self, context, pge_config, settings, job_result, mozart_es=mozart_es, grq_es=grq_es
        )
        logger.info("job_result: {}".format(job_result))
        self.accountability = OperaAccountability(self._context, job_result.get(chimera_consts.WORK_DIR))

    def update_product_accountability(self):
        self.accountability.set_products(self._job_result)
        return {}

    def mark_input_state_config_dswx_s1(self):
        from data_subscriber.rtc import rtc_state_config_constants as c

        output_datasets = self._job_result.get("output_datasets", [])
        job_id = self._job_result.get("job_id")

        input_dataset_type = self._context.get("dataset_type")
        input_dataset_id = self._context.get("input_dataset_id")

        if (input_dataset_type in {c.DSWX_S1_STATE_CONFIG, c.DSWX_S1_EXPIRED_STATE_CONFIG}
                and not self._job_result.get('error')):
            if input_dataset_type == c.DSWX_S1_EXPIRED_STATE_CONFIG:
                main_sc_id = (input_dataset_id.removesuffix('-expired-state-config')) + '-state-config'
                logger.info(f'Got main-line state config ID for {input_dataset_id}: {main_sc_id}')
                input_dataset_id = main_sc_id

            logger.info(f'Marking DSWx-S1 State Config "{input_dataset_id}" with output')
            marked_job_result = {
                "job_id": job_id,
                "completion_time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                "triggered_bursts": self._context.get('product_metadata', {})
                                                 .get('metadata', {}).get('found_bursts', []),
                "output_products": [os.path.basename(ds) for ds in output_datasets],
            }

            try:
                result = backoff_wrapper(
                    grq_es.es.update_by_query,
                    index=c.DSWX_S1_STATE_CONFIG_ES_PATTERN,
                    body={
                        "script": {
                            "source": "if (ctx._source.metadata.triggered_jobs == null) { "
                                      "ctx._source.metadata.triggered_jobs = new ArrayList(); } "
                                      "ctx._source.metadata.triggered_jobs.add(params.job); "
                                      "if (ctx._source.metadata.triggered_count == null) { "
                                      "ctx._source.metadata.triggered_count = 0; } "
                                      "ctx._source.metadata.triggered_count++;",
                            "params": {
                                "job": marked_job_result
                            },
                            "lang": "painless"
                        },
                        "query": {
                            "term": {
                                "id.keyword": input_dataset_id
                            }
                        }
                    }
                )

                logger.info(f"Job mark result: {result}")

                if not result.get("updated"):
                    logger.error("Failed to mark DSWx-S1 State Config - this may result in difficult "
                                 "to track duplicates")
            except Exception as e:
                logger.error(f"Failed to mark DSWx-S1 State Config: {e} - this may result in difficult to "
                             f"track duplicates")
        elif not self._job_result.get('error'):
            logger.info('Skipping input marking as this job was not triggered by a State Config')

        return {}
