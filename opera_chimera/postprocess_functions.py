"""
Class that contains the post process steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""

from chimera.postprocess_functions import PostProcessFunctions
# import ast
# import json
# import traceback
# from datetime import datetime

# from util.common_util import convert_datetime

from opera_chimera.accountability import OperaAccountability

from commons.logger import logger
from commons.es_connection import get_grq_es, get_mozart_es

from chimera.commons.constants import ChimeraConstants as chimera_consts

grq_es = get_grq_es(logger)
mozart_es = get_mozart_es(logger)


class OperaPostProcessFunctions(PostProcessFunctions):
    def __init__(self, context, pge_config, settings, job_result):
        PostProcessFunctions.__init__(
            self, context, pge_config, settings, job_result, mozart_es=mozart_es, grq_es=grq_es
        )
        logger.info("job_result: {}".format(job_result))
        self.accountability = OperaAccountability(self._context, job_result.get(chimera_consts.WORK_DIR))

    def update_product_accountability(self):
        self.accountability.set_products(self._job_result)
        return {}
