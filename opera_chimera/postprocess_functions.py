"""
Class that contains the post process steps used in the various PGEs
that are part of the OPERA PCM pipeline.

"""

from chimera.postprocess_functions import PostProcessFunctions
import ast
import os
import traceback
from datetime import datetime

from util.common_util import convert_datetime

from opera_chimera.accountability import OperaAccountability

from commons.logger import logger
from commons.es_connection import get_grq_es, get_mozart_es

from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as oc_const,
)

class OperaPostProcessFunctions(PostProcessFunctions):
    def __init__(self, context, pge_config, settings, job_result):
        ancillary_es = get_grq_es(logger)
        mozart_es = get_mozart_es(logger)
        PostProcessFunctions.__init__(
            self, context, pge_config, settings, job_result, mozart_es, ancillary_es
        )
        logger.info("job_result: {}".format(job_result))
        self.accountability = OperaAccountability(self._context)

    def _associate_products(self, output_types, products):
        results = {}
        if self.accountability.step == "L0B":
            # going through each output_type and checking if it was created
            for output_type in output_types:
                # finding the search tearm ex: L0B_L_RRSD or L0B_L_HST_DRT
                search_term = "_".join(output_type.split("_")[2:])
                # going through each product and seing if the search term is there (HST_DRT in product_id)
                for i in range(0, len(products)):
                    product_id = os.path.basename(products[i])
                    if search_term in product_id:
                        results[output_type] = product_id
                        products.remove(products[i])
                        break
            return results
        else:
            return {
                output_types[0]: products[0]
            }