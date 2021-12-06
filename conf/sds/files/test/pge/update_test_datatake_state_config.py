import sys
import json
import backoff

from hysds.es_util import get_grq_es

from util.common_util import create_state_config_dataset
from commons.constants import product_metadata as pm
from commons.logger import logger

grq_es = get_grq_es()  # get connection to GRQ's Elasticsearch


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=1200)
def search_for_state_config(id):
    result = grq_es.get_by_id(id=id, index="grq_1_{}".format(pm.DATATAKE_STATE_CONFIG), ignore=[404])
    if result.get("found", False):
        return result
    else:
        raise RuntimeError("ERROR: Cannot find datatake state config with ID {}.\n".format(id))


if __name__ == "__main__":
    state_config_id = sys.argv[1]
    es_record = search_for_state_config(state_config_id)
    metadata = es_record.get("_source", {}).get("metadata", {})

    # force submit it
    metadata[pm.FORCE_SUBMIT] = True
    logger.info("metadata: {}".format(json.dumps(metadata, indent=2, sort_keys=True)))

    # create dataset directory
    create_state_config_dataset(state_config_id, metadata,
                                metadata[pm.DATATAKE_BEGIN_TIME],
                                metadata[pm.DATATAKE_END_TIME])
