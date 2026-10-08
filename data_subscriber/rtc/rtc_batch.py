import json
import logging
import os
import shutil
from datetime import datetime, timezone
from uuid import uuid4

from data_subscriber.rtc import rtc_state_config_constants as c
from util.common_util import convert_datetime


logger = logging.getLogger(__name__)


def create_rtc_batch_dataset(datasets_dir, rtc_ids, sensor, slc_id=None):
    rtc_ids = sorted(set(rtc_ids))
    if not rtc_ids:
        raise ValueError('Batch cannot be empty')

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    batch_id = f'OPERA_RTC-S1_BATCH_{now.strftime("%Y%m%dT%H%M%S")}_{sensor}_{str(uuid4())}'

    batch_dir = os.path.join(datasets_dir, batch_id)

    if os.path.isdir(batch_dir):
        shutil.rmtree(batch_dir)

    batch_metadata = {
        "id": batch_id,
        "count": len(rtc_ids),
        "source_slc": slc_id,
        c.RTC_IDS: rtc_ids,
        c.SENSOR: sensor,
    }

    batch_met_path = os.path.join(batch_dir, f"{batch_id}.met.json")
    with open(batch_met_path, "w") as f:
        json.dump(batch_metadata, f, indent=2)

    # .dataset.json — HySDS dataset descriptor
    batch_dataset_info = {
        "version": "1",
        "creation_timestamp": convert_datetime(now),
        "index": {
            "suffix": "1_{}-{}".format(
                c.RTC_BATCH.lower(),
                now.strftime("%Y.%m")
            )
        },
    }

    batch_ds_path = os.path.join(batch_dir, f"{batch_id}.dataset.json")
    with open(batch_ds_path, "w") as f:
        json.dump(batch_dataset_info, f, indent=2)

    logger.info(f'Created RTC batch dataset {batch_id} for {len(rtc_ids)} bursts')
    return batch_id
