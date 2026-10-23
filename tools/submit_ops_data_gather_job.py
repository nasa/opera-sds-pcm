from datetime import datetime, timezone

from opera_commons.logger import logger
from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job


# TODO: Refine this to a proper command-line script, this is currently just for cron job submission, driven by current
#       time. User should be able to specify time windows/ranges/overrides when calling this

def main():
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    day = now.day
    dow = now.weekday()

    if day == 1:  # Monthly
        tag = 'monthly'
        dup_window = 45
        acc_window = 45
        slc_acc_window = 31
    elif dow == 0:  # Weekly
        tag = 'weekly'
        dup_window = 10
        acc_window = 10
        slc_acc_window = 7
    else:  # Daily
        tag = 'daily'
        dup_window = 3
        acc_window = 3
        slc_acc_window = 1

    params = [
        {
            "name": "report_tag",
            "from": "value",
            "type": "text",
            "value": tag
        },
        {
            "name": "default_duplicates_window",
            "from": "value",
            "type": "number",
            "value": f"{dup_window}"
        },
        {
            "name": "default_duplicates_start",
            "from": "value",
            "type": "text",
            "value": "1970-01-01T00:00:00Z"
        },
        {
            "name": "default_duplicates_end",
            "from": "value",
            "type": "text",
            "value": "3000-01-01T00:00:00Z"
        },
        {
            "name": "default_duplicates_venue",
            "from": "value",
            "type": "enum",
            "value": "GRQ" if tag != 'monthly' else "CMR"
        },
        {
            "name": "default_accountability_window",
            "from": "value",
            "type": "",
            "value": f"{acc_window}"
        },
        {
            "name": "default_accountability_start",
            "from": "value",
            "type": "text",
            "value": "1970-01-01T00:00:00Z"
        },
        {
            "name": "default_accountability_end",
            "from": "value",
            "type": "text",
            "value": "3000-01-01T00:00:00Z"
        },
        {
            "name": "default_accountability_venue",
            "from": "value",
            "type": "enum",
            "value": "CMR"
        },
        {
            "name": "override_config",
            "from": "value",
            "type": "object",
            "value": {
                "products": {
                    "RTC_S1": {
                        "accountability": {
                            "venue": "GRQ",
                            "window": slc_acc_window,
                        }
                    },
                    "CSLC_S1": {
                        "accountability": {
                            "venue": "GRQ",
                            "window": slc_acc_window,
                        }
                    },
                }
            }
        },
    ]

    release = SettingsConf().cfg.get('RELEASE_VERSION')

    try:
        job_id = try_submit_mozart_job(
            product={},
            job_queue='opera-job_worker-ops_report_data_gather',
            rule_name="trigger-job-gather_ops_report_data",
            params=params,
            job_spec=f"job-gather_ops_report_data:{release}",
            job_name=f"job-gather_ops_report_data-{now.strftime('%Y%m%dT%H%M%SZ')}",
        )
        logger.info(f"Submitted job {job_id}")
    except Exception as e:
        logger.error(f"Failed to submit job: {str(e)}")
        raise


if __name__ == "__main__":
    main()
