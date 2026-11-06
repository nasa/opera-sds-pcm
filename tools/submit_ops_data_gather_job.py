import argparse
import json
from datetime import datetime, timezone
from dateutil.parser import parse

from opera_commons.logger import logger
from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job


def main(
        *,
        dup_override=None,
        acc_override=None,
        dup_venue=None,
        acc_venue=None,
        fine_override=None,
):
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    args = (dup_override, acc_override, dup_venue, acc_venue, fine_override)

    day = now.day
    dow = now.weekday()

    if day == 1:  # Monthly
        tag = 'monthly'
        dup_window_default = 100
        acc_window_default = 100
        slc_acc_window_default = 90
    elif dow == 0:  # Weekly
        tag = 'weekly'
        dup_window_default = 40
        acc_window_default = 40
        slc_acc_window_default = 31
    else:  # Daily
        tag = 'daily'
        dup_window_default = 7
        acc_window_default = 7
        slc_acc_window_default = 4

    if any(arg is not None for arg in args):
        tag = 'manual'

    params = [
        {
            "name": "report_tag",
            "from": "value",
            "type": "text",
            "value": tag
        },
    ]

    if dup_override is not None:
        if isinstance(dup_override, (list, tuple)):  # Start/end
            params.extend([
                {
                    "name": "default_duplicates_start",
                    "from": "value",
                    "type": "text",
                    "value": dup_override[0]
                },
                {
                    "name": "default_duplicates_end",
                    "from": "value",
                    "type": "text",
                    "value": dup_override[1]
                },
            ])
        else:
            params.extend([
                {
                "name": "default_duplicates_window",
                "from": "value",
                "type": "number",
                "value": f"{dup_override}"
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
            ])
    else:
        params.extend([
            {
                "name": "default_duplicates_window",
                "from": "value",
                "type": "number",
                "value": f"{dup_window_default}"
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
        ])

    params.append({
        "name": "default_duplicates_venue",
        "from": "value",
        "type": "enum",
        "value": dup_venue or ("GRQ" if tag != 'monthly' else "PROD")
    })

    if acc_override is not None:
        if isinstance(acc_override, (list, tuple)):  # Start/end
            params.extend([
                {
                    "name": "default_accountability_start",
                    "from": "value",
                    "type": "text",
                    "value": acc_override[0]
                },
                {
                    "name": "default_accountability_end",
                    "from": "value",
                    "type": "text",
                    "value": acc_override[1]
                },
            ])
        else:
            params.extend([
                {
                    "name": "default_accountability_window",
                    "from": "value",
                    "type": "number",
                    "value": f"{acc_override}"
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
            ])
    else:
        params.extend([
            {
                "name": "default_accountability_window",
                "from": "value",
                "type": "number",
                "value": f"{acc_window_default}"
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
        ])

    params.append({
        "name": "default_accountability_venue",
        "from": "value",
        "type": "enum",
        "value": acc_venue or "PROD"
    })

    if fine_override is not None:
        params.append({
            "name": "override_config",
            "from": "value",
            "type": "object",
            "value": fine_override
        })
    else:
        params.append({
            "name": "override_config",
            "from": "value",
            "type": "object",
            "value": {
                "products": {
                    "RTC_S1": {
                        "accountability": {
                            "venue": "GRQ",
                            "window": slc_acc_window_default,
                        }
                    },
                    "CSLC_S1": {
                        "accountability": {
                            "venue": "GRQ",
                            "window": slc_acc_window_default,
                        }
                    },
                }
            }
        })

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
    parser = argparse.ArgumentParser()

    parser.add_argument('--duplicates-start', type=parse, help='Start datetime for duplicates detection')
    parser.add_argument('--duplicates-end', type=parse, help='End datetime for duplicates detection')
    parser.add_argument('--acc-start', type=parse, help='Start datetime for accountability detection')
    parser.add_argument('--acc-end', type=parse, help='End datetime for accountability detection')

    parser.add_argument('--duplicates-window', type=int, help='Window size for duplicate detection, overrides time range')
    parser.add_argument('--acc-window', type=int, help='Window size for accountability detection, overrides time range')

    parser.add_argument('--duplicates-venue', choices=['PROD', 'UAT', 'GRQ'], help='Venue for duplicate detection')
    parser.add_argument('--acc-venue', choices=['PROD', 'UAT', 'GRQ'], help='Venue for accountability detection')

    parser.add_argument('--overrides-file', help='Path to JSON file with fine overrides')

    args = parser.parse_args()

    if args.duplicates_window is not None:
        dup_override = args.duplicates_window
    elif args.duplicates_start is not None or args.duplicates_end is not None:
        dup_override = (
            args.duplicates_start.strftime('%Y-%m-%dT%H:%M:%SZ') if args.duplicates_start is not None else '1970-01-01T00:00:00Z',
            args.duplicates_end.strftime('%Y-%m-%dT%H:%M:%SZ') if args.duplicates_end is not None else '3000-01-01T00:00:00Z',
        )
    else:
        dup_override = None

    if args.acc_window is not None:
        acc_override = args.acc_window
    elif args.acc_start is not None or args.acc_end is not None:
        acc_override = (
            args.acc_start.strftime('%Y-%m-%dT%H:%M:%SZ') if args.acc_start is not None else '1970-01-01T00:00:00Z',
            args.acc_end.strftime('%Y-%m-%dT%H:%M:%SZ') if args.acc_end is not None else '3000-01-01T00:00:00Z',
        )
    else:
        acc_override = None

    if args.overrides_file is not None:
        with open(args.overrides_file) as f:
            overrides = json.load(f)  # TODO: Validate
    else:
        overrides = None

    main(
        dup_override=dup_override,
        acc_override=acc_override,
        dup_venue=args.duplicates_venue,
        acc_venue=args.acc_venue,
        fine_override=overrides
    )
