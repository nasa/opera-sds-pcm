#!/usr/bin/env python
"""
SDSWatch daemon to periodically dump SDSWatch logs of systemd services.
"""
import argparse
import time
import re
from subprocess import check_output
from datetime import datetime


# regexes
ACTIVESTATE_RE = re.compile(r"\nActiveState=(?P<ActiveState>.+)\n")
SUBSTATE_RE = re.compile(r"\nSubState=(?P<SubState>.+)\n")
ACTIVEENTER_TS_RE = re.compile(r"\nActiveEnterTimestamp=(?P<ActiveEnterTimestamp>.+)\n")
WATCHDOG_TS_RE = re.compile(r"\nWatchdogTimestamp=(?P<WatchdogTimestamp>.+)\n")


def daemon(check, host, name, source_type, source_id, services):
    print("configuration:")
    print(f"check: {check}")
    print(f"host: {host}")
    print(f"name: {name}")
    print(f"source_type: {source_type}")
    print(f"source_id: {source_id}")
    print(f"services: {services}")

    while True:
        for service in services:
            output = check_output(
                ["sudo", "systemctl", "show", "--no-page", service],
                universal_newlines=True,
            )
            if m := ACTIVESTATE_RE.search(output):
                active_state = m.group(1)
            if m := SUBSTATE_RE.search(output):
                sub_state = m.group(1)
            if m := ACTIVEENTER_TS_RE.search(output):
                active_enter_ts = m.group(1)
            if m := WATCHDOG_TS_RE.search(output):
                watchdog_ts = m.group(1)
            timestamp = datetime.utcnow().isoformat()
            print(
                f'{timestamp}, {host}, systemd, status, systemd.service={service} systemd.ActiveState={active_state} systemd.SubState={sub_state} systemd.ActiveStateTimestamp="{active_enter_ts}" systemd.WatchdogTimestamp="{watchdog_ts}"',
                flush=True,
            )

        time.sleep(check)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-c",
        "--check",
        type=int,
        default=60,
        help="check and dump logs every N seconds. Default is 60.",
    )
    parser.add_argument(
        "--host", type=str, required=True, help="Host name.",
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        default="systemd",
        help="Log file basename. Defaults to 'systemd'.",
    )
    parser.add_argument(
        "-t",
        "--source_type",
        type=str,
        default="unspecified",
        help="Source type. Defaults to 'unspecified'.",
    )
    parser.add_argument(
        "-i",
        "--source_id",
        type=str,
        default="systemd",
        help="Source type. Defaults to 'systemd'.",
    )
    parser.add_argument(
        "-s",
        "--services",
        nargs="+",
        required=True,
        type=str,
        help="Systemd services to check.",
    )
    args = parser.parse_args()
    daemon(
        args.check,
        args.host,
        args.name,
        args.source_type,
        args.source_id,
        args.services,
    )
