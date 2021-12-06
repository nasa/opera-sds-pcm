#!/usr/bin/env python
"""
SDSWatch daemon to periodically dump SDSWatch logs of supervisord services.
"""
import argparse
import time
import re
from subprocess import check_output
from datetime import datetime


# regexes
RUNNING_RE = re.compile(
    r"^(?P<service>.+?)\s+(?P<status>RUNNING)\s+pid\s+(?P<pid>\d+),\s+uptime\s+(?P<uptime>.+)$"
)
STOPPED_RE = re.compile(
    r"^(?P<service>.+?)\s+(?P<status>STOPPED)\s+(?P<date_stopped>.+)$"
)


def daemon(check, host, name, source_type, source_id, services):
    print("configuration:")
    print(f"check: {check}")
    print(f"host: {host}")
    print(f"name: {name}")
    print(f"source_type: {source_type}")
    print(f"source_id: {source_id}")
    print(f"services: {services}")

    while True:
        if services is None:
            try:
                output = check_output(
                    ["supervisorctl", "status"], universal_newlines=True
                )
            except Exception as e:
                output = str(e.output)
            lines = [i.strip() for i in output.split("\n")]
        else:
            lines = []
            for service in services:
                try:
                    output = check_output(
                        ["supervisorctl", "status", service], universal_newlines=True
                    )
                except Exception as e:
                    output = str(e.output)
                lines.extend([i.strip() for i in output.split("\n")])
        timestamp = datetime.utcnow().isoformat()
        for line in lines:
            if m := RUNNING_RE.search(line):
                g = m.groupdict()
                print(
                    f'{timestamp}, {host}, supervisord, status, supervisord.service={g["service"]} supervisord.status={g["status"]} supervisord.pid={g["pid"]} supervisord.uptime="{g["uptime"]}"',
                    flush=True,
                )
            elif m := STOPPED_RE.search(line):
                g = m.groupdict()
                print(
                    f'{timestamp}, {host}, supervisord, status, supervisord.service={g["service"]} supervisord.status={g["status"]} supervisord.date_stopped="{g["date_stopped"]}"',
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
        help="Log file basename. Defaults to 'supervisord'.",
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
        help="Source type. Defaults to 'supervisord'.",
    )
    parser.add_argument(
        "-s",
        "--services",
        nargs="+",
        required=False,
        type=str,
        default=None,
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
