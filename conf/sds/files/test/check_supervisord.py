#!/usr/bin/env python
"""Verify supervisord is started."""
import subprocess
import argparse
import os
import logging
from xmlrpc.client import ServerProxy

log_format = (
    "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"  # set logger
)
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

mozart = subprocess.getoutput(
    "(grep ^MOZART_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)"
)
grq = subprocess.getoutput("(grep ^GRQ_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)")
metrics = subprocess.getoutput(
    "(grep ^METRICS_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)"
)
factotum = subprocess.getoutput(
    "(grep ^FACTOTUM_PVT_IP ~/.sds/config | cut -d: -f2 | xargs)"
)


def check_state(f):
    for node in (mozart, factotum, grq, metrics):
        server = ServerProxy("http://ops:ops@" + str(node) + ":9001/RPC2")
        try:
            server.supervisor.getState()
            f.write("SUCCESS\n")
            num_process = len(server.supervisor.getAllProcessInfo())
            for proc in range(num_process):
                if (
                    server.supervisor.getAllProcessInfo()[proc]["statename"]
                    != "RUNNING"
                ):
                    f.write(
                        "ERROR :Service "
                        + server.supervisor.getAllProcessInfo()[proc]["name"]
                        + " not started on "
                        + node
                        + "\n"
                    )
                    logger.error(
                        "Service is not in an expected running state on {} : process={}, state={}".format(
                            node,
                            server.supervisor.getAllProcessInfo()[proc]["name"],
                            server.supervisor.getAllProcessInfo()[proc]["statename"],
                        )
                    )

        except Exception:
            f.write("ERROR Supevisord not started on " + node + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("res_file", help="results file")
    args = parser.parse_args()
    with open(args.res_file, "w") as f:
        check_state(f)
