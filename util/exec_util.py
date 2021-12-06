#!/usr/bin/env python
from builtins import str

import sys
import os
import traceback
import json
from datetime import datetime

from subprocess import check_output, STDOUT, CalledProcessError

from commons.logger import logger

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


def exec_wrapper(func):
    """Execution wrapper to dump alternate errors and tracebacks."""

    def wrapper(*args, **kwargs):
        try:
            status = func(*args, **kwargs)
        except (Exception, SystemExit) as e:
            with open("_alt_error.txt", "w") as f:
                f.write("%s\n" % str(e))
            with open("_alt_traceback.txt", "w") as f:
                f.write("%s\n" % traceback.format_exc())
            raise
        sys.exit(status)

    return wrapper


def call_noerr(cmd, work_dir, logr=logger):
    """Run command and warn if exit status is not 0."""
    info_dict = {}
    info_dict["time_start"] = datetime.utcnow().strftime(ISO_DATETIME_PATTERN) + "Z"
    logr.info("dir: {}".format(os.getcwd()))
    pge_info_path = work_dir + "/_pge_info.json"
    try:
        output = check_output(cmd, stderr=STDOUT, shell=True)
        logr.info("Ran:\n{}\nSTDOUT/STDERR:\n{}".format(cmd, output))
        info_dict["status"] = 0
        try:
            output = output.decode()
        except (UnicodeDecodeError, AttributeError):
            pass
        info_dict["stdout"] = output
        info_dict["stderr"] = ""
    except CalledProcessError as e:
        info_dict["status"] = e.returncode
        info_dict["stdout"] = ""
        info_dict["stderr"] = e.output.decode()
        raise RuntimeError("Got exception running:\n{}\nSTDOUT/STDERR:\n{}".format(cmd, e.output.decode()))
    except Exception as e:
        logr.error("Got exception running:\n{}\nException: {}".format(cmd, str(e)))
        logr.error("Traceback: {}".format(traceback.format_exc()))
        raise
    finally:
        logr.info("writing _pge_info.json: {}".format(info_dict))
        with open(pge_info_path, "w+") as pge_info:
            json.dump(info_dict, pge_info, indent=4)
