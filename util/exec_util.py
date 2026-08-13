#!/usr/bin/env python
from builtins import str

import sys
import os
import traceback
import json
from concurrent.futures import Executor, Future
from datetime import datetime, timezone

from subprocess import check_output, STDOUT, CalledProcessError

from opera_commons.logger import logger

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


"""
List of common substrings in PCM error messages we can strip out to reduce the short error message to be more readable.

For str values, all occurrences of the substring are removed.
Alternatively, a tuple can be provided with parameters for str.replace()
"""
SHORT_ERROR_REPLACEMENTS = [
    'SciFlo step ',
    'input_preprocessor_',
    'postprocessor_',
    ('_PGE', '', 1)
]


def get_short_error(e: Exception, strip=False) -> str:
    """Custom-elide error strings"""
    err_string = str(e)

    if strip:
        for replacement in SHORT_ERROR_REPLACEMENTS:
            if isinstance(replacement, str):
                err_string = err_string.replace(replacement, '')
            else:
                err_string = err_string.replace(*replacement)

    if len(err_string) > 35:  # https://github.com/hysds/hysds/blob/70f7ad93c99e986d90381b83313587e66409c189/hysds/utils.py#L347
        err_string = f"{err_string[:33]}.."

    return err_string


def exec_wrapper(func):
    """Execution wrapper to dump alternate errors and tracebacks."""

    import inspect
    if inspect.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            try:
                status = await func(*args, **kwargs)
            except (Exception, SystemExit) as e:
                with open("_alt_error.txt", "w") as f:
                    f.write("%s" % get_short_error(e))
                with open("_alt_traceback.txt", "w") as f:
                    f.write("%s\n" % traceback.format_exc())
                raise
            sys.exit(status)
    else:
        def wrapper(*args, **kwargs):
            try:
                status = func(*args, **kwargs)
            except (Exception, SystemExit) as e:
                with open("_alt_error.txt", "w") as f:
                    f.write("%s" % get_short_error(e))
                with open("_alt_traceback.txt", "w") as f:
                    f.write("%s\n" % traceback.format_exc())
                raise
            sys.exit(status)

    return wrapper


def call_noerr(cmd, work_dir, logr=logger):
    """Run command and warn if exit status is not 0."""
    info_dict = {}
    info_dict["time_start"] = datetime.now(timezone.utc).replace(tzinfo=None).strftime(ISO_DATETIME_PATTERN) + "Z"
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
        logr.critical("Got exception running:\n{}\nSTDOUT/STDERR:\n{}".format(cmd, e.output.decode()))

        # SAS exit code 1000 signals a large temporal gap in the input
        # stack. POSIX wait statuses are 8-bit, so through the
        # shell -> docker -> container chain exit(1000) arrives here as
        # 1000 % 256 == 232; match both in case a future invocation path
        # preserves the full code (aliasing risk for other codes == 232
        # mod 256 accepted and documented). The job still fails, but with
        # a distinct short error operators can facet on in Figaro (short
        # errors elide at 35 chars — keep this message under that).
        if e.returncode in (1000, 1000 % 256):
            err = RuntimeError('large data gap (SAS exit 1000)')
        else:
            err = RuntimeError('PGE/SAS failure')
        err.add_note(e.output.decode())
        raise err from e
    except Exception as e:
        logr.error("Got exception running:\n{}\nException: {}".format(cmd, str(e)))
        logr.error("Traceback: {}".format(traceback.format_exc()))
        raise
    finally:
        logr.info("writing _pge_info.json: {}".format(info_dict))
        with open(pge_info_path, "w+") as pge_info:
            json.dump(info_dict, pge_info, indent=4)


class DummyThreadPoolExecutor(Executor):
    """Dummy thread pool executor to keep single threaded execution using the TPE structure."""

    def __init__(self, max_workers=None, **kwargs):
        self._max_workers = max_workers

    def submit(self, fn, /, *args, **kwargs) -> Future:
        future = Future()
        future.set_result(fn(*args, **kwargs))
        return future

    def shutdown(self, wait = True, *, cancel_futures = False):
        pass

    def map(self, fn, *iterables, **kwargs):
        return map(fn, *iterables)
