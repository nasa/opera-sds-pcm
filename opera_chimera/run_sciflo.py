#!/usr/bin/env python

"""
Python entrypoint for execution of a SCIFLO pipeline
"""

import argparse
import os
import json
import sys
import subprocess
from importlib import import_module

from commons.logger import logger
from chimera.commons.accountability import Accountability
from chimera.commons.sciflo_util import (
  __create_placeholder_alt_files,
  __cleanup_placeholder_alt_files,
  extract_error,
  copy_sciflo_work
)

def run_sciflo(sfl_file, sfl_args, output_dir, timeout):
    """
    Run sciflo.

    This function has been adapted from the default version (chimera/run_sciflo.py)
    to support inclusion of a timeout value when invoking sflExec.py
    """

    # build paths to executables
    sflexec_path = os.path.join(os.environ["HOME"], "verdi", "bin", "sflExec.py")

    # create placeholder files
    __create_placeholder_alt_files()

    # execute sciflo
    cmd = [
        sflexec_path,
        "-s",
        "-f",
        "-o",
        output_dir,
        "-t",
        f"{timeout}",
        "--args",
        ",".join(sfl_args),
        sfl_file,
    ]
    logger.info("Running sflExec.py command:\n%s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, shell=False, check=True)
        status = proc.returncode
    except subprocess.CalledProcessError as err:
        status = err.returncode
    logger.info("Exit status is: %d", status)
    if status != 0:
        extract_error("%s/sciflo.json" % output_dir)
        status = 1

    # copy sciflo work and exec dir
    try:
        copy_sciflo_work(output_dir)
    except Exception:
        pass

    __cleanup_placeholder_alt_files()
    return status


# grabs accountability class if implemented and set in the sciflo jobspecs
def get_accountability_class(context_file):
    work_dir = None
    context = None
    if isinstance(context_file, str):
        work_dir = os.path.dirname(context_file)
        with open(context_file, "r") as f:
            context = json.load(f)
    path = context.get("module_path")
    if "accountability_module_path" in context:
        path = context.get("accountability_module_path")
    accountability_class_name = context.get("accountability_class", None)
    accountability_module = import_module(path, "opera-pcm")
    if accountability_class_name is None:
        logger.error("No accountability class specified")
        return Accountability(context, work_dir)
    cls = getattr(accountability_module, accountability_class_name)
    if not issubclass(cls, Accountability):
        logger.error("accountability class does not extend Accountability")
        return Accountability(context, work_dir)
    cls_object = cls(context, work_dir)
    return cls_object


def main(sfl_file, context_file, output_folder):
    """Main."""

    sfl_file = os.path.abspath(sfl_file)
    context_file = os.path.abspath(context_file)
    with open(context_file) as infile:
        timeout = (
            json.load(infile).get("job_specification", {}).get("soft_time_limit", 86400)
        )
    logger.info("sfl_file: %s", sfl_file)
    logger.info("context_file: %s", context_file)
    logger.info("soft_time_limit: %d", timeout)
    accountability = get_accountability_class(context_file)
    accountability.create_job_entry()
    result = run_sciflo(
        sfl_file, ["sf_context=%s" % context_file], output_folder, timeout
    )
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sfl_file", help="SciFlo workflow")
    parser.add_argument("context_file", help="HySDS context file")
    parser.add_argument("output_folder", help="Sciflo output file")
    args = parser.parse_args()
    sys.exit(main(args.sfl_file, args.context_file, args.output_folder))
