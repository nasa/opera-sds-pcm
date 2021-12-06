#!/usr/bin/env python
"""Dump job status."""

import os
import sys
import logging
import csv

from tabulate import tabulate


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


def dump_isl_report(isl_report_file):
    isl_table = list()
    with open(isl_report_file, "r") as f:
        reader = csv.reader(f)
        line_count = 0
        for row in reader:
            if line_count == 0:
                headers = row
            else:
                row_string = ""
                for i in range(len(row)):
                    value = row[i]
                    if i == (len(row) - 1):
                        row_string += "{}".format(value)
                    else:
                        row_string += "{}\n--\n".format(value)
                isl_table.append([row_string])
            line_count += 1

    headers_string = ""
    for i in range(len(headers)):
        value = headers[i]
        if i == (len(headers) - 1):
            headers_string += "{}".format(value)
        else:
            headers_string += "{}\n--\n".format(value)

    tbl = tabulate(isl_table, headers=[headers_string], tablefmt="fancy_grid")
    print(tbl)


if __name__ == "__main__":
    isl_report_file = sys.argv[1]
    dump_isl_report(isl_report_file)
