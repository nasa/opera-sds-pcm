#!/usr/bin/env python

# Copyright 2019, by the California Institute of Technology.
# ALL RIGHTS RESERVED. United States Government sponsorship acknowledged.
# Any commercial use must be negotiated with the Office of Technology
# Transfer at the California Institute of Technology.  This software
# may be subject to U.S. export control laws and regulations. By accepting
# this document, the user agrees to comply with all applicable U.S.
# export laws and regulations. User has the responsibility to obtain
# export licenses, or other export authority as may be required, before
# exporting such information to foreign countries or providing access
# to foreign persons.

# NOTE: this file was copied from the opera_pge repository
#
# Time utilities
#
# Authors: Alice Stanboli
#

from datetime import date
from datetime import datetime


def getCurrentISOTime():
    # time_in_iso = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    time_in_iso = datetime.now().isoformat(sep='T', timespec='microseconds') + "Z"
    # print("time_in_iso = " + time_in_iso + "\n")
    return time_in_iso


def getISOTime(dt):
    time_in_iso = dt.isoformat(sep='T', timespec='microseconds') + "Z"
    # print("time_in_iso = " + time_in_iso + "\n")
    return time_in_iso


def getISOTimeNoMilli(dt):
    time_in_iso_no_milli = dt.strftime('%Y-%m-%dT%H:%M:%S')
    # print("time_in_iso_no_milli = " + time_in_iso_no_milli + "\n")
    return time_in_iso_no_milli


def getISODate(year, month, day):
    date_in_iso = date(year, month, day).isoformat()
    return date_in_iso


def getTimeForFilename(dt):
    datetime_str = dt.strftime('%Y%m%dT%H%M%S')
    return datetime_str


def get_catalog_metadata_datetime_str(dt):
    '''Get date/time strings in L1/L2 catalog metadata format
    '''
    # TODO: Rework to support 0.1 nanosecond resolution
    # That probably means ditching Python's datetime class.
    datetime_str = dt.isoformat(sep='T', timespec='microseconds') + "0000" + "Z"
    return datetime_str
