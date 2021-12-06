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
# usage: sclk2scet.py --lrclk <lrclk time>
#
# Translate the NISAR lrclk time.
#
# Examples:
#
# $ sclk2scet.py --lrclk <lrclk time>
#
# Authors: Alice Stanboli
#

from datetime import datetime
# from astropy.time import Time
import re
import argparse
import traceback
from operator import itemgetter

import rost.time as TU


lrclk_scet_rate_set_store = []


def ingestLRCLKToSCETFile(filename):

    with open(filename, "r") as fp:
        line = fp.readline()
        while "__SCLK0__" not in line:
            line = fp.readline()
            # print(line)
        while True:
            line = fp.readline()
            if "CCSD" in line:
                break
            elif line:
                # print("Line : " + line.strip())
                vals = re.split(r'\s+', line)
                lrclk = vals[1]
                # print("lrclk: '"+str(vals[1])+"'")
                scet = vals[2][0:24]
                # print("scet: '"+str(scet)+"'")
                # convert scet to datetime 2019-001T00:00:00.0000000
                scetdt = datetime.strptime(scet, '%Y-%jT%H:%M:%S.%f')
                rate = vals[4]
                # print("rate: '"+str(vals[4])+"'")
                lrclk_scet_rate_set_store.append((float(lrclk), scetdt, float(rate)))
            else:
                break
        fp.close()


def getMostRecentRate(lrclk):

    try:
        sorted_store = sorted(lrclk_scet_rate_set_store, key=itemgetter(0))
        for rate_set in sorted_store:
            if lrclk > rate_set[0]:
                lrclk0 = rate_set[0]
                scet0 = rate_set[1]
                rate = rate_set[2]
            else:
                break
        # print("lrclk0: " + str(lrclk0) + ",  scet0: " +  str(scet0) + ", rate: " + str(rate))
        return lrclk0, scet0, rate
    except Exception:
        return None, None, None


def _radarTimeToUTC(lrclk, offset=0.):
    # TODO: Kludge to adjust lrlck by some offset to handle changes between different versions
    #       of SCLKSCET file. Note that https://jira.jpl.nasa.gov/browse/NSDS-1306 will
    #       completely rewrite this these time conversion utilities.
    lrclk -= 35403871

    lrclk0, scetdt0, rate = getMostRecentRate(lrclk)

    # Use this formula to calculate SCET in UTC from LRCLK
    # SCET(LRCLK) = SCET0 + LRCLKRATE * (LRCLK - LRCLK0)

    return datetime.fromtimestamp(scetdt0.timestamp() + (rate * (lrclk - lrclk0)) - offset)


def radarTimeToUTC(lrclk):
    try:
        scetdt = _radarTimeToUTC(lrclk)
        # print("scetdt: "+str(scetdt))

        return TU.getISOTime(scetdt)
    except Exception:
        # print(str(e))
        # print(traceback.print_exc())
        # gpsepoch = datetime(1980, 1, 6, 0, 0, 0)
        # print("radar_datetime: " + str(gpsepoch))
        # return TU.getISOTime(gpsepoch)
        raise Exception("Not a valid radar time.")


def radarTimeToUTCWithOffset(lrclk, offset):
    try:
        scetdt = _radarTimeToUTC(lrclk, offset)
        # print("scetdt: "+str(scetdt))

        return TU.getISOTimeNoMilli(scetdt)
    except Exception:
        # print(str(e))
        # print(traceback.print_exc())
        # gpsepoch = datetime(1980, 1, 6, 0, 0, 0)
        # print("radar_datetime: " + str(gpsepoch))
        # return TU.getISOTime(gpsepoch)
        raise Exception("Not a valid radar time.")


def radarTimeToDT(lrclk):
    try:
        scetdt = _radarTimeToUTC(lrclk)
        # print "scetdt: "+str(scetdt)

        return scetdt
    except Exception as e:
        print(str(e))
        print(traceback.print_exc())
        gpsepoch = datetime(1980, 1, 6, 0, 0, 0)
        # print("radar_datetime: " + str(gpsepoch))
        return gpsepoch


def main():

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Add arguments to parser
    parser.add_argument('--file', required=True)

    # Add arguments to parser
    parser.add_argument('--lrclk', required=False)

    # Add arguments to parser
    parser.add_argument('--rosttime', required=False)

    # Get command line arguments
    args = vars(parser.parse_args())

    # Get input lrclk (required)
    # print("float(args['lrclk'][1]): " + str(float(args['lrclk'])))
    lrclk = args['lrclk']
    print("lrclk: " + str(lrclk))
    rosttime = args['rosttime']
    print("rosttime: " + str(rosttime))

    print("ingesting file: " + str(args['file']))
    ingestLRCLKToSCETFile(args['file'])

    if lrclk is not None:
        lrclk_in_secs = float(lrclk) / 1e7
        print("lrclk_in_secs: " + str(lrclk_in_secs))
        # utc_time = radarTimeToUTC(float(lrclk) / 1e7)
        utc_time = radarTimeToUTC(float(lrclk))
        print("utc_time: " + str(utc_time))
        utc_time = radarTimeToUTCWithOffset(float(lrclk), 31)
        print("utc_time with offset: " + str(utc_time))
    else:
        # t = Time(rosttime, scale='tai')
        # leap_seconds = t.unix_leap - t.unix
        # lrclk = t.to_value('gps', subfmt='float')
        print("lrclk: " + str(lrclk))
        utc_time = radarTimeToUTCWithOffset(float(lrclk), 31)
        print("utc_time: " + str(utc_time))

        # lrclk = gpsepoch + datetime.timedelta(seconds=rosttime + delta + -1.0)

        # print("lrclk: " + str(lrclk))
        # lrclk = datetime.strptime(rosttime, '%Y-%jT%H:%M:%S.%f').timestamp()
        # utc_time = radarTimeToUTC(lrclk)
        # print("utc_time: " + str(utc_time))


if __name__ == "__main__":
    main()
