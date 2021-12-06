from util.common_util import convert_datetime
from util.common_util import fix_timestamp
from commons.logger import logger
from commons.es_connection import get_grq_es
from commons.constants import product_metadata as pm
import datetime
import pandas as pd
import xml.etree.ElementTree as ET
import boto3
from urllib.parse import urlparse

ancillary_es = get_grq_es(logger)

EQUATOR_ASC = "Ascending"
EQUATOR_DESC = "Descending"


def get_stuf_info_from_xml(beginning_time, ending_time):
    """
    Get the data from STUF file given begin and end time of PGE
    :param beginning_time: begin time of PGE
    :param ending_time: end time of PGE
    :return: orbit_num, orbit_start_time, orbit_end_time, ctz, orbit_dir, eq_cross_time
    """
    bt = convert_datetime(beginning_time) - datetime.timedelta(days=15)
    pad_beginning_time = convert_datetime(bt)

    results = ancillary_es.perform_es_range_intersection_query(
        beginning_date_time=pad_beginning_time,
        ending_date_time=ending_time,
        met_field_beginning_date_time="metadata.{}".format(pm.VALIDITY_START_DATE_TIME),
        met_field_ending_date_time="metadata.{}".format(pm.VALIDITY_END_DATE_TIME),
        index="grq_*_{}".format(pm.STUF.lower()),
    )

    datastore_refs = ancillary_es.get_datastore_refs(results)

    if len(datastore_refs) == 0:
        raise Exception(
            "Could not find any {} over time range {} to {} ".format(
                "stuf", pad_beginning_time, ending_time
            )
        )
    else:
        logger.info("Found matching STUF file(s): {}".format(datastore_refs))

    file_names = []
    s3 = boto3.resource("s3")
    for f in datastore_refs:
        d = urlparse(f, allow_fragments=False)
        d_part = d.path.split("/")
        bucket = d_part[1]
        separator = "/"
        filepathname = separator.join(d_part[2:])
        file_names.append(d_part[-1])
        logger.info("Downloading {} to {}".format(filepathname, d_part[-1]))
        s3.Bucket(bucket).download_file(filepathname, d_part[-1])

    ctz_time = []
    ctz = None
    orbit_dir = None
    orbit_start_time = None
    orbit_end_time = None
    eq_cross_time = None
    orbit_num = -1
    for f in file_names:
        orbit_num, orbit_start_time, orbit_end_time, ctz = get_stuf_info_from_file(
            f, beginning_time
        )
        if str(orbit_num) != "-1":
            orbit_dir, eq_cross_time = get_equator(f, orbit_num)
            break
        else:
            continue

    for f in file_names:
        ctz_time.append(get_ctz(f))

    # Find earlier ctz time if ctz in STUF is later than orbit start time
    if orbit_start_time:
        if ctz > orbit_start_time:
            for t in ctz_time:
                if t < orbit_start_time:
                    ctz = t
    else:
        logger.warning("orbit_start_time is empty")

    cycle_num = get_cycle(int(orbit_num))
    relative_orbit_num = get_relative_orbit_number(int(orbit_num))

    logger.info(
        "get_stuf_info_from_xml returning orbit_num={}, cycle_num={}, relative_orbit_num={}, orbit_start_time={}, ctz={}, "
        "orbit_dir={}, eq_cross_time={}".format(
            orbit_num,
            cycle_num,
            relative_orbit_num,
            orbit_start_time,
            ctz,
            orbit_dir,
            eq_cross_time,
        )
    )
    if isinstance(orbit_start_time, datetime.datetime):
        orbit_start_time = convert_datetime(orbit_start_time)
    if isinstance(orbit_end_time, datetime.datetime):
        orbit_end_time = convert_datetime(orbit_end_time)
    if isinstance(eq_cross_time, datetime.datetime):
        eq_cross_time = convert_datetime(eq_cross_time)
    return (
        orbit_num,
        cycle_num,
        relative_orbit_num,
        orbit_start_time,
        orbit_end_time,
        ctz,
        orbit_dir,
        eq_cross_time,
    )


def file_has_orbit_number(file_name, orbit_num):
    """
    Test STUF file has the oribt number
    :param file_name: STUF file
    :param orbit_num:
    :return: True or False
    """
    has_orbit_number = False
    tree = ET.parse(file_name)
    root = tree.getroot()
    for node in root.findall("orbitBoundary"):
        for n in node:
            for s in n:
                if s.tag == "event":
                    if s[1].text == str(orbit_num):
                        has_orbit_number = True
    return has_orbit_number


def get_cycle(orbit_num):
    """
    Calculate cycle number
    :param orbit_num: orbit number
    :return: cycle number
    """
    return (orbit_num - 1) // 173 + 1


def get_relative_orbit_number(orbit_num):
    """
    Calculate relative orbit number
    :param orbit_num: orbit number
    :return: relative orbit number
    """
    return (orbit_num - 1) % 173 + 1


def convert_orbit_direction(label):
    """
    Find orbit direction
    :param label: EQUATOR_DESC or EQUATOR_ASC
    :return: Decending or Ascending
    """
    if label == "EQUATOR_DESC":
        return "Descending"
    else:
        return "Ascending"


def get_ctz(file_name):
    """
    Get cycle time zero from XML file
    :param file_name:
    :return: ctz as datetime
    """
    tree = ET.parse(file_name)
    root = tree.getroot()
    time_val = ""
    for node in root.findall("fixedStates"):
        for n in node:
            for j in n:
                # print(j.text)
                if j.text == "SDS cycle reference":
                    for k in n:
                        if k.tag == "time" and k.attrib["sys"] == "UTC":
                            time_val = k.text

    return convert_stuf_datetime(time_val)


def convert_stuf_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%f"):
    """
    Convert STUF date time to proper format then convert to datetime
    STUF time string has more training digits than allowed. Need to trim them
    Example of  time string in STUF file : 2022-01-07T23:21:23.680452829
    :param datetime_obj: a datetime object or STUF time string format
    :param strformat: String format of datetime
    :return: datetime object or string
    """

    if isinstance(datetime_obj, datetime.datetime):
        return datetime_obj.strftime(strformat)
    return datetime.datetime.strptime(fix_timestamp(str(datetime_obj)), strformat)


def get_stuf_info_from_file(file_name, begin_time):
    """
    Get orbit_num, orbit_start_time, orbit_end_time from XML, given begin time
    :param file_name: STUF file
    :param begin_time: PGE begin time
    :return: orbit number, orbit_start_time, orbit_end_time, ctz
    """
    tree = ET.parse(file_name)
    root = tree.getroot()
    # example begin_time="2022-01-01T11:32:08.531124Z"
    orbit_num, orbit_start_time, orbit_end_time = find_orbit_num_and_time(
        root, begin_time
    )
    ctz = get_ctz(file_name)
    logger.info(
        "get_stuf_info_from_file return orbit_num={},orbit_start_time={},orbit_end_time={},ctz={}".format(
            orbit_num, orbit_start_time, orbit_end_time, ctz
        )
    )
    return orbit_num, orbit_start_time, orbit_end_time, ctz


def find_orbit_num_and_time(root, time_str):
    """
    Find the orbit number and start/end time, where the input time belong
    Ex. if input time is 2022-01-01 03:32, then
    orbit number = 2, orbit_start_time = 2022-01-01 02:32:50, orbit_end_time = 2022-01-01 04:12:43
             orbit_num                 start_time
    0            0 2021-12-31 23:59:59.999978
    1            1 2022-01-01 00:52:57.123668
    2            2 2022-01-01 02:32:50.265995
    3            3 2022-01-01 04:12:43.355644
    4            4 2022-01-01 05:52:36.367406
    :param root: Root of XML tree
    :param time_str: input time
    :return: orbit_number, orbit_start_time, orbit_end_time
    """
    start_datetime = convert_datetime(time_str)

    orbit_number = -1
    orbit_start_time = ""
    orbit_end_time = ""
    orbit_index = -1

    orbit_num = []
    start_time = []
    for node in root.findall("orbitBoundary"):
        for n in node:
            for s in n:
                if s.tag == "event":
                    orbit_num.append(int(s[1].text))
                    start_time.append(convert_stuf_datetime(s[0].text))

    d = {"orbit_num": orbit_num, "start_time": start_time}
    df = pd.DataFrame(d)
    row_count = df.shape[0]
    # start_datetime is outside range of this file
    if start_datetime > df.at[row_count - 1, "start_time"]:
        orbit_number = -1
        orbit_start_time = -1
        orbit_end_time = -1
        start_time.append(convert_stuf_datetime(s[0].text))

    for i in range(row_count):
        if (
            df.at[i, "start_time"] < start_datetime
            or df.at[i, "start_time"] == start_datetime
        ):
            orbit_number = df.at[i, "orbit_num"]
            orbit_start_time = df.at[i, "start_time"]
            orbit_index = i
        else:
            break
    if orbit_index + 1 < row_count:
        orbit_end_time = df.at[orbit_index + 1, "start_time"]
    else:
        orbit_end_time = "-1"  # out of range
    return orbit_number, orbit_start_time, orbit_end_time


def get_equator(file_name, orbit_number):
    """
    Get equator info, given orbit number.
    For a given orbit, equator come in pair. Use the second equator time as equator crossing time, since the first one is
    the same as  orbit start time
    Ex. For orbit_num 2, equator cross time=2022-01-01T03:22:42.414244420, Descending
    If the second equator does not exist, use the value of the first one, see orbit_num 101. Is this a problem with
    our file? eq cross time=2022-01-07T23:21:23 and direction=Ascending

            orbit_num                        eq_time orbit_direction
    0           0  2022-01-01T00:02:56.147395844    EQUATOR_DESC
    1           1  2022-01-01T00:52:57.123668552     EQUATOR_ASC
    2           1  2022-01-01T01:42:49.337662508    EQUATOR_DESC
    3           2  2022-01-01T02:32:50.265995495     EQUATOR_ASC
    4           2  2022-01-01T03:22:42.414244420    EQUATOR_DESC
    ..        ...                            ...             ...
    197        99  2022-01-07T20:01:37.553508176     EQUATOR_ASC
    198        99  2022-01-07T20:51:29.617244701    EQUATOR_DESC
    199       100  2022-01-07T21:41:30.580727109     EQUATOR_ASC
    200       100  2022-01-07T22:31:22.682711721    EQUATOR_DESC
    201       101  2022-01-07T23:21:23.680452829     EQUATOR_ASC
    :param file_name: STUF file
    :param orbit_number: orbit number
    :return: orbit direction and equator crossing time(in datetime format)
    """
    tree = ET.parse(file_name)
    root = tree.getroot()
    eq_time = []
    orbit_num = []
    label = []
    orbit_direction = ""
    eq_cross_time = ""

    for node in root.findall("events"):
        for n in node:
            if n.tag == "equator":
                for j in n:
                    if j.tag == "time":
                        eq_time.append(j.text)
                    if j.tag == "orbitNum":
                        orbit_num.append(j.text)
                    if j.tag == "label":
                        label.append(j.text)
    d = {"orbit_num": orbit_num, "eq_time": eq_time, "orbit_direction": label}
    df = pd.DataFrame(d)
    print(df)
    row_count = df.shape[0]

    for i in range(row_count):
        if (
            df.at[i, "orbit_num"] == str(orbit_number)
            and i < row_count
            and i + 1 < row_count
        ):
            orbit_direction = df.at[i + 1, "orbit_direction"]
            eq_cross_time = df.at[i + 1, "eq_time"]
            break
        elif (  # second equator is empty, reach end of file
            df.at[i, "orbit_num"] == str(orbit_number)
            and i < row_count
            and i + 1 == row_count
        ):
            orbit_direction = df.at[i, "orbit_direction"]
            eq_cross_time = df.at[i, "eq_time"]
    return (
        convert_orbit_direction(orbit_direction),
        convert_stuf_datetime(eq_cross_time),
    )
